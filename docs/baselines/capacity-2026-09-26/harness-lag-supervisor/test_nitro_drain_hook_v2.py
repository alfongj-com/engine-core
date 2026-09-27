import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

HOOK = Path('/tmp/engine-capacity-tools/nitro-drain-hook-v2.py')


class HookTests(unittest.TestCase):
    def fixture(self, directory, blocked=False):
        directory = Path(directory)
        calls = directory / 'calls.jsonl'
        fake = directory / 'fake-cast'
        fake.write_text('#!' + sys.executable + '\n' +
            'import json,os,sys,time\n' +
            "if sys.argv[1]=='chain-id': print('412346')\n" +
            'else:\n' +
            ' with open(' + repr(str(calls)) + ", 'a') as f: f.write(json.dumps({'time':time.time(),'pid':os.getpid()})+'\\n')\n" +
            (' time.sleep(30)\n' if blocked else " print('0x'+'1'*64)\n"))
        fake.chmod(0o755)
        wrapper = directory / 'wrapper.py'
        wrapper.write_text("import importlib.util,sys\ns=importlib.util.spec_from_file_location('hook'," + repr(str(HOOK)) + ")\nm=importlib.util.module_from_spec(s)\ns.loader.exec_module(m)\nm.CAST=" + repr(str(fake)) + "\nsys.argv=[m.__file__,*sys.argv[1:]]\nm.main()\n")
        return wrapper, calls

    def test_bounds_reject_without_cast(self):
        for invalid in ('0', '1801'):
            result = subprocess.run([sys.executable, str(HOOK), '--max-seconds', invalid], capture_output=True, timeout=3)
            self.assertEqual(result.returncode, 2)

    def test_deadline_and_at_most_one_initiation_per_second(self):
        with tempfile.TemporaryDirectory() as directory:
            wrapper, calls = self.fixture(directory)
            start = time.monotonic()
            result = subprocess.run([sys.executable, str(wrapper), '--max-seconds', '2'], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            events = [json.loads(line) for line in result.stdout.splitlines()]
            sends = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(events[0]['max_seconds'], 2)
            self.assertLess(time.monotonic() - start, 3)
            self.assertGreaterEqual(len(sends), 1)
            self.assertLessEqual(len(sends), 2)
            if len(sends) == 2:
                # Use the shared wall clock: macOS Python3.9 monotonic epochs are process-local.
                # Child-start jitter can differ slightly from initiation spacing.
                self.assertGreater(sends[1]['time'] - sends[0]['time'], .95)
            self.assertEqual(events[-1]['broadcasts'], len(sends))

    def test_owner_signal_reaps_blocked_cast_child(self):
        with tempfile.TemporaryDirectory() as directory:
            wrapper, calls = self.fixture(directory, blocked=True)
            child = subprocess.Popen([sys.executable, str(wrapper), '--max-seconds', '1800'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 3
                while not calls.exists() and time.monotonic() < deadline: time.sleep(.02)
                self.assertTrue(calls.exists())
                pid = json.loads(calls.read_text().splitlines()[0])['pid']
                child.send_signal(signal.SIGTERM)
                stdout, stderr = child.communicate(timeout=3)
                self.assertEqual(child.returncode, 0, stderr)
                self.assertTrue(json.loads(stdout.splitlines()[-1])['terminated_by_owner'])
                with self.assertRaises(ProcessLookupError): os.kill(pid, 0)
            finally:
                if child.poll() is None: child.kill(); child.wait()


if __name__ == '__main__': unittest.main()
