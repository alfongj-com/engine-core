import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('runner', str(Path(__file__).with_name('runner.py')))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def state(directory):
    return {'schema':'local-capacity-sequential-v4','id':'fixture','jobs':r.load_jobs(Path(__file__).with_name('jobs-fixture.json')),
        'runs':[],'active':None,'review_required':None,
        'frozen_sha256':{str(r.ENGINE):'binary',str(r.ROOT/'scripts/capacity_campaign.py'):'campaign',str(r.ROOT/'scripts/capacity_faults.py'):'faults',str(r.ROOT/'scripts/capacity_resource_guard.py'):'guard'},
        'config':{'drain_seconds':1800,'setup_seconds':120,'verification_seconds':600,'output_dir':str(directory)}}


def settled_report():
    return {'oracle':{'safety_pass':True,'observed_intents':9,'liveness_failures':['missed: offered intent not admitted']},
      'owned_children_stopped':True,'drain':{key:0 for key in r.DRAIN_FIELDS},'per_chain':{'evm12':{'responses':{'202':9}}},
      'engine_binary_sha256':'binary','harness_sha256':{'capacity_campaign.py':'campaign','capacity_faults.py':'faults','capacity_resource_guard.py':'guard'}}


class SupervisorTests(unittest.TestCase):
    def test_root_jobs_and_explicit_knobs(self):
        s=state('/tmp')
        self.assertEqual(len(s['jobs']),5)
        for job, expected in zip(s['jobs'],(32,64,64,64,64)):
            cmd=r.command(s,job,Path('/tmp/report.json'))
            self.assertEqual(cmd[cmd.index('--eoa-broadcast-concurrency')+1],str(expected))
            self.assertEqual(cmd[cmd.index('--max-schedule-lag-ms')+1],'100')
            self.assertEqual(r.timeout_seconds(s,job),2880)
        self.assertIn('nitro='+str(r.HOOK)+' --max-seconds 1800',r.command(s,s['jobs'][0],Path('/tmp/r')))
        self.assertEqual(r.command(s,s['jobs'][4],Path('/tmp/r'))[-1],'5')

    def test_http_concurrency_bounds_and_campaign_parser(self):
        # Validate real generated argv using the existing campaign parser; no nodes start.
        import capacity_campaign
        with tempfile.TemporaryDirectory() as d:
            jobs = r.load_jobs(Path(__file__).with_name('jobs-fixture.json'))
            job = copy.deepcopy(jobs[2])
            manifest = Path(d) / 'jobs.json'
            s = state(d)
            for value in (1, 128, 1024):
                job['extra'] = jobs[2]['extra'] + ['--http-concurrency', str(value)]
                manifest.write_text(json.dumps([job]))
                approved = r.load_jobs(manifest)[0]
                argv = r.command(s, approved, Path(d) / ('report-' + str(value) + '.json'))
                args, profiles = capacity_campaign.arguments(argv[2:])
                self.assertEqual(args.http_concurrency, value)
                self.assertEqual(args.max_schedule_lag_ms, 100)
                self.assertEqual(args.max_admission_p99_ms, 1000)
                self.assertEqual(args.max_inflight, 4096)
                self.assertEqual(args.proxy_concurrency, 256)
                self.assertEqual(set(profiles), {'evm12'})
            for value in ('0', '1025', '-1', '1.5'):
                job['extra'] = jobs[2]['extra'] + ['--http-concurrency', value]
                manifest.write_text(json.dumps([job]))
                with self.assertRaises(ValueError):
                    r.load_jobs(manifest)
            job['extra'] = jobs[2]['extra'] + ['--http-concurrency', '128', '--http-concurrency', '128']
            manifest.write_text(json.dumps([job]))
            with self.assertRaises(ValueError):
                r.load_jobs(manifest)
            original = r.command(s, jobs[2], Path(d) / 'default.json')
            args, _ = capacity_campaign.arguments(original[2:])
            self.assertEqual(args.http_concurrency, 64)

    def test_global_latch_and_active_block_other_subset(self):
        for field,value in [('review_required',{'reason':'unsafe'}),('active',{'name':'old'})]:
            s=state('/tmp');s[field]=value
            with self.assertRaises(RuntimeError):r.choose(s,['capacity-final-solana60-poll5'])

    def test_historical_unsafe_cannot_be_hidden_by_clear_latch(self):
        s=state('/tmp');s['runs']=[{'name':s['jobs'][0]['name'],'safely_settled':False}]
        with self.assertRaises(RuntimeError):r.choose(s,['capacity-final-solana60-poll5'])

    def test_dropped_offer_all_accepted_settled_allows_review_next_not_capacity(self):
        report=settled_report()
        self.assertTrue(r.safely_settled(report))
        for key,value in [('node_pending',1),('journal_unresolved',True),('redis_pending',{'count':0})]:
            changed=copy.deepcopy(report);changed['drain'][key]=value
            self.assertFalse(r.safely_settled(changed))
        report['oracle']['observed_intents']=8
        self.assertFalse(r.safely_settled(report))

    def test_unknown_liveness_or_cleanup_failure_blocks(self):
        for key,value in [('liveness_failures',['receipt missing']),('safety_pass',False)]:
            report=settled_report();report['oracle'][key]=value
            self.assertFalse(r.safely_settled(report))
        report=settled_report();report['owned_children_stopped']=False
        self.assertFalse(r.safely_settled(report))

    def test_unsafe_result_latches_durably_and_preserves_active(self):
        with tempfile.TemporaryDirectory() as d:
            s=state(d); path=Path(d)/'state.json';job=s['jobs'][2]
            def child(cmd, log, timeout):
                report=settled_report();report['drain']['redis_pending']=1
                Path(s['active']['report']).write_text(json.dumps(report));return2=2;return return2
            with patch.object(r,'verify_frozen'),patch.object(r.subprocess,'run'),patch.object(r,'run_child',side_effect=child):
                with self.assertRaises(RuntimeError):r.run_one(s,path,job)
            stored=json.loads(path.read_text())
            self.assertTrue(stored['review_required']);self.assertTrue(stored['active'])
            self.assertFalse(stored['runs'][0]['safely_settled'])
            with self.assertRaises(RuntimeError):r.choose(stored,['capacity-final-solana60-poll5'])

    def test_exit1_only_missing_offer_incomplete_can_continue(self):
        report=settled_report();report['outcome']='incomplete'
        self.assertTrue(r.safely_settled(report) and r.acceptable_exit(report,1))
        for failures in ([],['receipt missing']):
            changed=copy.deepcopy(report);changed['oracle']['liveness_failures']=failures
            self.assertFalse(r.acceptable_exit(changed,1))
        report['errors']=['actual engine error']
        self.assertFalse(r.acceptable_exit(report,1))
        self.assertFalse(r.acceptable_exit(settled_report(),3))

    def test_no_overwrite_initial_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'state.json';r.save(path,{'first':True},new=True)
            with self.assertRaises(FileExistsError):r.save(path,{'second':True},new=True)
            self.assertEqual(json.loads(path.read_text()),{'first':True})
            self.assertEqual(path.stat().st_mode & 0o777,0o600)

    def test_timeout_reaps_owned_process(self):
        with tempfile.TemporaryDirectory() as d:
            with (Path(d)/'log').open('w') as log:
                with self.assertRaises(subprocess.TimeoutExpired):
                    r.run_child([sys.executable,'-c','import signal,time; signal.signal(signal.SIGINT,signal.SIG_IGN);time.sleep(30)'],log,.1,graceful_seconds=.1)

    def test_native_pause_after_child_failure_and_global_latch(self):
        with tempfile.TemporaryDirectory() as d:
            s=state(d);path=Path(d)/'state.json'; calls=[]
            with patch.object(r,'verify_frozen'),patch.object(r.subprocess,'run',side_effect=lambda args,**kw:calls.append(args)),patch.object(r,'run_child',side_effect=RuntimeError('fake timeout')):
                with self.assertRaises(RuntimeError):r.run_one(s,path,s['jobs'][0])
            self.assertEqual([args[-2] for args in calls if 'docker' in args],['unpause','pause'])
            self.assertTrue(json.loads(path.read_text())['review_required'])


if __name__=='__main__':unittest.main()
