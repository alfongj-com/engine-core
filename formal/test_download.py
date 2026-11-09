"""Exercise pinned downloads over real HTTP, including process interruption."""
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import check
from download import DownloadError, download


PAYLOAD = b"the exact pinned release bytes\x00\xff"
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


@contextmanager
def server(action):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            try:
                action(self, len(requests))
            except (BrokenPipeError, ConnectionResetError):
                pass  # A deadline or interrupted client closed its connection.

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}/release?original=1", requests
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def respond(handler, body=PAYLOAD, *, status=200, length=None):
    handler.send_response(status)
    if length is not None:
        handler.send_header("Content-Length", str(length))
    handler.end_headers()
    handler.wfile.write(body)
    handler.wfile.flush()


class PinnedDownload(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="engine-download-test-")
        self.addCleanup(self.directory.cleanup)
        self.destination = Path(self.directory.name) / "tool"

    def assert_clean(self, expected=None):
        if expected is None:
            self.assertFalse(self.destination.exists())
        else:
            self.assertEqual(self.destination.read_bytes(), expected)
        self.assertEqual(list(self.destination.parent.glob("*.partial")), [])

    def test_transient_status_retries_original_url_with_unique_query(self):
        for status in (408, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                def action(handler, number):
                    respond(handler, b"retry" if number == 1 else PAYLOAD,
                            status=status if number == 1 else 200)

                with server(action) as (url, requests):
                    result = download(url, DIGEST, self.destination, retry_delay=0)
                self.assertEqual(result, self.destination)
                self.assert_clean(PAYLOAD)
                self.assertEqual(len(requests), 2)
                self.assertEqual(requests[0], "/release?original=1")
                retry = urlsplit(requests[1])
                self.assertEqual(retry.path, "/release")
                self.assertEqual(parse_qs(retry.query)["original"], ["1"])
                self.assertEqual(len(parse_qs(retry.query)["engine_download_retry"]), 1)

    def test_permanent_status_does_not_retry_or_overwrite(self):
        for status in (400, 401, 403, 404):
            with self.subTest(status=status):
                self.destination.write_bytes(PAYLOAD)
                with server(lambda h, _n: respond(h, b"denied", status=status)) as (url, requests):
                    with self.assertRaises(HTTPError):
                        download(url, DIGEST, self.destination, retry_delay=0)
                self.assertEqual(len(requests), 1)
                self.assert_clean(PAYLOAD)

    def test_invalid_bytes_or_framing_never_publish_or_overwrite(self):
        cases = [
            ("digest", lambda h, _n: respond(h, b"<html>proxy error</html>"), {}),
            ("malformed_length", lambda h, _n: respond(h, length="oops"), {}),
            ("truncated", lambda h, _n: respond(h, length=len(PAYLOAD) + 1), {}),
            ("oversize_header", lambda h, _n: respond(h, length=len(PAYLOAD)),
             {"max_bytes": len(PAYLOAD) - 1}),
            ("oversize_stream", lambda h, _n: respond(h), {"max_bytes": len(PAYLOAD) - 1}),
            ("bad_status_line", lambda h, _n: h.connection.sendall(b"INVALID\r\n\r\n"), {}),
            ("ambiguous_length", lambda h, _n: h.connection.sendall(
                b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx"), {}),
            ("broken_chunk", lambda h, _n: h.connection.sendall(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nnot-hex\r\n"), {}),
        ]
        for name, action, options in cases:
            for existing in (False, True):
                with self.subTest(case=name, existing=existing):
                    self.destination.unlink(missing_ok=True)
                    if existing:
                        self.destination.write_bytes(PAYLOAD)
                    with server(action) as (url, requests):
                        with self.assertRaises(DownloadError):
                            download(url, DIGEST, self.destination, retry_delay=0, **options)
                    self.assertEqual(len(requests), 1)
                    self.assert_clean(PAYLOAD if existing else None)

    def test_transport_retry_count_is_bounded(self):
        with server(lambda h, _n: respond(h, b"retry", status=503)) as (url, requests):
            with self.assertRaisesRegex(DownloadError, "after 3 attempts"):
                download(url, DIGEST, self.destination, attempts=3, retry_delay=0)
        self.assertEqual(len(requests), 3)
        self.assertEqual(len(set(requests)), 3)
        self.assert_clean()

    def test_connection_dropped_before_headers_retries(self):
        def action(handler, number):
            if number == 1:
                handler.close_connection = True
            else:
                respond(handler)

        with server(action) as (url, requests):
            download(url, DIGEST, self.destination, retry_delay=0)
        self.assertEqual(len(requests), 2)
        self.assertNotEqual(requests[0], requests[1])
        self.assert_clean(PAYLOAD)

    def test_absolute_body_deadline_cleans_partial_and_preserves_output(self):
        release = threading.Event()
        self.addCleanup(release.set)
        self.destination.write_bytes(PAYLOAD)

        def slow(handler, _number):
            respond(handler, PAYLOAD[:1], length=len(PAYLOAD))
            release.wait(timeout=2)
            handler.wfile.write(PAYLOAD[1:])

        with server(slow) as (url, requests):
            started = time.monotonic()
            with self.assertRaises(DownloadError):
                download(url, DIGEST, self.destination, deadline_seconds=0.15, retry_delay=0)
            elapsed = time.monotonic() - started
            release.set()
        self.assertLess(elapsed, 1)
        self.assertEqual(len(requests), 1)
        self.assert_clean(PAYLOAD)

    @unittest.skipUnless(os.name == "posix", "SIGINT cleanup uses POSIX process signals")
    def test_cli_interruption_cleans_partial_and_preserves_output(self):
        release = threading.Event()
        self.destination.write_bytes(PAYLOAD)

        def slow(handler, _number):
            respond(handler, PAYLOAD[:1], length=len(PAYLOAD))
            release.wait(timeout=5)
            handler.wfile.write(PAYLOAD[1:])

        with server(slow) as (url, _requests):
            child = subprocess.Popen([sys.executable, str(Path(__file__).with_name("download.py")),
                                      url, DIGEST, str(self.destination)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not list(self.destination.parent.glob("*.partial")):
                    self.assertIsNone(child.poll(), "Downloader exited before receiving the response")
                    self.assertLess(time.monotonic(), deadline, "Downloader did not start a partial file")
                    time.sleep(0.01)
                child.send_signal(signal.SIGINT)
                child.communicate(timeout=5)
                self.assertNotEqual(child.returncode, 0)
            finally:
                release.set()
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=5)
        self.assert_clean(PAYLOAD)

    def test_tlc_cache_is_verified_without_downloading(self):
        self.destination.write_bytes(PAYLOAD)
        with patch.dict(os.environ, {"TLA_JAR": str(self.destination)}), \
                patch.object(check, "TLC_SHA256", DIGEST), \
                patch.object(check, "download", side_effect=AssertionError("Unexpected download")):
            self.assertEqual(check.tlc_jar(), self.destination.resolve())
            self.destination.write_bytes(b"corrupt cache")
            with self.assertRaisesRegex(RuntimeError, "TLC checksum mismatch"):
                check.tlc_jar()
        self.assert_clean(b"corrupt cache")

    def test_tlc_missing_cache_uses_verified_download(self):
        with server(lambda h, _n: respond(h)) as (url, requests), \
                patch.dict(os.environ, {"TLA_JAR": str(self.destination)}), \
                patch.object(check, "TLC_SHA256", DIGEST), patch.object(check, "TLC_URL", url):
            self.assertEqual(check.tlc_jar(), self.destination.resolve())
        self.assertEqual(len(requests), 1)
        self.assert_clean(PAYLOAD)


if __name__ == "__main__":
    unittest.main()
