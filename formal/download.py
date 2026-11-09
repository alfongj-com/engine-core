#!/usr/bin/env python3
"""Fetch a checksum-pinned build tool; retry transport failures at the same URL."""
import argparse
import hashlib
import http.client
import math
import os
from pathlib import Path
import re
import socket
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class DownloadError(RuntimeError):
    pass


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise DownloadError("Download deadline exceeded")
    return remaining


def _retry_url(url):
    parts = urllib.parse.urlsplit(url)
    query = parts.query + ("&" if parts.query else "")
    query += "engine_download_retry=" + uuid.uuid4().hex
    return urllib.parse.urlunsplit(parts._replace(query=query))


def download(url, expected_sha256, destination, *, attempts=4, max_bytes=1024**3,
             deadline_seconds=300, retry_delay=1):
    """Publish only verified bytes. Existing output survives every failed attempt.

    HTTP integrity/framing failures are permanent. Only transport failures and
    408/429/5xx responses retry; retries cache-bust the original release URL.
    Check the overall deadline between chunks and before publication. A body
    read already in progress can overrun it by at most its 10-second I/O timeout.
    """
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("Download URL must use HTTP or HTTPS")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256):
        raise ValueError("Expected SHA256 must contain 64 hexadecimal characters")
    if not 1 <= attempts <= 8 or max_bytes <= 0:
        raise ValueError("Download needs 1..8 attempts and a positive byte limit")
    if (not math.isfinite(deadline_seconds) or deadline_seconds <= 0
            or not math.isfinite(retry_delay) or retry_delay < 0):
        raise ValueError("Download needs a positive finite deadline and nonnegative retry delay")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + deadline_seconds
    expected_sha256 = expected_sha256.lower()
    last_error = None
    for attempt in range(attempts):
        temporary = None
        try:
            request_url = url if attempt == 0 else _retry_url(url)
            request = urllib.request.Request(request_url, headers={"Cache-Control": "no-cache"})
            with urllib.request.urlopen(request, timeout=min(10, _remaining(deadline))) as response:
                _remaining(deadline)
                lengths = response.headers.get_all("Content-Length", [])
                if len(lengths) > 1 or (lengths and response.headers.get("Transfer-Encoding")):
                    raise DownloadError("Ambiguous HTTP response framing")
                length = lengths[0] if lengths else None
                if length is not None:
                    if not re.fullmatch(r"[0-9]+", length):
                        raise DownloadError("Malformed HTTP Content-Length")
                    length = int(length)
                    if length > max_bytes:
                        raise DownloadError("Download exceeds byte limit")
                descriptor, name = tempfile.mkstemp(
                    prefix="." + destination.name + ".", suffix=".partial",
                    dir=destination.parent,
                )
                temporary = Path(name)
                digest, total = hashlib.sha256(), 0
                with os.fdopen(descriptor, "wb") as output:
                    while True:
                        _remaining(deadline)
                        # read1 returns available data without waiting to fill
                        # the entire buffer, so trickling bodies reach this check.
                        chunk = response.read1(min(65536, max_bytes - total + 1))
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise DownloadError("Download exceeds byte limit")
                        digest.update(chunk)
                        output.write(chunk)
                    if length is not None and total != length:
                        raise DownloadError("Truncated HTTP response")
                    if digest.hexdigest() != expected_sha256:
                        raise DownloadError("Downloaded SHA256 does not match the pinned release")
                    output.flush()
                    os.fsync(output.fileno())
                _remaining(deadline)
                temporary.replace(destination)
                return destination
        except urllib.error.HTTPError as error:
            error.close()
            if error.code not in {408, 429} and not 500 <= error.code <= 599:
                raise
            last_error = error
        except (urllib.error.URLError, TimeoutError, socket.timeout, ConnectionError) as error:
            last_error = error
        except http.client.HTTPException as error:
            raise DownloadError(f"Malformed HTTP response: {error}") from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        if attempt + 1 < attempts:
            time.sleep(min(retry_delay * (attempt + 1), _remaining(deadline)))
    raise DownloadError(f"Download failed after {attempts} attempts: {last_error}") from last_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("sha256")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--attempts", type=int, default=4)
    parser.add_argument("--max-bytes", type=int, default=1024**3)
    parser.add_argument("--deadline-seconds", type=float, default=300)
    args = parser.parse_args()
    try:
        download(args.url, args.sha256, args.destination, attempts=args.attempts,
                 max_bytes=args.max_bytes, deadline_seconds=args.deadline_seconds)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"Download failed: {error}\n")


if __name__ == "__main__":
    main()
