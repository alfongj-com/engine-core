#!/usr/bin/env python3
"""Bounded local Nitro drain ticker; no Engine signer transactions.

Run with no arguments after the offered load stops. Owner terminates this process
when drained. A 600-second deadline bounds forgotten hooks; node/VM is not owned
by this script. Only the public synthetic Nitro development wallet is used.
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time

CAST = '/tmp/engine-capacity-tools/cast'
RPC = 'http://127.0.0.1:18547'
DEV_KEY = '0xb6b15c8cb491557369f3c7d2c287b053eb229daa9c22138887752191c9520659'
DEV_ADDRESS = '0x3f1eae7d46d88f08fc2f8ed27fcb2ab183eb2d0e'
stop = threading.Event()
active = None
started = time.monotonic()
deadline = started + 600
sent = 0


def terminate_group(proc):
    if proc is not None and proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def on_signal(_signum, _frame):
    stop.set()
    terminate_group(active)


def call(args):
    global active
    if stop.is_set() or time.monotonic() >= deadline:
        return None
    proc = subprocess.Popen([CAST, *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            start_new_session=True)
    active = proc
    try:
        # Covers a signal in the Popen-to-assignment interval.
        if stop.is_set():
            terminate_group(proc)
        out, err = proc.communicate(timeout=min(15, max(.01, deadline-time.monotonic())))
        if stop.is_set():
            return None
        if proc.returncode:
            raise RuntimeError('Local ticker RPC/transaction failed: ' + err.strip()[:500])
        return out.strip()
    finally:
        terminate_group(proc)
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=2)
        active = None


def main():
    global sent
    if len(sys.argv) != 1:
        raise SystemExit('No arguments: this hook is fixed to owned Nitro chain412346.')
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    identity = call(['chain-id', '--rpc-url', RPC])
    if identity is None:
        return
    if identity != '412346':
        raise RuntimeError('Refusing unexpected chain ID')
    print(json.dumps({'event': 'drain_ticker_start', 'chain_id': 412346,
                      'wallet': DEV_ADDRESS, 'max_seconds': 600}), flush=True)
    while not stop.is_set() and time.monotonic() < deadline:
        tick_start = time.monotonic()
        tx_hash = call(['send', '--rpc-url', RPC, '--private-key', DEV_KEY,
                       '--value', '0', '--async', DEV_ADDRESS])
        if tx_hash is None:
            break
        sent += 1
        print(json.dumps({'event': 'drain_ticker_broadcast', 'hash': tx_hash,
                          'count': sent, 'elapsed_seconds': time.monotonic()-started}), flush=True)
        stop.wait(max(0, min(1-(time.monotonic()-tick_start), deadline-time.monotonic())))
    print(json.dumps({'event': 'drain_ticker_stop', 'broadcasts': sent,
                      'elapsed_seconds': time.monotonic()-started,
                      'terminated_by_owner': stop.is_set()}), flush=True)


if __name__ == '__main__':
    try:
        main()
    finally:
        terminate_group(active)
