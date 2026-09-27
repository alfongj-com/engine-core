#!/usr/bin/env python3
"""Bounded local Nitro drain ticker; no Engine signer transactions.

Run after the offered load stops. Owner terminates this process when drained.
--max-seconds defaults to600 and cannot exceed1800; node/VM is not owned
by this script. Only the public synthetic Nitro development wallet is used.
"""
import argparse
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
started = 0.0
deadline = 0.0
sent = 0


def terminate_child(proc):
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


def on_signal(_signum, _frame):
    stop.set()
    terminate_child(active)


def call(args):
    global active
    if stop.is_set() or time.monotonic() >= deadline:
        return None
    # Inherit the campaign group: supervisor fallback also covers this known
    # direct Cast child, while ordinary owner signals reap it explicitly.
    proc = subprocess.Popen([CAST, *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            start_new_session=False)
    active = proc
    try:
        # Covers a signal in the Popen-to-assignment interval.
        if stop.is_set():
            terminate_child(proc)
        out, err = proc.communicate(timeout=min(15, max(.01, deadline-time.monotonic())))
        if stop.is_set():
            return None
        if proc.returncode:
            raise RuntimeError('Local ticker RPC/transaction failed: ' + err.strip()[:500])
        return out.strip()
    finally:
        terminate_child(proc)
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
        active = None


def main():
    global sent, started, deadline
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-seconds', type=int, default=600)
    args = parser.parse_args()
    if not 1 <= args.max_seconds <= 1800:
        parser.error('--max-seconds must be between1 and1800')
    started = time.monotonic()
    deadline = started + args.max_seconds
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    identity = call(['chain-id', '--rpc-url', RPC])
    if identity is None:
        return
    if identity != '412346':
        raise RuntimeError('Refusing unexpected chain ID')
    print(json.dumps({'event': 'drain_ticker_start', 'chain_id': 412346,
                      'wallet': DEV_ADDRESS, 'max_seconds': args.max_seconds}), flush=True)
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
        terminate_child(active)
