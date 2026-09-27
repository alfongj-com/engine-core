#!/usr/bin/env python3
"""Explicit old-Engine v4 control launcher; preview unless v4 receives --execute.

This launcher never copies/replaces the current release. Use the identical
launcher for preparation and resumption. The launcher and supervisor bytes are
both included in the frozen state's hashes.
"""
import argparse
import importlib.util
from pathlib import Path
import sys

ENGINE = Path('/tmp/engine-capacity-pre-dispatch-bin/thirdweb-engine')
EXPECTED = '300db9782868ddca5b6d552e7ead5d2621322a82c8297ffde8f6ecb067858b23'


def main():
    outer = argparse.ArgumentParser(add_help=False)
    outer.add_argument('--supervisor', type=Path, required=True)
    opts, remaining = outer.parse_known_args()
    path = opts.supervisor.resolve()
    spec = importlib.util.spec_from_file_location('capacity_v4_control', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if module.sha(ENGINE) != EXPECTED:
        raise RuntimeError('Old control Engine differs from pinned300db artifact')
    module.ENGINE = ENGINE
    initialize = module.initialize
    def initialize_control(args):
        state = initialize(args)
        state['frozen_sha256'][str(Path(__file__).resolve())] = module.sha(__file__)
        return state
    module.initialize = initialize_control
    sys.argv = [str(path), *remaining]
    module.main()


if __name__ == '__main__':
    main()
