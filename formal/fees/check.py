#!/usr/bin/env python3
"""Verify production fee arithmetic and require specific mutation counterexamples."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
VERSION = "0.68.0"
SOURCE = Path("executors/src/eoa/worker/fee_math.rs")
PROOFS = Path("formal/fees/src/lib.rs")
HARNESS_NAMES = {
    "all_multipliers_respect_caps_and_priority",
    "all_increases_are_nondecreasing_when_cap_permits",
    "actual_parts_match_mathematical_saturation_threshold",
    "division_parts_reconstruct_all_inputs",
    "actual_dynamic_bump_preserves_valid_order_and_values",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def fingerprints():
    return {str(path): hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in [SOURCE, PROOFS, Path("formal/fees/check.py")]}


def verify(manifest, output, label, timeout, jobs, harness=None, expected_failure=None):
    report = output / (label + ".json")
    log = output / (label + ".log")
    # An old success must never mask a compiler error before JSON emission.
    report.unlink(missing_ok=True)
    command = ["cargo", "kani", "--manifest-path", str(manifest),
               "--output-format", "terse", "-Z", "unstable-options",
               "--harness-timeout", f"{timeout}s", "--export-json", str(report),
               "-j", str(jobs)]
    if harness:
        command += ["--harness", "proofs::" + harness, "--exact"]
    print(f"Checking {label}...", flush=True)
    started = time.monotonic()
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=timeout * (len(HARNESS_NAMES) if not harness else 1) + 120)
        finally:
            # Include solver descendants if a verifier timeout leaves one alive.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            process.wait()
    require(report.exists(), f"{label}: verifier did not produce JSON; see {log}")
    data = json.loads(report.read_text())
    require(data["tools"]["kani"] == VERSION, "Unexpected verifier version")
    results = data["verification_results"]["results"]
    expected = {"proofs::" + name for name in ([harness] if harness else HARNESS_NAMES)}
    require({result["harness_id"] for result in results} == expected,
            f"{label}: missing or unexpected harnesses")
    failed_checks = [check for result in results for check in result["checks"]
                     if check["status"] == "Failure"]
    if expected_failure:
        require(code != 0 and all(result["status"] == "Failure" for result in results),
                f"{label}: expected a verification failure")
        require(any(check["category"] == "assertion" and expected_failure in check["description"]
                    for check in failed_checks), f"{label}: required assertion did not fail")
        require(all(error["error_type"] == "assertion_failure" for error in data["error_details"]),
                f"{label}: unrelated verifier failure")
    else:
        require(code == 0 and all(result["status"] == "Success" for result in results),
                f"{label}: proof did not succeed; see {log}")
        require(not failed_checks, f"{label}: failed checks")
    return {"label": label, "status": "expected_counterexample" if expected_failure else "proved",
            "seconds": round(time.monotonic() - started, 3), "command": command,
            "harnesses": [{"name": result["harness_id"], "status": result["status"],
                           "checks": len(result["checks"]), "duration_ms": result["duration_ms"]}
                          for result in results],
            "expected_assertion": expected_failure,
            "failed_checks": [{key: check[key] for key in ["category", "description"]}
                              for check in failed_checks]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "formal/fees/results")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--jobs", type=int, default=3)
    args = parser.parse_args()
    require(30 <= args.timeout <= 3600 and 1 <= args.jobs <= 8, "Invalid resource bounds")
    version = subprocess.check_output(["cargo", "kani", "--version"], text=True)
    require(re.search(r"Kani Rust Verifier ([\d.]+)", version).group(1) == VERSION,
            f"Install kani-verifier {VERSION}")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    hashes = fingerprints()
    outcome = {"status": "incomplete", "kani_version": VERSION, "source_sha256": hashes, "runs": []}
    try:
        outcome["runs"].append(verify(ROOT / "formal/fees/Cargo.toml", output, "production",
                                      args.timeout, args.jobs))
        mutations = [
            ("removed_cap", "bumped.min(cap.unwrap_or(u128::MAX))", "bumped",
             "all_multipliers_respect_caps_and_priority", "new_fee <= fee_cap.unwrap_or(u128::MAX)"),
            ("wrapping_multiply", ".saturating_mul(multiplier)", ".wrapping_mul(multiplier)",
             "all_increases_are_nondecreasing_when_cap_permits",
             "fee_math::capped_increase(value, multiplier, Some(cap)) >= value"),
        ]
        for label, old, new, harness, assertion in mutations:
            with tempfile.TemporaryDirectory(prefix="engine-fee-mutation-") as directory:
                mirror = Path(directory)
                for path in [SOURCE, PROOFS, Path("formal/fees/Cargo.toml"), Path("formal/fees/Cargo.lock")]:
                    target = mirror / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(ROOT / path, target)
                target = mirror / SOURCE
                text = target.read_text()
                require(text.count(old) == 1, f"{label}: production mutation anchor changed")
                target.write_text(text.replace(old, new))
                outcome["runs"].append(verify(mirror / "formal/fees/Cargo.toml", output, label,
                                              args.timeout, 1, harness, assertion))
        require(fingerprints() == hashes, "Source changed during verification")
        outcome["status"] = "pass"
    finally:
        (output / "summary.json").write_text(json.dumps(outcome, indent=2) + "\n")
    print(json.dumps(outcome, indent=2))


if __name__ == "__main__":
    main()
