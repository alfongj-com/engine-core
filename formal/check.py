#!/usr/bin/env python3
"""Run finite TLC models and require specific counterexamples from fault models.

Uses only the Python standard library and Java 11+. No RPC credentials or chain
access. Downloaded TLC bytes are checked against a pinned digest before execution.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
HERE = ROOT / "formal"
TLC_VERSION = "1.7.4"
TLC_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLC_URL = f"https://github.com/tlaplus/tlaplus/releases/download/v{TLC_VERSION}/tla2tools.jar"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tlc_jar():
    path = Path(os.environ.get("TLA_JAR", HERE / ".cache" / f"tla2tools-{TLC_VERSION}.jar"))
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        # Validate before publishing a cache entry; interrupted downloads cannot
        # masquerade as a verified tool on the next invocation.
        with urllib.request.urlopen(TLC_URL, timeout=60) as response:
            data = response.read(16 * 1024 * 1024)
        if hashlib.sha256(data).hexdigest() != TLC_SHA256:
            raise RuntimeError("Downloaded TLC checksum does not match the pinned release")
        temporary = path.with_suffix(".download")
        temporary.write_bytes(data)
        temporary.replace(path)
    if sha256(path) != TLC_SHA256:
        raise RuntimeError(f"TLC checksum mismatch: {path}")
    return path.resolve()


def classify_output(output, code, expected):
    """A parser error, resource failure or unrelated invariant is never a pass."""
    if expected == "pass":
        return (code == 0 and "Model checking completed. No error has been found." in output
                and re.search(r"\b0 states left on queue\.", output) is not None
                and "Error:" not in output)
    if expected.startswith("invariant:"):
        name = expected.split(":", 1)[1]
        return (code == 12 and f"Error: Invariant {name} is violated." in output
                and "State 1:" in output and "Error: The behavior up to this point is:" in output)
    if expected == "liveness":
        return (code == 13 and "Error: Temporal properties were violated." in output
                and "State 1:" in output)
    raise ValueError(f"Unknown expected result: {expected}")


def check_source_map():
    mapping = json.loads((HERE / "source-map.json").read_text())
    if not mapping["files"]:
        raise RuntimeError("The modeled-source map is empty")
    stale = []
    for item in mapping["files"]:
        path = ROOT / item["path"]
        if not path.is_file() or sha256(path) != item["sha256"]:
            stale.append(item["path"])
    if stale:
        raise RuntimeError("Modeled source changed; review models and the coverage map, then update "
                           "formal/source-map.json. This guard is NOT a refinement proof.\n"
                           + "\n".join(stale))
    return mapping


def run_model(case, java, jar, results, timeout):
    started = time.monotonic()
    model_hash = sha256(HERE / "tla" / (case["module"] + ".tla"))
    config_hash = sha256(HERE / "tla" / case["config"])
    with tempfile.TemporaryDirectory(prefix="engine-tlc-") as tmp:
        command = [java, "-XX:+UseParallelGC", "-Xmx2g", "-cp", str(jar), "tlc2.TLC",
                   "-workers", "1", "-seed", "1", "-fp", "0", "-cleanup",
                   "-metadir", tmp, "-coverage", "999",
                   "-config", case["config"], case["module"] + ".tla"]
        try:
            proc = subprocess.run(command, cwd=HERE / "tla", text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=timeout)
            output, code = proc.stdout, proc.returncode
        except subprocess.TimeoutExpired as exc:
            output = exc.stdout or b""
            if isinstance(output, bytes):
                output = output.decode("utf-8", errors="replace")
            output += f"\nRUNNER TIMEOUT after {timeout}s\n"
            code = -1
    log_name = Path(case["config"]).stem + ".log"
    (results / log_name).write_text(output)
    counts = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left", output)
    count = counts[-1] if counts else None
    record = dict(case, passed=classify_output(output, code, case["expected"]),
                  exit_code=code, seconds=round(time.monotonic() - started, 3), log=log_name,
                  generated=int(count[0].replace(",", "")) if count else None,
                  distinct=int(count[1].replace(",", "")) if count else None,
                  pending=int(count[2].replace(",", "")) if count else None,
                  model_sha256=sha256(HERE / "tla" / (case["module"] + ".tla")),
                  config_sha256=sha256(HERE / "tla" / case["config"]))
    if (model_hash, config_hash) != (record["model_sha256"], record["config_sha256"]):
        record["passed"] = False
        record["error"] = "Model/configuration changed during verification"
    print(f"{'PASS' if record['passed'] else 'FAIL'} {case['config']}: "
          f"{record['distinct']} states, expected {case['expected']}, {record['seconds']}s", flush=True)
    if not record["passed"]:
        print(output[-6000:], file=sys.stderr)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="Run configs whose name contains this text")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    args.results.mkdir(parents=True, exist_ok=True)
    (args.results / "report.json").unlink(missing_ok=True)
    sources = check_source_map()
    cases = json.loads((HERE / "models.json").read_text())
    listed = [case["config"] for case in cases]
    available = {path.name for path in (HERE / "tla").glob("*.cfg")}
    if len(set(listed)) != len(listed) or set(listed) != available:
        raise RuntimeError("Every TLC configuration must appear exactly once in models.json")
    if args.only:
        cases = [case for case in cases if args.only in case["config"]]
    if not cases:
        raise RuntimeError("No models selected")
    java = os.environ.get("JAVA", "java")
    if not shutil.which(java):
        raise RuntimeError("Install Java 11+ or set JAVA to its executable")
    version = subprocess.run([java, "-version"], capture_output=True, text=True, check=True)
    jar = tlc_jar()
    records = [run_model(case, java, jar, args.results, args.timeout) for case in cases]
    if check_source_map() != sources:
        raise RuntimeError("Modeled source map changed during verification")
    report = {"scope": "finite models; not a Rust/Redis refinement proof",
              "tlc_version": TLC_VERSION, "tlc_sha256": TLC_SHA256,
              "java": version.stderr.strip(), "sources": sources,
              "cases": records, "passed": all(r["passed"] for r in records)}
    (args.results / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        print(f"Verification failed: {exc}", file=sys.stderr)
        sys.exit(1)
