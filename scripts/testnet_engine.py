"""Local process/provenance helpers for the bounded public-testnet harnesses."""
import hashlib
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def provenance(binary, source_commit, harness):
    binary = Path(binary).expanduser().resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("Engine binary must be an executable file")
    if source_commit is not None and not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise RuntimeError("Binary source commit must be a full Git commit hash")
    return {"engine_binary_path": str(binary), "engine_binary_sha256": file_hash(binary),
            "declared_binary_source_commit": source_commit,
            "harness_source_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "harness_sha256": file_hash(harness),
            "process_helper_sha256": file_hash(__file__)}


def pinned_binary(evidence):
    path = Path(evidence["engine_binary_path"])
    if file_hash(path) != evidence["engine_binary_sha256"]:
        raise RuntimeError("Engine binary changed; retained intents require the pinned binary")
    return str(path)


def prepare_recovery(evidence, directory, env, *, resume=False):
    """Initialize once; a restarted Redis must pass exact-checkpoint reattach."""
    journal = Path(directory) / "recovery.sqlite"
    if env.get("APP__RECOVERY__JOURNAL_PATH") != str(journal):
        raise RuntimeError("Recovery journal must stay inside this private run")
    if journal.exists() != resume:
        raise RuntimeError("New runs need an empty journal; resumed runs need retained journal evidence")
    command = "--reattach-recovery" if resume else "--initialize-recovery"
    result = subprocess.run([pinned_binary(evidence), command], cwd=ROOT / "server", env=env,
                            capture_output=True, timeout=60)
    if result.returncode:
        # CLI output can contain operational details. Retain it privately only.
        path = Path(directory) / "recovery-setup.log"
        with path.open("ab") as stream:
            os.chmod(path, 0o600)
            stream.write(result.stdout + result.stderr)
        raise RuntimeError("Recovery setup refused; inspect private retained evidence")
