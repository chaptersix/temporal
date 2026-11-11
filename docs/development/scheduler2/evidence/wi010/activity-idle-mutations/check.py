"""Run five WI-010 one-fault checks, restoring exact source bytes after each case.

Run exclusively: this mutates production source temporarily, so no concurrent
builds/tests or source edits may use the checkout. A named Go assertion failure is
required; compiler errors, panics and timeouts do not count as detection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", type=Path, default=Path("/private/tmp/scheduler2-wi010/activity-idle-mutations"))
args = parser.parse_args()
root = Path(__file__).resolve().parents[6]
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
core = "chasm/lib/scheduler2/internal/decision/activity.go"
idle = "chasm/lib/scheduler2/idle.go"
cases = [
    ("anchor-overwrites-high-water", core,
     "if !root.ActivitySet || at > root.ActivityAt {", "if true {",
     "TestActivityControlAndRollbackClock", "./chasm/lib/scheduler2/internal/decision"),
    ("duplicate-observation-refreshes", core,
     'event.ChainID == op.ChainID && !op.StartObserved {', 'event.ChainID == op.ChainID {',
     "TestActivityTerminalObservation", "./chasm/lib/scheduler2/internal/decision"),
    ("admission-loses-unconfirmed-summary", core,
     "next.Root.UnconfirmedStarts += unconfirmed(next.Operation)\n", "next.Root.UnconfirmedStarts += 0\n",
     "TestActivitySummaryAcrossKeyedResults", "./chasm/lib/scheduler2/internal/decision"),
    ("idle-ignores-unconfirmed-hold", idle,
     " || state.Root.UnconfirmedStarts != 0", "",
     "TestIdleAmbiguousWaitAndObservedStartedEligibility", "./chasm/lib/scheduler2"),
    ("idle-accepts-stale-token-deadline", idle,
     " || task.Token != s.IdleToken || attrs.ScheduledTime.UnixNano() != s.IdleAt", "",
     "TestIdleRearmIsAtomicAndDoesNotDuplicate", "./chasm/lib/scheduler2"),
]
manifest = {
    "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "results": [],
}


def save_manifest():
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


for name, relative, old, new, test, package in cases:
    path = root / relative
    original = path.read_bytes()
    source = original.decode()
    if source.count(old) != 1:
        raise RuntimeError(f"{name}: expected exactly one replacement, found {source.count(old)}")
    mutant = source.replace(old, new, 1).encode()
    command = ["go", "test", "-p", "2", "-parallel", "4", "-tags", "test_dep", "-count=1", "-timeout=2m", package, "-run", "^" + test + "$", "-v"]
    result = {
        "mutation": name, "path": relative, "test": test,
        "original_sha256": hashlib.sha256(original).hexdigest(),
        "mutant_sha256": hashlib.sha256(mutant).hexdigest(),
        "old": old, "new": new, "command": command,
        "exit_code": None, "timed_out": False, "detected": False,
    }
    process = None
    try:
        path.write_bytes(mutant)
        with (out / (name + ".log")).open("w") as stream:
            process = subprocess.Popen(command, cwd=root,
                env=dict(os.environ, CGO_ENABLED="0", GOMAXPROCS="4"),
                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                process.communicate(timeout=150)
                result["exit_code"] = process.returncode
            except subprocess.TimeoutExpired:
                result["timed_out"] = True
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
        log = (out / (name + ".log")).read_text()
        named_failure = re.search(r"--- FAIL: " + re.escape(test) + r"(?:/|\s|$)", log) is not None
        result["detected"] = (
            result["exit_code"] == 1 and not result["timed_out"]
            and named_failure and "Error Trace:" in log
            and "[build failed]" not in log and "[setup failed]" not in log
            and "panic:" not in log and "fatal error:" not in log
        )
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        path.write_bytes(original)
        result["restored_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        result["restored"] = path.read_bytes() == original
        manifest["results"].append(result)
        save_manifest()
    if not result["restored"]:
        raise RuntimeError(f"{name}: source restoration failed")
    print(name, "detected=" + str(result["detected"]), flush=True)
if not all(result["detected"] and result["restored"] for result in manifest["results"]):
    raise SystemExit("Required named assertion detection missing; inspect logs and manifest")
