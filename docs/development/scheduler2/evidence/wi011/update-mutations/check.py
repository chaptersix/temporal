"""Run WI-011 atomic-update one-fault checks, restoring exact source bytes after each case.

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
parser.add_argument("--output", type=Path, default=Path("/private/tmp/scheduler2-wi011/update-mutations"))
parser.add_argument("--only", help="Run one named mutation")
args = parser.parse_args()
root = Path(__file__).resolve().parents[6]
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
generator = "chasm/lib/scheduler2/generator.go"
update = "chasm/lib/scheduler2/update.go"
cases = [
    ("update-drops-pause-replacement", generator,
     "s.Paused = preview.Root.Paused", "s.Paused = false",
     "TestUpdateConfigurationPublishesWholeReplacement", "./chasm/lib/scheduler2"),
    ("stale-update-reports-success", update,
     'return serviceerror.NewFailedPrecondition("configuration revision must advance")', 'return nil',
     "TestUpdateConfigurationRejectsBeforeWrites", "./chasm/lib/scheduler2"),
    ("idle-preflight-uses-old-controls", generator,
     's.checkIdlePlan(preview, encodeContinuation(state), true, len(s.Backfills) > 0)',
     's.checkIdlePlan(s.stateFor(ctx, ""), encodeContinuation(state), true, len(s.Backfills) > 0)',
     "TestUpdateConfigurationRejectsBeforeWrites", "./chasm/lib/scheduler2"),
    ("update-rewrites-admitted-requests", generator,
     "g.Continuation = encodeContinuation(state)",
     "for id := range s.Requests {\n\t\ts.Requests[id] = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: request})\n\t}\n\tg.Continuation = encodeContinuation(state)",
     "TestUpdateConfigurationPreservesAdmittedEffectsAndWaiters", "./chasm/lib/scheduler2"),
    ("update-bypasses-request-dedup", update,
     "}, config, chasm.WithRequestID(requestID))", '}, config, chasm.WithRequestID(""))',
     "TestUpdateRequestDedupAndAtomicPublication", "./chasm/lib/scheduler2"),
    ("update-drops-start-obligations-replay", generator,
     "g.Continuation = encodeContinuation(state)",
     "s.PendingStarts = nil\n\tg.Continuation = encodeContinuation(state)",
     "TestUpdateSavedReplay", "./chasm/lib/scheduler2"),
    ("update-drops-start-obligations", generator,
     "g.Continuation = encodeContinuation(state)",
     "s.PendingStarts = nil\n\tg.Continuation = encodeContinuation(state)",
     "TestUpdateComposedProperties", "./chasm/lib/scheduler2"),
]
if args.only:
    cases = [case for case in cases if case[0] == args.only]
    if not cases:
        raise SystemExit("Unknown mutation: " + args.only)

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
    if test == "TestUpdateComposedProperties":
        command += ["-rapid.seed=1", "-rapid.checks=100"]
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
