"""WI013 correspondence sensitivity. Requires an exclusive checkout mutation window.

Each single production fault must cause a direct Rapid Go/Lean assertion, shrink,
produce concrete JSON, and fail the standalone non-Rapid replay. Restored source
must pass that same replay. A seed, compiler failure, timeout or crash is not
sensitivity evidence. Byte restoration runs in finally; no oracle mutation occurs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--oracle", type=Path, required=True)
parser.add_argument("--output", type=Path, default=Path("/private/tmp/scheduler2-wi013/correspondence-mutations"))
parser.add_argument("--only", choices=["lost-pending", "cursor-skip", "duplicate-output", "lost-superseded"])
args = parser.parse_args()
root = Path(__file__).resolve().parents[6]
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
oracle = args.oracle.resolve()
if not oracle.is_file():
    raise SystemExit("Required independently compiled oracle is missing")
source_path = root / "chasm/lib/scheduler2/internal/occurrence/occurrence.go"
original = source_path.read_bytes()
test_path = root / "chasm/lib/scheduler2/internal/occurrence/lean_correspondence_test.go"


def digest(data):
    return hashlib.sha256(data).hexdigest()


manifest = {
    "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "source_sha256": digest(original),
    "test_sha256": digest(test_path.read_bytes()),
    "oracle_path": str(oracle), "oracle_sha256": digest(oracle.read_bytes()),
    "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
    "rapid_seed": 1, "rapid_checks": 100, "shrink_timeout": "10s",
    "comparison": "full mapped state/result/status/probes/superseded/error against immutable external Lean oracle",
    "minimization_limit": "Rapid's final shrunk failing draws; no global size-minimality claim. Concrete JSON replay is additionally required.",
    "results": [],
}


def save():
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def run(name, test, replay=None, rapid=False):
    command = ["go", "test", "-p", "2", "-tags", "test_dep,lean_correspondence", "-count=1", "-timeout=90s",
               "./chasm/lib/scheduler2/internal/occurrence", "-run", "^" + test + "$", "-v"]
    if rapid:
        command += ["-rapid.seed=1", "-rapid.checks=100", "-rapid.shrinktime=10s", "-rapid.nofailfile"]
    env = dict(os.environ, CGO_ENABLED="0", GOMAXPROCS="4", SCHEDULER2_LEAN_ORACLE=str(oracle))
    env.pop("SCHEDULER2_LEAN_REPLAY", None)
    if replay is not None:
        env["SCHEDULER2_LEAN_REPLAY"] = str(replay)
    result = {"command": command, "replay": str(replay) if replay else None, "timed_out": False}
    start = time.monotonic()
    with (out / (name + ".log")).open("w") as stream:
        process = subprocess.Popen(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            process.communicate(timeout=110)
        except subprocess.TimeoutExpired:
            result["timed_out"] = True
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
    result["exit_code"] = process.returncode
    result["elapsed_seconds"] = round(time.monotonic() - start, 3)
    log = (out / (name + ".log")).read_text()
    result["log_sha256"] = digest(log.encode())
    bad_infrastructure = any(marker in log for marker in ["[build failed]", "[setup failed]", "panic:", "fatal error:", "oracle response missing", "oracle stderr:"])
    result["assertion_detected"] = (process.returncode == 1 and not result["timed_out"] and not bad_infrastructure
        and re.search(r"--- FAIL: " + re.escape(test) + r" \(", log) is not None and "Go=" in log and "Lean=" in log)
    result["passed"] = process.returncode == 0 and not result["timed_out"]
    return result, log


baseline, _ = run("baseline", "TestLeanCorrespondenceRapid", rapid=True)
manifest["baseline"] = baseline
save()
if not baseline["passed"]:
    raise SystemExit("Baseline did not pass; no mutation applied")

cases = [
    ("lost-pending",
     "result.State.Pending = &Candidate{Nominal: candidate.Nominal, Actual: candidate.Next, Revision: e.revision}",
     "result.State.Pending = nil\n\tresult.State.DisposedThrough = candidate.Nominal"),
    ("cursor-skip", "after := result.State.SearchAfter", "after := result.State.SearchAfter.Add(time.Second)"),
    ("duplicate-output", "r.Occurrences = append(r.Occurrences, *pending)", "r.Occurrences = append(r.Occurrences, *pending, *pending)"),
    ("lost-superseded", "return next, old.Pending, nil", "old.Pending = nil\n\treturn next, old.Pending, nil"),
]
for name, old, new in cases:
    if args.only and name != args.only:
        continue
    if source_path.read_bytes() != original:
        raise RuntimeError("Source changed outside this runner; refusing to overwrite")
    text = original.decode()
    if text.count(old) != 1:
        raise RuntimeError(f"{name}: expected one replacement, found {text.count(old)}")
    mutant = text.replace(old, new, 1).encode()
    row = {"name": name, "old": old, "new": new, "mutant_sha256": digest(mutant)}
    replay = None
    try:
        source_path.write_bytes(mutant)
        property_name = "TestLeanCorrespondenceRapidWindowsAndUpdates" if name == "lost-superseded" else "TestLeanCorrespondenceRapid"
        row["property"] = property_name
        row["rapid"], log = run(name, property_name, rapid=True)
        # Rapid's final rerun prints the minimized concrete failing trace. Keep
        # every failure log as well, so the shrinking/provenance is auditable.
        traces = re.findall(r"lean-replay=(\{[^\n]*\})", log)
        row["concrete_trace_count"] = len(traces)
        if traces:
            trace = json.loads(traces[-1])
            replay = out / (name + "-minimized.json")
            replay.write_text(json.dumps(trace, indent=2) + "\n")
            row["replay_sha256"] = digest(replay.read_bytes())
            row["minimized_replay"], _ = run(name + "-replay-mutant", "TestLeanCorrespondenceSavedReplay", replay)
    finally:
        source_path.write_bytes(original)
        row["restored"] = source_path.read_bytes() == original
        row["restored_sha256"] = digest(source_path.read_bytes())
        manifest["results"].append(row)
        save()
    if replay is not None:
        row["restored_replay"], _ = run(name + "-replay-restored", "TestLeanCorrespondenceSavedReplay", replay)
    row["detected"] = (row["rapid"]["assertion_detected"] and row["minimized_replay"]["assertion_detected"]
                       and row["restored_replay"]["passed"] and row["restored"]) if replay is not None else False
    save()
    print(name, "detected=" + str(row["detected"]), flush=True)

if not all(row["detected"] for row in manifest["results"]):
    raise SystemExit("Required assertion/shrink/standalone replay evidence incomplete; inspect manifest and logs")
