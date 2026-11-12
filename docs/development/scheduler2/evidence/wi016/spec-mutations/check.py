"""WI-016 read-only overlay mutation/shrink/standalone-replay gate.

Run with a fresh --output directory. Production source is never rewritten.
Compiler errors, panics, timeouts, flaky errors, or absent shrink evidence fail the
runner instead of counting as detection. Generated expected data must replay on
both the mutated binary (fail) and baseline binary (pass).
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


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--only", choices=["fold-late-base", "jitter-ignore-lookahead", "ignore-exclusions"])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[6]
    out = args.output.resolve()
    if out.exists() and any(out.iterdir()):
        raise RuntimeError("output directory must be fresh; preserve previous evidence")
    out.mkdir(parents=True, exist_ok=True)
    sandbox = out / "isolated-cwd"
    sandbox.mkdir()
    package = "./chasm/lib/scheduler2/internal/occurrence"
    tracked = ["service/worker/scheduler/calendar.go", "service/worker/scheduler/candidate.go",
               "service/worker/scheduler/spec.go", "chasm/lib/scheduler2/internal/occurrence/occurrence.go",
               "chasm/lib/scheduler2/internal/occurrence/occurrence_test.go",
               "chasm/lib/scheduler2/internal/occurrence/spec_boundaries_test.go", "go.mod", "go.sum"]
    tracked += [str(p.relative_to(root)) for p in (root / "chasm/lib/scheduler2/internal/occurrence/testdata").glob("*.tzif")]
    before = {p: sha(root / p) for p in tracked}
    env = dict(os.environ, CGO_ENABLED="0", GOMAXPROCS="4")
    manifest = {"status": "running", "checkout": str(root), "sources_before": before,
                "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "go_version": subprocess.check_output(["go", "version"], cwd=root, text=True).strip(),
                "rapid_version": "v1.2.0", "commands": [], "cases": []}

    def save():
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def run(argv, label, cwd, extra=None):
        started = time.monotonic()
        rec = {"argv": [str(a) for a in argv], "cwd": str(cwd), "environment":
               {"CGO_ENABLED": "0", "GOMAXPROCS": "4", **(extra or {})}, "log": label + ".log"}
        proc = subprocess.Popen(argv, cwd=cwd, env=dict(env, **(extra or {})),
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        try:
            data, _ = proc.communicate(timeout=180)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGKILL)
            data, _ = proc.communicate()
        (out / rec["log"]).write_bytes(data)
        rec.update(exit_code=proc.returncode, timed_out=timed_out, seconds=time.monotonic()-started)
        manifest["commands"].append(rec)
        save()
        if timed_out:
            raise RuntimeError(label + ": timed out")
        return proc.returncode, data.decode(errors="replace")

    def assertion_failure(code, log, test):
        return (code == 1 and re.search(r"--- FAIL: " + re.escape(test) + r"(?:\s|/|$)", log)
                and "panic:" not in log and "panic after" not in log and "flaky test" not in log
                and "[build failed]" not in log and "[setup failed]" not in log and "fatal error:" not in log)

    def property_args(binary, checks):
        return [binary, "-test.run=^TestSpecBoundaryProperties$", "-test.v", "-test.count=1", "-test.timeout=150s",
                "-rapid.checks=" + str(checks), "-rapid.seed=29", "-rapid.shrinktime=20s",
                "-rapid.nofailfile=true", "-rapid.debug=true"]

    cases = [
        ("fold-late-base", "service/worker/scheduler/calendar.go", "return earlier", "return result"),
        ("jitter-ignore-lookahead", "service/worker/scheduler/candidate.go",
         "maxJitter = min(maxJitter, following.Sub(nominal))", "maxJitter = timestamp.DurationValue(cs.spec.Jitter)"),
        ("ignore-exclusions", "service/worker/scheduler/candidate.go", "if cs.excluded(nominal) {", "if false && cs.excluded(nominal) {"),
    ]
    if args.only:
        cases = [c for c in cases if c[0] == args.only]
    try:
        baseline = out / "baseline.test"
        code, _ = run(["go", "test", "-c", "-tags", "test_dep", "-o", baseline, package], "baseline-compile", root)
        if code:
            raise RuntimeError("baseline compilation failed")
        manifest["baseline_binary_sha256"] = sha(baseline)
        code, _ = run(property_args(baseline, 100), "baseline-property", sandbox)
        if code:
            raise RuntimeError("baseline property failed")
        for name, relative, old, new in cases:
            source = (root / relative).read_text()
            if source.count(old) != 1:
                raise RuntimeError(name + ": replacement must match once")
            mutated = out / (name + ".go.txt")
            mutated.write_text(source.replace(old, new, 1))
            overlay = out / (name + "-overlay.json")
            overlay.write_text(json.dumps({"Replace": {str(root / relative): str(mutated)}}, indent=2) + "\n")
            binary = out / (name + ".test")
            record = {"name": name, "source": relative, "old": old, "new": new,
                      "original_sha256": before[relative], "mutant_sha256": sha(mutated),
                      "overlay": overlay.name, "detected": False, "minimized": False,
                      "standalone_mutant_failed": False, "standalone_baseline_passed": False}
            manifest["cases"].append(record)
            code, _ = run(["go", "test", "-c", "-tags", "test_dep", "-overlay", overlay, "-o", binary, package], name+"-compile", root)
            if code:
                raise RuntimeError(name + ": compilation failed, not detection")
            record["binary_sha256"] = sha(binary)
            code, log = run(property_args(binary, 1000), name+"-property", sandbox)
            record["detected"] = bool(assertion_failure(code, log, "TestSpecBoundaryProperties") and "[rapid] failed after" in log)
            shrunk = re.search(r"\[shrink\] done, (\d+) rounds total \((\d+) tries, (\d+) shrinks", log)
            if shrunk:
                record["shrink"] = dict(zip(["rounds", "tries", "shrinks"], map(int, shrunk.groups())))
                record["minimized"] = record["shrink"]["shrinks"] > 0
            if not record["detected"] or not record["minimized"]:
                raise RuntimeError(name + ": missing assertion detection or actual shrink")
            marker = "SPEC_BOUNDARY_REPLAY="
            offset = log.rfind(marker)
            if offset < 0:
                raise RuntimeError(name + ": missing explicit replay")
            scenario, _ = json.JSONDecoder().raw_decode(log[offset+len(marker):].lstrip())
            if scenario.get("version") != "spec-boundaries-v1":
                raise RuntimeError(name + ": unsupported replay version")
            replay = out / (name + "-minimized.json")
            replay.write_text(json.dumps(scenario, indent=2) + "\n")
            record["replay"] = replay.name
            record["replay_sha256"] = sha(replay)
            replay_args = ["-test.run=^TestSpecBoundarySavedReplay$", "-test.v", "-test.count=1", "-test.timeout=60s"]
            code, log = run([binary, *replay_args], name+"-standalone-mutant", sandbox, {"SCHEDULER2_SPEC_REPLAY": str(replay)})
            record["standalone_mutant_failed"] = bool(assertion_failure(code, log, "TestSpecBoundarySavedReplay") and "sequence:" in log)
            code, _ = run([baseline, *replay_args], name+"-standalone-baseline", sandbox, {"SCHEDULER2_SPEC_REPLAY": str(replay)})
            record["standalone_baseline_passed"] = code == 0
            if not record["standalone_mutant_failed"] or not record["standalone_baseline_passed"]:
                raise RuntimeError(name + ": standalone mutation/baseline replay mismatch")
            if {p: sha(root/p) for p in tracked} != before:
                raise RuntimeError("tracked source changed during overlay campaign")
            save()
            print(name + ": detected, shrunk, standalone replay failed; baseline replay passed", flush=True)
        code, _ = run(property_args(baseline, 100), "restored-baseline-property", sandbox)
        if code:
            raise RuntimeError("final baseline failed")
        manifest["status"] = "passed"
    except Exception as error:
        manifest["status"] = "failed"
        manifest["error"] = str(error)
        raise
    finally:
        manifest["sources_after"] = {p: sha(root/p) for p in tracked}
        manifest["checkout_unchanged"] = manifest["sources_after"] == before
        if not manifest["checkout_unchanged"]:
            manifest["status"] = "failed"
        save()


if __name__ == "__main__":
    main()
