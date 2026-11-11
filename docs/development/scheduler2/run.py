#!/usr/bin/env python3
"""Run local scheduler2 verification and record exact evidence, including failures."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
PROFILES = ("regressions", "model", "properties", "simulation", "proofs", "fuzz", "race", "benchmarks", "enumeration", "completion", "completion-model", "sequential", "sequential-model", "buffer", "buffer-model", "allowall", "allowall-model", "effects", "effects-model", "manual", "manual-model", "backfill-range")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=PROFILES)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exploration", action="store_true")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--fizz", default="/private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads((HERE / "profile.json").read_text())
    env = dict(os.environ, GOMAXPROCS="4", CGO_ENABLED="0")
    package = "./chasm/lib/scheduler2"
    common = ["go", "test", "-p", "2", "-parallel", "4", "-tags", "test_dep", "-count=1", "-timeout=10m"]
    checks = 10000 if args.exploration else 100
    events = 17 if args.profile == "completion" else (128 if args.profile in ("sequential", "buffer", "allowall", "effects", "manual") or args.exploration else 32)
    commands = {
        "regressions": [common + [package, "-run", "Test(Replay|Snapshot|Lost|Failed|Metadata|Dropped|Independent|Task)", "-v"]],
        "backfill-range": [common + [package + "/internal/occurrence", "-run", "TestBackfillRange", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "manual": [common + [package, package + "/internal/decision", "-run", "TestManual", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "manual-model": [[sys.executable, str(HERE / "model/manual/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "effects": [common + [package, package + "/internal/decision", "-run", "TestEffects", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "effects-model": [[sys.executable, str(HERE / "model/effects/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "allowall": [common + [package, package + "/internal/decision", "-run", "TestAllowAll", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "allowall-model": [[sys.executable, str(HERE / "model/allowall/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "buffer": [common + [package, package + "/internal/decision", "-run", "TestBuffer", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "buffer-model": [[sys.executable, str(HERE / "model/buffer/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "sequential": [common + [package, package + "/internal/decision", "-run", "Test(Sequential|Generation)", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "sequential-model": [[sys.executable, str(HERE / "model/sequential/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "completion": [common + [package, package + "/internal/decision", "-run", "Test(Completion|Callback|Carryover|MetadataCompletion|CurrentPause|MalformedCallback)", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "completion-model": [[sys.executable, str(HERE / "model/completion/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "enumeration": [common + [package + "/internal/occurrence", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "properties": [common + [package + "/internal/decision", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-scheduler2.events=" + str(events)]],
        "simulation": [common + [package, "-run", "TestReplay", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "race": [["go", "test", "-race", "-p", "2", "-parallel", "4", "-tags", "test_dep", "-count=1", "-timeout=15m", package + "/...", "./chasm/lib/all", "-rapid.seed=" + str(args.seed)]],
        "model": [[sys.executable, str(HERE / "model/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
    }
    if args.profile == "race":
        env["CGO_ENABLED"] = "1"
        env["SDKROOT"] = "/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk"
        # all does not import Rapid and cannot accept its flags.
        commands["race"][0] = commands["race"][0][:-1]
    diff = subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=ROOT)
    (output / "source.diff").write_bytes(diff)
    files = list((ROOT / "chasm/lib/scheduler2").rglob("*.go")) + list((ROOT / "chasm/lib/scheduler2").rglob("*.proto"))
    files += [ROOT / "service/worker/scheduler" / name for name in ("candidate.go", "spec.go")]
    files += [p for p in (ROOT / "chasm/lib/scheduler2").rglob("*") if "testdata" in p.parts]
    files = [p for p in files if p.is_file()]
    manifest = {
        "profile": args.profile, "source_sha": git("rev-parse", "HEAD"),
        "status_before": git("status", "--short"), "diff_sha256": hashlib.sha256(diff).hexdigest(),
        "source_files": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)},
        "configuration": config, "seed": args.seed, "scenarios": checks, "event_limit": events,
        "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
        "python_version": sys.version, "environment": {k: env[k] for k in ("GOMAXPROCS", "CGO_ENABLED", "SDKROOT") if k in env},
        "trace_schema": 1, "results": [],
    }
    if args.profile not in commands:
        manifest["result"] = "unsupported"
        manifest["reason"] = "Deferred beyond M1; no completed Lean, fuzz target, or load benchmark is claimed"
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return 1
    passed = True
    for i, command in enumerate(commands[args.profile]):
        start = time.monotonic()
        log = output / f"{i}.log"
        try:
            with log.open("w") as stream:
                result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=1000)
            code = result.returncode
            outcome = "passed" if code == 0 else "failed"
        except (OSError, subprocess.TimeoutExpired) as exc:
            code, outcome = None, "blocked"
            with log.open("a") as stream:
                stream.write(str(exc) + "\n")
        passed &= outcome == "passed"
        manifest["results"].append({"command": command, "exit_code": code, "result": outcome, "seconds": time.monotonic() - start, "log": str(log)})
    manifest["result"] = "passed" if passed else "failed"
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"profile": args.profile, "result": manifest["result"], "manifest": str(output / "manifest.json")}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
