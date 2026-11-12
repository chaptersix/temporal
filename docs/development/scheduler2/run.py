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
PROFILES = ("regressions", "model", "properties", "simulation", "proofs", "fuzz", "race", "benchmarks", "enumeration", "completion", "completion-model", "sequential", "sequential-model", "buffer", "buffer-model", "allowall", "allowall-model", "effects", "effects-model", "manual", "manual-model", "backfill-range", "backfill", "backfill-model", "lifecycle", "lifecycle-model", "updates", "lean-correspondence")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def oracle_metadata(binary, build_manifest):
    """Require an already built, explicitly selected, fingerprinted local oracle."""
    binary, build_manifest = binary.resolve(), build_manifest.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("--oracle must name an existing executable file")
    build = json.loads(build_manifest.read_text())
    binary_sha = sha256(binary)
    if build.get("status") != "passed" or build.get("binary_sha256") != binary_sha:
        raise ValueError("oracle must match a passed --oracle-manifest binary SHA256")
    version_log = build_manifest.parent / "version.log"
    version = version_log.read_text().strip()
    if "version 4.24.0," not in version:
        raise ValueError("oracle manifest version.log must identify pinned Lean 4.24.0")
    return {
        "path": str(binary), "sha256": binary_sha, "wire_version": 1,
        "lean_version": version, "tool_sha256": build["tool_sha256"],
        "build_manifest": str(build_manifest), "build_manifest_sha256": sha256(build_manifest),
        "version_log": str(version_log), "version_log_sha256": sha256(version_log),
        "build_sources": build["sources"],
    }


def source_hashes():
    files = list((ROOT / "chasm/lib/scheduler2").rglob("*.go")) + list((ROOT / "chasm/lib/scheduler2").rglob("*.proto"))
    files += [ROOT / "service/worker/scheduler" / name for name in ("candidate.go", "spec.go")]
    files += [p for p in (ROOT / "chasm/lib/scheduler2").rglob("*") if "testdata" in p.parts]
    return {str(p.relative_to(ROOT)): sha256(p) for p in sorted(files) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=PROFILES)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exploration", action="store_true")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--fizz", default="/private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz")
    parser.add_argument("--oracle", type=Path, help="required local executable for lean-correspondence")
    parser.add_argument("--oracle-manifest", type=Path, help="required passed oracle build manifest, with adjacent version.log")
    args = parser.parse_args()
    lean = args.profile == "lean-correspondence"
    oracle = None
    if lean:
        if args.oracle is None or args.oracle_manifest is None or args.seed is None:
            parser.error("lean-correspondence requires explicit --oracle, --oracle-manifest and --seed")
        try:
            oracle = oracle_metadata(args.oracle, args.oracle_manifest)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            parser.error(str(exc))
    elif args.seed is None:
        args.seed = 1  # Preserve the existing profiles' default.
    output = args.output.resolve()
    if args.profile in ("lean-correspondence", "proofs") and output.exists() and any(output.iterdir()):
        parser.error(args.profile + " output directory must be new or empty")
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads((HERE / "profile.json").read_text())
    env = dict(os.environ, GOMAXPROCS="4", CGO_ENABLED="0")
    package = "./chasm/lib/scheduler2"
    common = ["go", "test", "-p", "2", "-parallel", "4", "-tags", "test_dep", "-count=1", "-timeout=10m"]
    checks = 10000 if args.exploration else 100
    events = 17 if args.profile == "completion" else (128 if args.profile in ("sequential", "buffer", "allowall", "effects", "manual") or args.exploration else 32)
    if args.profile == "lifecycle":
        events = config["lifecycle"]["scenario_events"]
    if args.profile == "updates":
        events = config["updates"]["scenario_events"]
    commands = {
        "proofs": [[sys.executable, str(HERE / "lean/exact/build.py"), "--out", str(output / "lean-build")]],
        "updates": [common + [package, "-run", "TestUpdate", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "regressions": [common + [package, "-run", "Test(Replay|Snapshot|Lost|Failed|Metadata|Dropped|Independent|Task)", "-v"]],
        "lifecycle": [common + [package, package + "/internal/decision", "-run", "Test(Activity|Idle|Lifecycle)", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "lifecycle-model": [[sys.executable, str(HERE / "model/lifecycle/run.py"), "--fizz", args.fizz, "--output", str(output / "model")]],
        "backfill": [common + [package, package + "/internal/decision", "-run", "Test(Coordinator|Backfill)", "-rapid.seed=" + str(args.seed), "-rapid.checks=" + str(checks), "-v"]],
        "backfill-model": [[sys.executable, str(HERE / "model/backfill/run.py"), "--fizz", args.fizz, "--output", str(output / "model")], [sys.executable, str(HERE / "model/backfill/run_capacity.py"), "--fizz", args.fizz, "--output", str(output / "capacity")]],
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
    if lean:
        lean_common = common.copy()
        lean_common[lean_common.index("test_dep")] = "test_dep,lean_correspondence"
        commands[args.profile] = [lean_common + [package + "/internal/occurrence",
            "-run", "^TestLeanCorrespondence", "-rapid.seed=" + str(args.seed),
            "-rapid.checks=" + str(checks), "-v"]]
        env["SCHEDULER2_LEAN_ORACLE"] = oracle["path"]
        # This named profile always runs all three checked-in saved traces.
        env.pop("SCHEDULER2_LEAN_REPLAY", None)
    if args.profile == "race":
        env["CGO_ENABLED"] = "1"
        env["SDKROOT"] = "/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk"
        # all does not import Rapid and cannot accept its flags.
        commands["race"][0] = commands["race"][0][:-1]
    diff = subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=ROOT)
    (output / "source.diff").write_bytes(diff)
    manifest = {
        "profile": args.profile, "source_sha": git("rev-parse", "HEAD"),
        "status_before": git("status", "--short"), "diff_sha256": hashlib.sha256(diff).hexdigest(),
        "source_files": source_hashes(),
        "configuration": config, "seed": args.seed, "scenarios": checks, "event_limit": events,
        "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
        "python_version": sys.version, "environment": {k: env[k] for k in ("GOMAXPROCS", "CGO_ENABLED", "SDKROOT") if k in env},
        "trace_schema": 1, "results": [],
    }
    if args.profile == "proofs":
        manifest["lean_proofs"] = {
            "build_manifest": str(output / "lean-build/manifest.json"),
            "toolchain": "/private/tmp/scheduler2-lean-toolchain/lean-4.24.0-darwin_aarch64/bin",
            "builder": str(HERE / "lean/exact/build.py"),
            "builder_sha256": sha256(HERE / "lean/exact/build.py"),
        }
        manifest["event_limit"] = None
        manifest["scenarios"] = None
    if lean:
        occurrence = ROOT / "chasm/lib/scheduler2/internal/occurrence"
        inputs = [occurrence / "lean_correspondence_test.go", HERE / "run.py", HERE / "profile.json"]
        inputs += sorted((occurrence / "testdata").glob("lean_correspondence_*.json"))
        manifest["lean_correspondence"] = {
            "oracle": oracle, "replay_version": 2,
            "rapid_checks_per_property": checks, "rapid_property_count": 2,
            "inputs": {str(p.relative_to(ROOT)): sha256(p) for p in inputs},
            "saved_replay_override": None,
        }
        manifest["trace_schema"] = 2
        manifest["event_limit"] = None  # The two properties have different bounds.
        manifest["environment"]["SCHEDULER2_LEAN_ORACLE"] = oracle["path"]
        manifest["environment"]["GOFLAGS"] = env.get("GOFLAGS", "")
    if args.profile not in commands:
        manifest["result"] = "unsupported"
        manifest["reason"] = "No implemented command for this fuzz or load benchmark profile; no passing evidence is claimed"
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
        if lean:
            lines = log.read_text().splitlines()
            missing = [name for name in config["lean_correspondence"]["required_tests"]
                if not any(line.startswith("--- PASS: " + name + " ") for line in lines)]
            manifest["lean_correspondence"]["missing_required_tests"] = missing
            if missing:
                outcome = "failed"
        passed &= outcome == "passed"
        manifest["results"].append({"command": command, "exit_code": code, "result": outcome, "seconds": time.monotonic() - start, "log": str(log)})
    if lean:
        # Fail closed if evidence inputs or the selected binary drift during a run.
        try:
            oracle_after = oracle_metadata(args.oracle, args.oracle_manifest)
            inputs_after = {str(p.relative_to(ROOT)): sha256(p) for p in inputs}
            manifest["source_files_after"] = source_hashes()
            source_unchanged = manifest["source_files_after"] == manifest["source_files"]
            unchanged = source_unchanged and oracle_after == oracle and inputs_after == manifest["lean_correspondence"]["inputs"]
            manifest["lean_correspondence"]["production_sources_unchanged"] = source_unchanged
            manifest["lean_correspondence"]["inputs_after"] = inputs_after
            manifest["lean_correspondence"]["oracle_after"] = oracle_after
            manifest["lean_correspondence"]["inputs_unchanged"] = unchanged
            passed &= unchanged
        except (OSError, ValueError, KeyError, TypeError) as exc:
            passed = False
            manifest["lean_correspondence"]["postcheck_error"] = str(exc)
    if args.profile == "proofs":
        try:
            build_manifest = output / "lean-build/manifest.json"
            build = json.loads(build_manifest.read_text())
            manifest["lean_proofs"]["build_manifest_sha256"] = sha256(build_manifest)
            manifest["lean_proofs"]["status"] = build["status"]
            manifest["lean_proofs"]["sources"] = build["sources"]
            manifest["lean_proofs"]["sources_after"] = build["sources_after"]
            manifest["lean_proofs"]["tool_sha256"] = build["tool_sha256"]
            manifest["lean_proofs"]["lean_version"] = (output / "lean-build/version.log").read_text().strip()
            manifest["lean_proofs"]["binary_sha256"] = build.get("binary_sha256")
            passed &= build["status"] == "passed" and build["sources"] == build["sources_after"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            passed = False
            manifest["lean_proofs"]["metadata_error"] = str(exc)
    manifest["result"] = "passed" if passed else "failed"
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"profile": args.profile, "result": manifest["result"], "manifest": str(output / "manifest.json")}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
