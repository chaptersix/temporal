#!/usr/bin/env python3
"""Check the independent factored lifecycle model."""
import argparse
import hashlib
import json
import os
import signal
from pathlib import Path
import re
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--fizz", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
sources = {name: Path(__file__).with_name(name + ".fizz").read_text()
    for name in ["anchor", "idle"]}
args.output.mkdir(parents=True, exist_ok=True)
cases = [(f"{model}-{bound}", model, bound, None)
    for model in sources for bound in [100, 200]]
mutations = {
    "ANCHOR_REGRESSES": ("anchor", ["AnchorMatchesAcceptedEvidence"]),
    "DUPLICATE_ACK_REFRESH": ("anchor", ["FirstObservationImmutable", "AnchorMatchesAcceptedEvidence"]),
    "CLOSE_WHILE_PENDING": ("idle", ["NoUnauthorizedClose"]),
    "LOST_REARM": ("idle", ["StableIdleEventuallyCloses"]),
    "STALE_TIMER_CLOSE": ("idle", ["NoUnauthorizedClose"]),
}
cases += [(name.lower(), model, 100, name) for name, (model, _) in mutations.items()]
results = []
for name, model, bound, mutation in cases:
    case = args.output / name
    case.mkdir(parents=True, exist_ok=True)
    spec = sources[model].replace("max_actions: 100", f"max_actions: {bound}")
    if mutation:
        spec = spec.replace(f"{mutation} = False", f"{mutation} = True")
    # Lost rearm also makes positive witnesses unreachable; suppress witnesses
    # for this mutant only so the checker reaches the named temporal assertion.
    if mutation == "LOST_REARM":
        spec = spec.split("\nexists assertion ", 1)[0] + "\n"
    path = case / (model + ".fizz")
    path.write_text(spec)
    command = [str(args.fizz.resolve()), "--output-dir", str((case / "graph").resolve()), str(path.resolve())]
    log = case / "run.log"
    try:
        with log.open("w") as stream:
            process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                text=True, start_new_session=True)
            try:
                process.communicate(timeout=300)
                code = process.returncode
            except subprocess.TimeoutExpired as exc:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                code = None
                stream.write(str(exc) + "\n")
    except OSError as exc:
        code = None
        with log.open("a") as stream:
            stream.write(str(exc) + "\n")
    output = log.read_text()
    passed = code == 0 and "PASSED: Model checker completed successfully" in output
    expected = mutations[mutation][1] if mutation else None
    detected = code is not None and expected is not None and any(
        f"FAILED: Model checker failed. Invariant:  {invariant}" in output or
        (mutation == "LOST_REARM" and "FAILED: Liveness check failed" in output
         and f"Invariant: {invariant}\n" in output)
        for invariant in expected)
    results.append({"case": name, "command": command, "exit_code": code,
        "bound": bound, "mutation": mutation, "expected_invariant": expected,
        "sha256": hashlib.sha256(spec.encode()).hexdigest(), "passed": passed,
        "detected": detected, "counts": re.findall(r"Nodes: .*|Valid Nodes: .*", output)})
    print(name, "passed=" + str(passed), "detected=" + str(detected), flush=True)
    (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    if not mutation and not passed:
        sys.exit("Baseline failed or timed out; no redundant remaining bounds launched")
(args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
if not all(r["detected"] if r["mutation"] else r["passed"] for r in results):
    sys.exit("Required baseline or named mutation detection missing; inspect run.log")
