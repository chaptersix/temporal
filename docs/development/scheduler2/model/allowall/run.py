#!/usr/bin/env python3
"""Check the independent keyed ALLOW_ALL model and three named sensitivity defects."""
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
source = Path(__file__).with_name("allowall.fizz").read_text()
args.output.mkdir(parents=True, exist_ok=True)
cases = [("baseline-100", 100, None), ("baseline-200", 200, None)]
mutations = {
    "IGNORE_OLDER_RESULT": ["KeyedEvidenceCountedOnce", "TerminalEvidenceSettlesItsIntent"],
    "ALLOW_RESULT_LEAKS": ["NoAllowCarryoverOrPause"],
    "LOSE_CAPACITY_WAKE": ["DrainObligationWithoutSpin"],
}
cases += [(name.lower(), 100, name) for name in mutations]
results = []
for name, bound, mutation in cases:
    case = args.output / name
    case.mkdir(parents=True, exist_ok=True)
    spec = source.replace("max_actions: 100", f"max_actions: {bound}")
    if mutation:
        spec = spec.replace(f"{mutation} = False", f"{mutation} = True")
    path = case / "allowall.fizz"
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
    expected = mutations.get(mutation)
    detected = expected is not None and any(f"FAILED: Model checker failed. Invariant:  {name}" in output for name in expected)
    results.append({"case": name, "command": command, "exit_code": code,
        "bound": bound, "mutation": mutation, "expected_invariant": expected,
        "sha256": hashlib.sha256(spec.encode()).hexdigest(), "passed": passed,
        "detected": detected, "counts": re.findall(r"Nodes: .*|Valid Nodes: .*", output)})
    print(name, "passed=" + str(passed), "detected=" + str(detected), flush=True)
(args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
if not all(r["detected"] if r["mutation"] else r["passed"] for r in results):
    sys.exit("Required baseline or named mutation detection missing; inspect run.log")
