#!/usr/bin/env python3
"""Pinned, fail-fast local proof check and separately compiled JSON oracle."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--toolchain", type=Path, default=Path("/private/tmp/scheduler2-lean-toolchain/lean-4.24.0-darwin_aarch64/bin"))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    out = args.out.resolve()
    if out.exists() and any(out.iterdir()):
        raise RuntimeError("output directory must be new or empty; preserve earlier evidence")
    out.mkdir(parents=True, exist_ok=True)
    tracked = sorted(root.glob("*.lean")) + [root / "wire-v1.md", root / "build.py"]
    sources = {p.name: digest(p) for p in tracked}
    manifest = {"sources": sources, "tool_sha256": digest(args.toolchain / "lean"), "commands": [], "status": "running"}
    env = dict(os.environ, LEAN_PATH=str(out))

    def command(argv, label):
        started = time.monotonic()
        process = subprocess.Popen(argv, cwd=root, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        try:
            data, _ = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            data, _ = process.communicate()
        (out / (label + ".log")).write_bytes(data)
        record = {"argv": [str(x) for x in argv], "cwd": str(root), "LEAN_PATH": str(out),
                  "exit_code": process.returncode, "timed_out": timed_out,
                  "seconds": time.monotonic() - started, "log": label + ".log"}
        manifest["commands"].append(record)
        if process.returncode != 0 or timed_out:
            raise RuntimeError(label + " failed")

    try:
        command([args.toolchain / "lean", "--version"], "version")
        if "version 4.24.0," not in (out / "version.log").read_text():
            raise RuntimeError("unexpected Lean version; require 4.24.0")
        for module in ["Kernel", "Frontier", "Oracle", "Controls", "Validity", "Public", "KernelComposition", "PublicComposition"]:
            command([args.toolchain / "lean", "-o", out / (module + ".olean"),
                     "-c", out / (module + ".c"), module + ".lean"], module)
        command([args.toolchain / "lean", "Audit.lean"], "audit")
        audit = (out / "audit.log").read_text()
        expected = re.findall(r"^#print axioms (\S+)", (root / "Audit.lean").read_text(), re.MULTILINE)
        audited = re.findall(r"^'([^']+)' (?:depends on axioms: \[[^\]]*\]|does not depend on any axioms)$", audit, re.MULTILINE)
        if len(audited) != len(expected) or not all(any(a == e or a.endswith("." + e) for a in audited) for e in expected):
            raise RuntimeError("required theorem axiom audit is incomplete")
        allowed = {"propext", "Quot.sound", "Classical.choice"}
        for group in re.findall(r"depends on axioms: \[([^\]]*)\]", audit):
            if set(filter(None, (x.strip() for x in group.split(",")))) - allowed:
                raise RuntimeError("unapproved theorem axiom dependency")
        for label in ["Kernel", "Frontier", "Oracle", "Controls", "Validity", "Public", "KernelComposition", "PublicComposition", "audit"]:
            if re.search(r"declaration uses.*sorry|sorryAx|Lean\.ofReduceBool", (out / (label + ".log")).read_text()):
                raise RuntimeError("unfinished or trusted-evaluation proof in " + label)
        command([args.toolchain / "leanc", out / "Oracle.c", out / "Kernel.c", out / "Frontier.c",
                 "-o", out / "oracle"], "link")
        manifest["binary_sha256"] = digest(out / "oracle")
        manifest["binary"] = str(out / "oracle")
        manifest["status"] = "passed"
    except Exception as error:
        manifest["status"] = "failed"
        manifest["error"] = str(error)
        raise
    finally:
        manifest["sources_after"] = {p.name: digest(p) for p in tracked}
        if manifest["sources_after"] != sources:
            manifest["status"] = "failed"
            manifest["error"] = "sources changed during build"
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if manifest["status"] != "passed":
        raise RuntimeError(manifest.get("error", "build failed"))
    print(str(out / "oracle"))


if __name__ == "__main__":
    main()
