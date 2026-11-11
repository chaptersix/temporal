#!/usr/bin/env python3
"""Run targeted regressions against production mutations using Go overlays."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[4]
mutations = [
    ("completion.go", "early-callback", "./chasm/lib/scheduler/schedulertest",
     "TestStartAcceptanceCallbackAndRecording/AllowAll/before",
     '\tif !internal.TracksCompletionResult(start.GetOverlapPolicy()) && start.GetRunId() == "" {',
     '\tif start.GetCompleted() != nil {'),
    ("invoker.go", "deferred-rearm", "./chasm/lib/scheduler/schedulertest",
     "TestExpiredSelectionReleasesRemainingBuffer|TestLedger_DeferredStartsStrandedByPausedCompletion",
     '\t// A discarded selection cannot produce', '\t// Re-arm tasks if this call changed state'),
    ("backfiller_tasks.go", "capacity-progress", "./chasm/lib/scheduler",
     "TestBackfillCapacityProgress", None, None),
]
for filename, name, package, test, begin, end in mutations:
    source = root / "chasm/lib/scheduler" / filename
    text = source.read_text()
    if begin is not None:
        first = text.index(begin)
        last = text.index(end, first)
        text = text[:first] + text[last:]
    else:
        original = "return min(available, max(1, available/backfillerCount))"
        assert text.count(original) == 1
        text = text.replace(original, "return available / backfillerCount")
    with tempfile.TemporaryDirectory(prefix="scheduler-mutation-") as temp:
        directory = Path(temp)
        replacement = directory / filename
        replacement.write_text(text)
        overlay = directory / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(source): str(replacement)}}))
        result = subprocess.run(
            ["go", "test", "-tags", "test_dep", "-overlay", str(overlay), package,
             "-run", test, "-count=1"],
            cwd=root, env={**os.environ, "CGO_ENABLED": "0"},
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        if result.returncode == 0 or "--- FAIL: " not in result.stdout or "[build failed]" in result.stdout:
            raise SystemExit(f"{name}: mutation did not produce an assertion failure\n{result.stdout}")
        print(f"{name}: detected")
