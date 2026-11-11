"""Run WI-008 one-fault sensitivity checks; restore every source byte afterward."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

out = Path('/private/tmp/scheduler2-wi008-final/mutations')
out.mkdir(parents=True, exist_ok=True)
cases = [
    ('lose-command-dedup', Path('chasm/lib/scheduler2/manual.go'), 'chasm.WithRequestID(requestID)', 'chasm.WithRequestID("")', 'TestManualPublicDedupAtomicityAndChangedBody', './chasm/lib/scheduler2'),
    ('same-time-start-collision', Path('chasm/lib/scheduler2/internal/decision/decision.go'), 'occurrenceID = fmt.Sprintf("manual/%d", manualID)', 'occurrenceID = fmt.Sprintf("manual/%d", nominal)', 'TestManualSameTimeBypassesPauseBudgetCursor', './chasm/lib/scheduler2'),
    ('manual-debits-budget', Path('chasm/lib/scheduler2/internal/decision/decision.go'), 'if next.Root.Limited && manualID == 0 {', 'if next.Root.Limited {', 'TestManualSameTimeBypassesPauseBudgetCursor', './chasm/lib/scheduler2'),
    ('manual-loses-reuse-policy', Path('chasm/lib/scheduler2/component.go'), 'if op.ManualID != 0 {', 'if false {', 'TestManualSameTimeBypassesPauseBudgetCursor', './chasm/lib/scheduler2'),
    ('disposal-hydrates-template', Path('chasm/lib/scheduler2/manual.go'), '!decision.ManualNeedsTemplate(state)', '!decision.CanDispatchManual(state)', 'TestManualDiscardAvoidsDefinitionPayload', './chasm/lib/scheduler2'),
    ('manual-loses-eligibility', Path('chasm/lib/scheduler2/internal/decision/buffer.go'), 'return q.ManualID != 0 || (', 'return (', 'TestManualMixedQueueEligibility', './chasm/lib/scheduler2/internal/decision'),
    ('manual-loses-capacity-wake', Path('chasm/lib/scheduler2/component.go'), 's.reconcileManual(ctx, next)', 's.ManualArmed = false', 'TestManualFullUnfinishedWaitAndSameTimeEviction', './chasm/lib/scheduler2'),
    ('timestamp-range-wraps', Path('chasm/lib/scheduler2/manual.go'), 'if !time.Unix(0, nominal.UnixNano()).Equal(nominal) {', 'if false {', 'TestManualTimestampDefaultsAndInvalidInputs', './chasm/lib/scheduler2'),
]
manifest = {'source_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), 'results': []}
for name, path, old, new, test, package in cases:
    original = path.read_bytes()
    text = original.decode()
    assert text.count(old) == 1, name
    mutant = text.replace(old, new).encode()
    command = ['go', 'test', '-p', '2', '-parallel', '4', '-tags', 'test_dep', '-count=1', '-timeout=2m', package, '-run', '^' + test + '$', '-v']
    try:
        path.write_bytes(mutant)
        with (out / (name + '.log')).open('w') as stream:
            result = subprocess.run(command, env=dict(os.environ, CGO_ENABLED='0', GOMAXPROCS='4'), stdout=stream, stderr=subprocess.STDOUT, timeout=150)
        log = (out / (name + '.log')).read_text()
        detected = result.returncode == 1 and '--- FAIL: ' + test in log and '[build failed]' not in log
        manifest['results'].append({'mutation': name, 'path': str(path), 'original_sha256': hashlib.sha256(original).hexdigest(), 'mutant_sha256': hashlib.sha256(mutant).hexdigest(), 'old': old, 'new': new, 'command': command, 'exit_code': result.returncode, 'detected': detected})
    finally:
        path.write_bytes(original)
    assert path.read_bytes() == original
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest, indent=2))
assert all(r['detected'] for r in manifest['results'])
