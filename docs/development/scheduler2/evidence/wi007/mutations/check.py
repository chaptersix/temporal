"""Run WI-007 one-fault sensitivity checks; restore every source byte afterward."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

out = Path('/private/tmp/scheduler2-wi007-reviewed/mutations')
out.mkdir(parents=True, exist_ok=True)
core = Path('chasm/lib/scheduler2/internal/decision/effects.go')
adapter = Path('chasm/lib/scheduler2/effects.go')
cases = [
    ('expiry-issuance-enabled', Path('chasm/lib/scheduler2/effects.go'), 'if now >= effect.Deadline {', 'if false {', 'TestEffectsTaskDeadlineBoundary', './chasm/lib/scheduler2'),
    ('permit-removed', Path('chasm/lib/scheduler2/effects_tasks.go'), '\tselect {\n\tcase h.permit <- struct{}{}:\n\t\tdefer func() { <-h.permit }()\n\tcase <-ctx.Done():\n\t\treturn ctx.Err()\n\t}\n', '', 'TestEffectsTaskPermitAndDetachedRequest', './chasm/lib/scheduler2'),
    ('cancel-fence-omitted', adapter, 'FirstExecutionRunId: effect.ChainID, RequestId:', 'FirstExecutionRunId: "", RequestId:', 'TestEffectsUnknownTargetBindingAndFrozenRetry', './chasm/lib/scheduler2'),
    ('cancel-not-superseded', core, 'effect.Phase = EffectSuperseded', 'effect.Phase = EffectPending', 'TestEffectsTerminateSupersedesCancelAndLateEvidence', './chasm/lib/scheduler2'),
    ('effect-releases-blocker', core, 'effect.Phase = EffectAccepted', 'effect.Phase = EffectAccepted; next.Root.BlockingOperationID = ""', 'TestEffectsUnknownTargetBindingAndFrozenRetry', './chasm/lib/scheduler2'),
    ('horizon-shortened', core, 'effect.Deadline = now + RetryHorizon', 'effect.Deadline = now + RetryBackoff', 'TestEffectsUnknownTargetBindingAndFrozenRetry', './chasm/lib/scheduler2'),
    ('rollback-retry-regresses', core, 'max(event.Now, effect.RetryAt)', 'event.Now', 'TestEffectsClockRollbackPreservesObligation', './chasm/lib/scheduler2'),
    ('notfound-claims-accepted', core, 'effect.Phase = EffectAbsent', 'effect.Phase = EffectAccepted', 'TestEffectsTaskLostAckFrozenRetryAndNotFound', './chasm/lib/scheduler2'),
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
