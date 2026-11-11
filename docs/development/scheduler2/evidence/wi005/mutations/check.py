"""Run WI-005 one-fault sensitivity checks; restore every source byte afterward."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

out = Path('/private/tmp/scheduler2-wi005-final/mutations')
out.mkdir(parents=True, exist_ok=True)
core = Path('chasm/lib/scheduler2/internal/decision/buffer.go')
component = Path('chasm/lib/scheduler2/component.go')
cases = [
    ('alias-queue', Path('chasm/lib/scheduler2/internal/decision/decision.go'), 'state.Queue = append([]Queued(nil), state.Queue...)', '// queue copy removed', 'TestBufferQueueOwnership', './chasm/lib/scheduler2/internal/decision'),
    ('debit-on-enqueue', core, 'next.Queue = append(next.Queue,', 'next.Root.Remaining--\n\tnext.Queue = append(next.Queue,', 'TestBufferCapacityAndNoAdmissionDebit', './chasm/lib/scheduler2/internal/decision'),
    ('ignore-catchup', core, 'now <= q.Actual || now-q.Actual <= catchup', 'now <= q.Actual || now-q.Actual >= 0', 'TestBufferExpiredPrefixAndRefillOrdering', './chasm/lib/scheduler2/internal/decision'),
    ('lose-completion-drain', component, 's.reconcileDrain(ctx, next)', 's.DrainArmed = false', 'TestBufferCompletionDrainsExhaustedGenerator', './chasm/lib/scheduler2'),
    ('replace-queued-identity', component, 'request.WorkflowId = op.WorkflowID', 'op.WorkflowID = request.WorkflowId', 'TestBufferCompletionDrainsExhaustedGenerator', './chasm/lib/scheduler2'),
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
