"""Run WI-009 one-fault sensitivity checks; restore every source byte afterward."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

out = Path('/private/tmp/scheduler2-wi009-final/mutations')
out.mkdir(parents=True, exist_ok=True)
cases = [
    ('lose-backfill-dedup', Path('chasm/lib/scheduler2/backfill.go'), 'chasm.WithRequestID(requestID)', 'chasm.WithRequestID("")', 'TestBackfillPublicDedupAndPublication', './chasm/lib/scheduler2'),
    ('backfill-debits-budget', Path('chasm/lib/scheduler2/internal/decision/decision.go'), 'if next.Root.Limited && manualID == 0 && candidate.BackfillID == 0 {', 'if next.Root.Limited && manualID == 0 {', 'TestCoordinatorHistoricalBackfillAtomicHandoff', './chasm/lib/scheduler2'),
    ('backfill-identity-collision', Path('chasm/lib/scheduler2/internal/decision/decision.go'), 'fmt.Sprintf("backfill/%d/%d", candidate.BackfillID, nominal)', 'fmt.Sprintf("backfill/%d/%d", uint64(1), nominal)', 'TestCoordinatorBackfillCapAndRequestRotation', './chasm/lib/scheduler2'),
    ('lose-coordinator-token-preflight', Path('chasm/lib/scheduler2/coordinator.go'), 'if !s.HasGenerator || s.Coordination == nil || s.Coordination.Token < math.MaxUint64 {', 'if true {', 'TestCoordinatorTokenRejectionBeforeManualMutation', './chasm/lib/scheduler2'),
    ('release-lease-on-new-ingress', Path('chasm/lib/scheduler2/coordinator.go'), 'c = common.CloneProto(s.Coordination)', 'c = common.CloneProto(s.Coordination); c.OwnerKey = ""; c.NextQueue = scheduler2pb.WORK_CLASS_BACKFILL', 'TestCoordinatorCapacityLeaseSurvivesNewIngress', './chasm/lib/scheduler2'),
    ('ignore-backfill-revision', Path('chasm/lib/scheduler2/backfill.go'), 'b.Continuation == nil || b.Continuation.Revision != g.Continuation.Revision || b.Continuation.Pending == nil', 'b.Continuation == nil || b.Continuation.Pending == nil', 'TestCoordinatorBackfillRevisionInvalidatesLeaseAndRebasesNominal', './chasm/lib/scheduler2'),
    ('request-capacity-unbounded', Path('chasm/lib/scheduler2/backfill.go'), 'if len(s.Backfills) >= 4 {', 'if len(s.Backfills) >= 5 {', 'TestBackfillPublicDedupAndPublication', './chasm/lib/scheduler2'),
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
