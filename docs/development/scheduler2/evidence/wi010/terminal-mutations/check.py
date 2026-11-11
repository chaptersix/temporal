"""Run WI-010 terminal-fence one-fault sensitivity checks; restore every source byte afterward."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

out = Path('/private/tmp/scheduler2-wi010/terminal-mutations')
out.mkdir(parents=True, exist_ok=True)
cases = [
    ('close-never-persists-terminal', Path('chasm/lib/scheduler2/lifecycle.go'), 's.Closed = true', 's.Closed = false', 'TestLifecycleClosePublicationAndTaskFences', './chasm/lib/scheduler2'),
    ('closed-start-reads-payload', Path('chasm/lib/scheduler2/component.go'), 'func (s *Scheduler) request(ctx chasm.Context, operationID string) (*workflowservice.StartWorkflowExecutionRequest, error) {\n\tif s.Closed {', 'func (s *Scheduler) request(ctx chasm.Context, operationID string) (*workflowservice.StartWorkflowExecutionRequest, error) {\n\tif false {', 'TestLifecycleClosedResultsAndPoisonedPayloadsAreNoOps', './chasm/lib/scheduler2'),
    ('closed-effect-reads-payload', Path('chasm/lib/scheduler2/effects.go'), 'func (s *Scheduler) effectRequest(ctx chasm.Context, id string) (*EffectRequest, error) {\n\tif s.Closed {', 'func (s *Scheduler) effectRequest(ctx chasm.Context, id string) (*EffectRequest, error) {\n\tif false {', 'TestLifecycleClosedResultsAndPoisonedPayloadsAreNoOps', './chasm/lib/scheduler2'),
    ('closed-control-validates-body', Path('chasm/lib/scheduler2/generator.go'), 'if s.Closed {', 'if false {', 'TestLifecycleClosedControlsRejectAndTerminateIsIdempotent', './chasm/lib/scheduler2'),
    ('delete-does-not-close', Path('chasm/lib/scheduler2/lifecycle.go'), 'return chasm.TerminateComponentResponse{}, s.Close(ctx)', 'return chasm.TerminateComponentResponse{}, nil', 'TestLifecycleClosedControlsRejectAndTerminateIsIdempotent', './chasm/lib/scheduler2'),
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
