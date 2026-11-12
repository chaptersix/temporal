# D59 prototype byte ceilings: local implementation evidence

The assigned change removes the prototype 64KiB serialized completion/result/failure and whole start-template/seed checks, and the 128KiB serialized effect check. Callback protobuf decoding, terminal-status/outcome validation, identities and configuration policy/revision checks remain. Existing public ingress and actual workflow-start resource validation remain authoritative; this patch does not replace them or invent a second fixed limit for already accepted outcomes. The source-backed authority and configured limits are recorded in `design/public-execution-contract.md`.

Tests exercise former completion boundaries 65535/65536/65537 bytes, 256KiB and 2MiB−16KiB wire values. Failed publication followed by retry retains the exact value after serialized CHASM reload. Large malformed wire still rejects. A failure near 2MiB retains a previous result near 2MiB and both become the next admission's frozen request. Reducer tests also check detached input bytes and unchanged prior state. Template/seed boundary neighbors now persist, and an actual input Payload near 2MiB survives caller mutation and serialized reload. Cancel/terminate effects at 131071/131072/131073/262144 bytes survive persistence and detached RPC decoding.

The effect-size fixture deliberately uses a synthetic long chain ID to control exact serialized size. It establishes local serialization and removal of the prototype guard, not that public ingress accepts such an ID. Template boundary-neighbor fixtures similarly use synthetic Identity; the separate near-2MiB test uses Input.

## Validation

`CGO_ENABLED=0 GOMAXPROCS=4 go test -tags test_dep ./chasm/lib/scheduler2 ./chasm/lib/scheduler2/internal/decision -count=1` passed: scheduler2 16.270s, decision 0.884s. Log: `d59-byte-tests.log`.

`git diff --check` passed for all eight owned files. Fast lint completed with two SA4006 unused context assignments, one in the new completion test and one in the independently owned public mutation test. The completion assignment was corrected to `_`; the parent owns the consolidated final rerun after the other owner fixes their finding. `d59-byte-lint-initial.log` preserves this result. The first attempt encountered a concurrent golangci-lint lock, which was not a code failure. The final completion-test edit changes only the unused assignment, after the passing full test run.

## Limits and remaining work

The harness uses actual CHASM node serialization/reload and modeled atomic publication. It does not exercise database persistence limits or the active engine's mutable-state enforcement. It does not prove safety under the default aggregate 8MiB mutable-state error limit, nor bounded aggregate bytes. Multiple large accepted payloads can still be duplicated in public definition, generation template, admitted requests and carryover; D59's DATA reference/deduplication and aggregate capacity work remains open. These tests must not be reported as general public creation readiness.

The occurrence-spec prototype size guard is outside this change's ownership. Catchup/idle/time upper bounds are intentionally unchanged pending their separately owned reconciliation. This implementation is author evidence, not independent approval.

## Source fingerprints

SHA-256 at this review snapshot (other agents may subsequently change shared files):

- `chasm/lib/scheduler2/completion.go`: `e647415fe29b279853603e2215c7a6b674c5de937f4e5e332ce8256ae2e53abf`
- `chasm/lib/scheduler2/internal/decision/completion.go`: `3e1d21615f7d5c3422182c1ea45908b59fad47491c2f203a2e027cc3e9f897fc`
- `chasm/lib/scheduler2/generator.go`: `31a2053610af937c87898a7f713d06ab7dee75fac2f84527ecaf8e8ad4e57c71`
- `chasm/lib/scheduler2/effects.go`: `874d9b95fa689fa79dbd65c401037c05897773b8d064f2aa7b468f5dd9473758`
- `chasm/lib/scheduler2/completion_test.go`: `a9a5d79278fb4df9f8a04e23c1ce4a7593091c1614d4d431030e14eea186898e`
- `chasm/lib/scheduler2/internal/decision/completion_test.go`: `033a1481b06ef8c44376cfec50cddee2b9d352384b3e3443d6a2e9228b1dbbc4`
- `chasm/lib/scheduler2/effects_limits_test.go`: `d21540c137e43801c7246195351c8af2ddd46851721e32314e1a727d51e5456c`
- `chasm/lib/scheduler2/configuration_limits_test.go`: `df2c15c7db483db4e899676e4351a7f1dbdde363f9693b8503ee27e66fc4f411`
