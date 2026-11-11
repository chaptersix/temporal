# Evidence ledger

Base: 85b80a1836dbc73741cb52586297dc35e6c937ca; comparison:
9ab3a9f770da20df7d94bcc0030f28eec7b0b947. No implementation/model results yet.

- E4 provenance: `git worktree list --porcelain`, `git reflog show sch-redesign --date=iso`,
  original `git status --porcelain=v1` and `git diff --binary`; files beside this ledger.
  Original diff is evidence only, not an applied contribution. W1-W4.
- E5 source inspection: ../investigation.md and ../contract.md; three independent
  read-only investigations at base SHA. H1-H7, C1-C8 relevant slice; no model or
  runtime claim. Fix inventory commands: `git log --oneline 9ab3a9f77..HEAD --
  chasm/lib/scheduler`, `git log --all --oneline --not HEAD -- chasm/lib/scheduler`.
- E6 tooling: `go version` => go1.27.3 darwin/arm64; `command -v lean/lake` absent;
  `shasum -a 256 /private/tmp/fizzbee-v0.5.4.tar.gz` => pinned hash in contract.
  Tool availability only, not model execution. FizzBee path:
  /private/tmp/fizzbee-v0.5.4/fizzbee-v0.5.4-macos_arm/fizz.
- E7 baseline: `go test -tags test_dep ./chasm/lib/scheduler/internal -count=1`
  passed (0.231s), after sandbox initially denied Go cache access and approved retry.
  No changed code. This validates baseline internal package only, not M1.

Unmodeled: database crash recovery, failover, full calendars, all API behavior.
All future executable results require source/diff identity and standalone traces;
none of the above inspection records satisfies an implementation gate.

- E8 independent WI-000 review: three contract gaps found and resolved in contract.md
  Review resolutions (deadline/late results, identities/chain binding, fairness).
  M0 accepted for bounded slice; no M1 implementation evidence.
