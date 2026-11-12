# Superseded calendar source ownership

This increment resolves the missing cleanup discovery ownership recorded at definition-preparation/review.md. It does not select or attach CalendarPreparation to scheduler runtime.

ReplaceSource validates the existing fixed header and FIFO, preflights ordinal exhaustion and occupied tail, then atomically stores the prior source ID, seven range counts and exact installed frontier under the FIFO tail key. Head/tail/count survive replacement. Ordinals never wrap or reset. A zero-page source still has one discoverable owner.

CleanupSuperseded visits only the head and at most sixteen installed pages. All addressed page identities and lengths are validated before any deletion; missing/malformed pages or current-source ownership fail closed. It deletes only the installed prefix, retains the cursor between transactions, removes a completed descriptor, and advances head at most once per call. New arrivals cannot reset or overtake the old cursor. There is no map enumeration or whole-generation destruction. Existing execution deletion remains responsible for final closed-execution storage reclamation; no custom lifecycle activation was added.

CHASM serialization/reload tests cover failed replacement publication, failed cleanup publication and retry, partial install ownership, fresh reload after every quantum, rapid replacements during old-head drain, zero-page owners, current-source retention, duplicate empty cleanup, metadata exhaustion and missing/wrong owner/page corruption. A corrupt later page must not delete an earlier valid page. First-post-reload inspection observes at most one owner plus the requested page count decoded and zero collection-key hydration. This is in-process NodesMutation persistence, not a SQLite queue witness.

Commands:

- make proto: PASS, cleanup-proto01.
- CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2 -run '^TestCalendarPreparation' -count=1: PASS 0.719s before strengthened later-page assertion.
- CGO_ENABLED=0 go test -tags test_dep ./chasm/lib/scheduler2/... -count=1: final PASS root 12.473s, decision 0.315s, occurrence 2.106s, queueindex 8.369s, including strengthened assertion.
- make lint-code LINT_CODE_TARGETS='./chasm/lib/scheduler2' GOLANGCI_LINT_FIX=false GOLANGCI_LINT_BASE_REV=HEAD: PASS zero issues.

Remaining scope: actual runtime scheduling of cleanup, whole-transaction framework worklists, timezone/calendar evaluation and eventual resource capacity remain separate work. FIFO progress is conditional on servicing cleanup quanta; arrival rate can exceed service rate. Source staging remains input-proportional outside mutation. No WI018 or milestone acceptance is claimed.
