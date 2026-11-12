# Independent public command capacity review

Parent reviewed the whole patch preflight, both direct Accept entry points,
manual reducer profile selection, old scheduler handlePatch/maxBackfillers, and
combined capacity/rollback/reload tests. PublicRevision is persisted and initialized
before initial patch processing. Public paths permit100 combined pending manual
and range commands;101 rejects with the same FailedPrecondition as the reference.
Preflight precedes pause/config changes and direct entry checks prevent bypass.
Subtracting retained counts avoids addition overflow in the comparison. Internal
nonpublic commands retain the original4 limit and error type; queue and operation
capacities are unaffected.

No behavioral blocker found for this ingress increment. Public work scales with
up to100 command metadata records; existing finite proofs with4 commands are not
proofs for the expanded public state. These tests establish acceptance/rejection,
rollback and persistence, not100-way draining throughput or full capacity parity.
Those remain under runtime coverage/M4. Unchanged public validation ports passed
in author's run; this review does not claim an independent rerun.
