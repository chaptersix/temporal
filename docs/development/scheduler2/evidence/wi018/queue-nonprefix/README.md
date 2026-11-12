# Non-prefix automatic expiry regression

The existing reducer accepts automatic entries (nominal,actual)=(100,109),(102,102),(103,110) across an Update after execution clock rollback. After the blocking operation completes, drain at110 with catchup3 admits the first, expires the middle and retains the last. The test checks the complete queued identities before and after, selected operation identity and no false supersession count.

This disproves a sequence-prefix expiry optimization at the reducer boundary. It does not itself construct a full generator definition producing the trace; independent source review links that possibility to clock rollback, definition replacement and changed jitter. The regression is retained as an exact reference obligation for the indexed implementation. No new restriction on execution clocks is introduced.
