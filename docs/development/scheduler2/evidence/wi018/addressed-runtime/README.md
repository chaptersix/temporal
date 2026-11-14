# Fresh complete addressed operation runtime: physical task tests

`addressed_runtime_test.go` uses the parent's private newAddressedPublicSchedule
constructor. All stores are selected before definition/generator/initial patch.
The fixture reuses the existing locked mock-engine adapter but discards its entire
setup tree; the selected execution is a new empty tree, never a conversion.

Manual initial patch and automatic occurrence admission both run through real
coordinator pure-task delivery. Start issuance uses the actual persisted physical
ChasmTask envelope, filtered by the registered StartDispatchTask type, and
Node.ExecuteSideEffectTask. Locked updates execute real CHASM CloseTransaction and
publish a cloned durable tree image. Parent context deadlines establish claims;
there is no custom context clock wrapper. RPC observation uses the fixture's
execution clock. Recovery advances virtual time without sleeping.

Cases cover:

- successful start, duplicate physical delivery, full request identity and input;
- callback before physical acknowledgment, late enrichment and no running output;
- negative RPC result, fixed pending authority, exact frozen retry request;
- committed claim followed by context cancellation before result merge, reload,
  rejected recovery publication, accepted physical pure recovery, new fairness
  sequence with unchanged logical retry/deadline, stale start replay and success;
- Close during RPC, disabled dispatch, retained historical claim/request, ignored
  returned acknowledgment, and stale physical replay;
- actual automatic coordinator admission and physical start.

Assertions compare operation phase, pending/request/running counters, addressed
request presence, exact shared blob owner count, absence of legacy maps, public
recent/running output and immutable requests. Tracked Started operations retain
requests until terminal completion. Closing retains owner records and the claim
as historical index membership; Closed plus disarmed control fences authority.

Evidence is a mocked engine with real CHASM serialization/claim/task boundaries,
not an external SQL crash test or production routing. It does not select the
bounded framework registry yet and does not establish the final transaction cost.

Retained fixture corrections: log01 chose the last task (visibility sentinel)
instead of filtering StartDispatch; log02/03 wrongly expected tracked Started
request removal and cleared Close claim; log05 used an incorrect '/auto/' identity
substring instead of '/automatic/'. Corrected tests pass; no runtime defect was
identified in these cases. Final logs and frozen test hash are alongside this file.
