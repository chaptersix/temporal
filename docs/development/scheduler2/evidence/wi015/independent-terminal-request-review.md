# Independent terminal request reclamation review

Reviewed the parent's deletion of `Requests[id]` when the operation becomes Terminal (Completed/StartOnly), excluding review of my own public projection hook. No blocking issue found in current consumers.

Only two production readers consume admitted Requests. `request()` first reconstructs metadata and calls `decision.Issue`; terminal phases are rejected before DATA access. `prepareEffects()` reads the target request only when creating a new pending effect envelope. `Step` reconciles effects after result handling: a terminal blocker clears BlockingOperationID and removes its effects before preparation/publication; an ALLOW_ALL start-only record never owns the blocker. Prepared effects retain their own immutable EffectRequests. Preparation precedes deletion in the same transaction, and callbacks/late acknowledgments merge using operation metadata. Already in-flight starts/effects own detached RPC snapshots. Pending, Started and Ambiguous requests remain retained. Projection reads compact PublicRuntime/operation metadata and does not need terminal requests.

This is a source-consumer safety review, not an aggregate byte-limit or physical persistence proof. Parent owns updated replay/property tests and full-suite evidence. The invariant depends on terminal phases never regaining issuance authority and on a pending effect retaining its independently frozen envelope.

SHA-256 source snapshot:

- `chasm/lib/scheduler2/component.go`: `fc4bd043c4cf4a9fdd8b3e1f04b73bbfea8eb3a2010509d70113b6c6a558f295`
- `chasm/lib/scheduler2/effects.go`: `874d9b95fa689fa79dbd65c401037c05897773b8d064f2aa7b468f5dd9473758`
- `chasm/lib/scheduler2/internal/decision/decision.go`: `bd91223e09ffdf3c5ac1cb8c1a154842bdf030f1da8bd356330296d26055f7ec`
- `chasm/lib/scheduler2/internal/decision/effects.go`: `4e32a154961b808366b5d72d80b2ce997d1bee6262462117dcc059c20043bb8c`
