import Std

/-!
Executable WI-002 specification. `Proposed` contains proposition definitions,
not proved theorems. No production enumeration code is imported.
-/
namespace Scheduler2.Enumeration

structure Candidate where
  nominal : Nat
  actualMillis : Nat
  eligible : Bool
  deriving Repr, BEq, DecidableEq

structure Source where
  revision : Nat
  lowerNominal : Nat
  upperMillis : Nat
  hasInclusions : Bool
  raw : List Candidate
  deriving Repr

structure State where
  revision : Nat
  searchAfter : Nat
  disposedThrough : Nat
  pending : Option Candidate := none
  fullRange : Bool
  eligibleSeen : Bool := false
  deriving Repr, BEq, DecidableEq

inductive Stop where
  | yielded | waiting | limited | empty | impossible | exhausted
  deriving Repr, BEq, DecidableEq

inductive Error where
  | changedRevision | staleRevision | actualOutsideRange | malformedOrder
  deriving Repr, BEq, DecidableEq

inductive DispositionKind where
  | emitted | excluded | superseded
  deriving Repr, BEq, DecidableEq

structure Disposition where
  revision : Nat
  candidate : Candidate
  kind : DispositionKind
  deriving Repr, BEq, DecidableEq

structure Result where
  state : State
  output : List Candidate
  dispositions : List Disposition
  probes : Nat
  stop : Stop
  deriving Repr, BEq, DecidableEq

def initial (source : Source) (after : Nat) : State :=
  { revision := source.revision, searchAfter := after, disposedThrough := after,
    fullRange := after < source.lowerNominal }

-- This finite-list oracle is deliberately independent of calendar evaluation.
-- Its linear lookup cost is not the Go source's bounded calendar-call cost.
def nextRaw (source : Source) (after : Nat) : Option Candidate :=
  source.raw.find? (fun candidate => after < candidate.nominal)

def terminal (state : State) : Stop :=
  if state.fullRange && !state.eligibleSeen then .impossible else .exhausted

private def finish (state : State) (output : List Candidate)
    (dispositions : List Disposition) (probes : Nat) (stop : Stop) : Result :=
  { state, output, dispositions, probes, stop }

-- Fuel and output slots are separate. The structural counter starts at
-- twice the fuel plus slots plus two. Exclusion disposal uses an extra step,
-- including at most one excluded candidate buffered by a previous call.
private def loop (source : Source) (through : Nat) :
    Nat → Nat → Nat → State → List Candidate → List Disposition → Nat → Except Error Result
  | 0, _, _, _, _, _, _ => .error .malformedOrder
  | ticks + 1, fuel, slots, state, output, dispositions, probes => do
    if slots == 0 then
      return finish state output dispositions probes .limited
    match state.pending with
    | some candidate =>
      if candidate.actualMillis > source.upperMillis then
        throw .actualOutsideRange
      if candidate.actualMillis > through then
        return finish state output dispositions probes .waiting
      let next := { state with pending := none, disposedThrough := max state.disposedThrough candidate.nominal }
      if candidate.eligible then
        let disposition : Disposition := { revision := state.revision, candidate, kind := DispositionKind.emitted }
        loop source through ticks fuel (slots - 1) next
          (output ++ [candidate]) (dispositions ++ [disposition]) probes
      else
        let disposition : Disposition := { revision := state.revision, candidate, kind := DispositionKind.excluded }
        loop source through ticks fuel slots next output
          (dispositions ++ [disposition]) probes
    | none =>
      if fuel == 0 then
        return finish state output dispositions probes .yielded
      match nextRaw source state.searchAfter with
      | none => return finish state output dispositions (probes + 1) (terminal state)
      | some candidate =>
        if candidate.nominal <= state.searchAfter then
          throw .malformedOrder
        if candidate.actualMillis > source.upperMillis then
          throw .actualOutsideRange
        let next := { state with searchAfter := candidate.nominal, pending := some candidate, eligibleSeen := state.eligibleSeen || candidate.eligible }
        loop source through ticks (fuel - 1) slots next output dispositions (probes + 1)

def scan (source : Source) (through fuel slots : Nat) (state : State) : Except Error Result :=
  if source.revision != state.revision then .error .changedRevision
  else if !source.hasInclusions then
    .ok (finish state [] [] 0 .empty)
  else loop source through (2 * fuel + slots + 2) fuel slots state [] [] 0

structure Updated where
  state : State
  dispositions : List Disposition
  deriving Repr, BEq, DecidableEq

-- Updating disposes the skipped interval through the explicit boundary. A
-- pending future nominal does not extend that boundary under the new revision.
def update (newSource : Source) (boundaryTime : Nat) (old : State) : Except Error Updated :=
  if newSource.revision ≤ old.revision then .error .staleRevision
  else
    let boundary := max boundaryTime old.disposedThrough
    let dispositions : List Disposition := match old.pending with
      | none => []
      | some candidate => [{ revision := old.revision, candidate, kind := .superseded }]
    .ok { state := initial newSource boundary, dispositions }

-- Reviewable assumptions, not axioms. The propositions below take these as
-- explicit hypotheses. They do not assert correctness of Go calendar/jitter.
def WellFormedSource (source : Source) : Prop :=
  source.raw.Pairwise (fun a b =>
    a.nominal < b.nominal ∧ a.actualMillis < b.nominal * 1000) ∧
  (∀ c ∈ source.raw, source.lowerNominal ≤ c.nominal ∧
    c.nominal * 1000 ≤ c.actualMillis ∧ c.actualMillis ≤ source.upperMillis ∧
    (c.eligible = false → c.actualMillis = c.nominal * 1000)) ∧
  (source.hasInclusions = false → source.raw = [])

def WellFormedState (source : Source) (state : State) : Prop :=
  state.revision = source.revision ∧ state.disposedThrough ≤ state.searchAfter ∧
  (∀ c, state.pending = some c →
    c ∈ source.raw ∧ c.nominal = state.searchAfter ∧
    state.disposedThrough < c.nominal ∧ (c.eligible = true → state.eligibleSeen = true))

-- This is the declarative answer, independently of scan's recursive decisions.
def expected (source : Source) (after through : Nat) : List Candidate :=
  source.raw.filter (fun c => after < c.nominal && c.eligible && c.actualMillis <= through)

namespace Proposed

-- SP1: soundness, strict advancement and no skipped earlier eligible candidate.
def NextIsFirst : Prop :=
  ∀ source after through fuel result,
    WellFormedSource source →
    scan source through fuel 1 (initial source after) = .ok result →
    result.output ≠ [] → result.output = (expected source after through).take 1

-- SP1: sufficient resources must successfully return every eligible value.
-- Existence prevents an always-empty or always-error scanner satisfying this claim.
def CompleteEnumeration : Prop :=
  ∀ source after through,
    WellFormedSource source →
    ∃ result,
      scan source through (source.raw.length + 1) (source.raw.length + 1)
        (initial source after) = .ok result ∧
      result.output = expected source after through

-- SP1/SP2: successful output has strictly increasing nominal identities.
def OutputUnique : Prop :=
  ∀ source through fuel slots state result,
    WellFormedSource source → WellFormedState source state →
    scan source through fuel slots state = .ok result →
    result.output.Pairwise (fun a b => a.nominal < b.nominal)

-- SP2: splitting probe work cannot change output or committed progress when
-- the first call yields before its output cap. Final stop is compared as well.
def ResumeEquivalence : Prop :=
  ∀ source through firstFuel secondFuel slots state first resumed whole,
    WellFormedSource source → WellFormedState source state →
    scan source through firstFuel slots state = .ok first →
    first.stop = .yielded →
    scan source through secondFuel (slots - first.output.length) first.state = .ok resumed →
    scan source through (firstFuel + secondFuel) slots state = .ok whole →
    first.output ++ resumed.output = whole.output ∧
    first.dispositions ++ resumed.dispositions = whole.dispositions ∧
    first.probes + resumed.probes = whole.probes ∧
    resumed.state = whole.state ∧ resumed.stop = whole.stop

-- SP3: adjacent inclusive windows preserve the output sequence. Sufficient
-- per-call fuel/slots exclude work/output truncation; waiting preserves pending.
def BatchEquivalence : Prop :=
  ∀ source after middle through first second whole,
    WellFormedSource source → middle ≤ through →
    scan source middle (source.raw.length + 1) (source.raw.length + 1)
      (initial source after) = .ok first →
    scan source through (source.raw.length + 1) (source.raw.length + 1)
      first.state = .ok second →
    scan source through (source.raw.length + 1) (source.raw.length + 1)
      (initial source after) = .ok whole →
    first.output ++ second.output = whole.output ∧ second.state = whole.state

-- SP4: waiting never recomputes or drops a pending jittered candidate.
def PendingStable : Prop :=
  ∀ source through fuel slots state candidate result,
    WellFormedSource source → WellFormedState source state →
    state.pending = some candidate → slots > 0 → candidate.actualMillis > through →
    scan source through fuel slots state = .ok result →
    result.state = state ∧ result.output = [] ∧ result.dispositions = [] ∧
    result.probes = 0 ∧ result.stop = .waiting

-- SP5: update boundaries are independent of the discarded pending nominal.
def UpdateBoundary : Prop :=
  ∀ source boundaryTime old result,
    update source boundaryTime old = .ok result →
    source.revision > old.revision ∧
    result.state.searchAfter = max boundaryTime old.disposedThrough ∧
    result.state.disposedThrough = max boundaryTime old.disposedThrough ∧
    result.state.pending = none ∧ result.state.revision = source.revision ∧
    (∀ c, old.pending = some c → result.dispositions =
      [{ revision := old.revision, candidate := c, kind := DispositionKind.superseded }])

def UpdateRejectsStale : Prop :=
  ∀ source boundaryTime old,
    source.revision ≤ old.revision →
    update source boundaryTime old = .error .staleRevision

-- Candidate probes, not CPU steps or raw calendar calls, are counted here.
def ProbeBound : Prop :=
  ∀ source through fuel slots state result,
    scan source through fuel slots state = .ok result → result.probes ≤ fuel

end Proposed
end Scheduler2.Enumeration

open Scheduler2.Enumeration
private def auditSource : Source :=
  { revision := 1, lowerNominal := 1, upperMillis := 10000, hasInclusions := true,
    raw := [{nominal := 1, actualMillis := 1500, eligible := true}] }
-- Zero fuel is executable here, unlike Go public Run.
#guard match scan auditSource 10000 0 1 (initial auditSource 0) with
  | .ok r => r.stop == .yielded
  | _ => false
-- Equal revision is insufficient to authenticate an immutable source in Go.
#guard match scan {auditSource with raw := [{nominal := 2, actualMillis := 2000, eligible := true}]}
    10000 3 3 (initial auditSource 0) with
  | .ok r => r.output == [{nominal := 2, actualMillis := 2000, eligible := true}]
  | _ => false
-- Unrestricted restart equivalence cannot also preserve terminal probe counts.
#guard match scan auditSource 10000 3 3 (initial auditSource 0) with
  | .ok first => match scan auditSource 10000 3 3 first.state with
    | .ok second => first.probes == 2 && second.probes == 1 && first.stop == .exhausted && second.stop == .exhausted
    | _ => false
  | _ => false
-- Initial zero search excludes nominal zero, so an epoch shift needs a sentinel.
#guard match scan {auditSource with lowerNominal := 0, raw := [{nominal := 0, actualMillis := 0, eligible := true}]}
    10000 3 3 (initial auditSource 0) with
  | .ok r => r.output.isEmpty
  | _ => false
