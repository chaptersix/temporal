import Oracle

open Scheduler2.Exact
namespace Scheduler2.Wire

theorem begin_identity (src : Source) (after : Nat) (s : State)
    (success : begin src after = .ok s) :
    s.identity = src.identity ∧ s.revision = src.revision ∧ s.origin = after ∧
    s.searchAfter = after ∧ s.disposedThrough = after ∧ s.pending = none ∧
    s.fullRange = decide (after < src.lower) ∧ s.eligibleSeen = false ∧ s.terminal = .active := by
  simp only [begin] at success
  split at success
  · simp at success
  · simp only [Except.ok.injEq] at success
    subst s
    simp [initial, beginMachine, restore]

theorem replacement_boundary (src : Source) (old : State) (atTime : Option Nat)
    (next : State) (superseded : Option Candidate)
    (success : replacement src old atTime = .ok (next, superseded)) :
    old.revision < src.revision ∧ superseded = old.pending ∧
    next.searchAfter = (match atTime with | some time => max time old.disposedThrough | none => old.disposedThrough) ∧
    next.disposedThrough = next.searchAfter ∧ next.pending = none ∧
    next.identity = src.identity ∧ next.revision = src.revision := by
  unfold replacement at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · rename_i newer
      have atBoundary : ∀ boundary, replacementAt src old boundary = .ok (next, superseded) →
          next.searchAfter = boundary ∧ next.disposedThrough = next.searchAfter ∧
          next.pending = none ∧ next.identity = src.identity ∧ next.revision = src.revision ∧ superseded = old.pending := by
        intro boundary hs
        unfold replacementAt at hs
        split at hs
        · simp at hs
        · rename_i result hb
          simp only [Except.ok.injEq, Prod.mk.injEq] at hs
          rcases hs with ⟨rfl, rfl⟩
          have h := begin_identity src boundary result hb
          exact ⟨h.2.2.2.1, by omega, h.2.2.2.2.2.1, h.1, h.2.1, rfl⟩
      cases atTime with
      | none =>
        have h := atBoundary old.disposedThrough success
        exact ⟨by omega, h.2.2.2.2.2, h.1, h.2.1, h.2.2.1, h.2.2.2.1, h.2.2.2.2.1⟩
      | some time =>
        simp only at success
        split at success
        · simp at success
        · have h := atBoundary (max time old.disposedThrough) success
          exact ⟨by omega, h.2.2.2.2.2, h.1, h.2.1, h.2.2.1, h.2.2.2.1, h.2.2.2.2.1⟩

-- Successful public Run uses precisely the proved kernel. Parsing/validation and
-- the probe-time range error cannot substitute another occurrence algorithm.
theorem scan_success_kernel (src : Source) (s : State) (through probes outputs : Nat)
    (result : Result) (active : s.terminal = .active) (inclusions : src.hasInclusions = true)
    (success : scan src s through probes outputs false = .ok result) :
    result = run through probes outputs
      (restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s) := by
  unfold scan at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · split at success
      · simp at success
      · simp only [active, inclusions, show (Stop.active != Stop.active) = false from rfl, Bool.not_true, Bool.false_eq_true, ↓reduceIte] at success
        split at success
        · simp at success
        · split at success
          · simp at success
          · simpa only [Except.ok.injEq] using success.symm

theorem replacement_stale (src : Source) (old : State) (atTime : Option Nat)
    (valid : validState old = true) (stale : src.revision ≤ old.revision) :
    replacement src old atTime = .error "stale-revision" := by
  simp [replacement, valid, stale]

theorem dispose_success (src : Source) (old next : State)
    (success : dispose src old = .ok next) :
    ∃ c, old.pending = some c ∧ c.excluded = false ∧
      next = { old with pending := none, disposedThrough := c.nominal } := by
  unfold dispose at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · split at success
      · simp at success
      · rename_i c hp
        split at success
        · simp at success
        · rename_i he
          exact ⟨c, hp, by simpa using he, by simpa only [Except.ok.injEq] using success.symm⟩

theorem peek_no_output (initial : Machine) (r : Result) : (asPeek initial r).output = [] := by
  unfold asPeek
  split
  · assumption
  · rfl

theorem peek_retains (initial : Machine) (r : Result) (c : Candidate) (tail : List Candidate)
    (head : r.output = c :: tail) :
    (asPeek initial r).machine.state.pending = some c ∧
    (asPeek initial r).stop = .active ∧ (asPeek initial r).probes = r.probes := by
  simp [asPeek, head]

theorem scan_reconstructible (src : Source) (s : State) (through probes outputs : Nat)
    (result : Result) (active : s.terminal = .active) (inclusions : src.hasInclusions = true)
    (frontier : Frontier (restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s))
    (success : scan src s through probes outputs false = .ok result) :
    restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd))
      result.machine.state = result.machine := by
  rw [scan_success_kernel src s through probes outputs result active inclusions success]
  exact run_reconstructible _ _ _ _ _ frontier rfl

theorem scan_conservation (src : Source) (s : State) (through probes outputs : Nat)
    (result : Result) (active : s.terminal = .active) (inclusions : src.hasInclusions = true)
    (success : scan src s through probes outputs false = .ok result) :
    result.output ++ eligible (remaining result.machine) =
      eligible (remaining (restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s)) := by
  rw [scan_success_kernel src s through probes outputs result active inclusions success]
  exact run_conservation _ _ _ _

end Scheduler2.Wire
