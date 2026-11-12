import Kernel

namespace Scheduler2.Exact

def Frontier (m : Machine) : Prop :=
  m.rest.Pairwise (fun a b => a.nominal < b.nominal) ∧
  (∀ c ∈ m.rest, m.state.searchAfter < c.nominal) ∧
  (m.state.pending = none → m.state.disposedThrough = m.state.searchAfter) ∧
  (∀ c, m.state.pending = some c →
    m.state.disposedThrough < c.nominal ∧ c.nominal = m.state.searchAfter)

def restore (raw : List Candidate) (s : State) : Machine :=
  { state := s, rest := raw.filter (fun c => s.searchAfter < c.nominal) }

def beginMachine (identity : String) (revision lower after : Nat) (raw : List Candidate) : Machine :=
  restore raw { identity, revision, origin := after, searchAfter := after, disposedThrough := after, fullRange := after < lower }

theorem begin_frontier (identity : String) (revision lower after : Nat) (raw : List Candidate)
    (ordered : raw.Pairwise (fun a b => a.nominal < b.nominal)) :
    Frontier (beginMachine identity revision lower after raw) := by
  refine ⟨List.Pairwise.filter _ ordered, ?_, ?_, ?_⟩
  · intro c hc
    simpa [beginMachine, restore, List.mem_filter] using (List.mem_filter.mp hc).2
  · simp [beginMachine, restore]
  · simp [beginMachine, restore]

theorem dispose_frontier (m : Machine) (c : Candidate) (h : Frontier m)
    (pending : m.state.pending = some c) :
    Frontier { m with state := { m.state with pending := none, disposedThrough := c.nominal } } := by
  rcases h with ⟨ordered, future, _, buffered⟩
  have hc := buffered c pending
  exact ⟨ordered, future, fun _ => hc.2, by simp⟩

theorem probe_frontier (m : Machine) (c : Candidate) (cs : List Candidate) (h : Frontier m)
    (pending : m.state.pending = none) (rest : m.rest = c :: cs) :
    Frontier { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs } := by
  rcases h with ⟨ordered, future, unbuffered, _⟩
  rw [rest] at ordered
  have parts := List.pairwise_cons.mp ordered
  have hc := future c (by simp [rest])
  have hd := unbuffered pending
  refine ⟨parts.2, parts.1, by simp, ?_⟩
  intro b hb
  have eq : c = b := Option.some.inj hb
  subst b
  exact ⟨by dsimp; omega, rfl⟩

theorem loop_frontier (through ticks fuel slots : Nat) (m : Machine) (h : Frontier m) :
    Frontier (loop through ticks fuel slots m).machine := by
  induction ticks generalizing fuel slots m with
  | zero => exact h
  | succ ticks ih =>
    simp only [loop]
    split
    · exact h
    · split
      · exact h
      · split
        · rename_i c hp
          split
          · exact h
          · split
            · exact ih _ _ _ (dispose_frontier m c h hp)
            · exact ih _ _ _ (dispose_frontier m c h hp)
        · rename_i hp
          split
          · exact h
          · split
            · exact h
            · rename_i c cs hr
              exact ih _ _ _ (probe_frontier m c cs h hp hr)

theorem frontier_remaining_ordered (m : Machine) (h : Frontier m) :
    (remaining m).Pairwise (fun a b => a.nominal < b.nominal) := by
  rcases h with ⟨ordered, future, _, buffered⟩
  cases hp : m.state.pending with
  | none => simpa [remaining, hp] using ordered
  | some c =>
    have hc := buffered c hp
    simp only [remaining, hp, Option.toList_some, List.singleton_append, List.pairwise_cons]
    exact ⟨by simpa [hc.2] using future, ordered⟩

-- The executable cursor is sufficient to reconstruct the unprobed source suffix.
theorem filter_advance (raw : List Candidate) (after : Nat) (c : Candidate) (cs : List Candidate)
    (suffixEq : raw.filter (fun x => after < x.nominal) = c :: cs)
    (future : after < c.nominal)
    (ordered : (c :: cs).Pairwise (fun a b => a.nominal < b.nominal)) :
    raw.filter (fun x => c.nominal < x.nominal) = cs := by
  have nested : (raw.filter (fun x => after < x.nominal)).filter (fun x => c.nominal < x.nominal) =
      raw.filter (fun x => c.nominal < x.nominal) := by
    rw [List.filter_filter]
    congr 1
    funext x
    by_cases hx : c.nominal < x.nominal
    · simp [hx, show after < x.nominal by omega]
    · simp [hx]
  rw [suffixEq] at nested
  have tailFilter : cs.filter (fun x => c.nominal < x.nominal) = cs := by
    apply List.filter_eq_self.mpr
    intro x hx
    simpa using (List.pairwise_cons.mp ordered).1 x hx
  simpa [tailFilter] using nested.symm

theorem loop_source_suffix (raw : List Candidate) (through ticks fuel slots : Nat) (m : Machine)
    (frontier : Frontier m)
    (sourceEq : raw.filter (fun x => m.state.searchAfter < x.nominal) = m.rest) :
    raw.filter (fun x => (loop through ticks fuel slots m).machine.state.searchAfter < x.nominal) =
      (loop through ticks fuel slots m).machine.rest := by
  induction ticks generalizing fuel slots m with
  | zero => exact sourceEq
  | succ ticks ih =>
    simp only [loop]
    split
    · exact sourceEq
    · split
      · exact sourceEq
      · split
        · rename_i c hp
          split
          · exact sourceEq
          · split
            · exact ih _ _ _ (dispose_frontier m c frontier hp) sourceEq
            · exact ih _ _ _ (dispose_frontier m c frontier hp) sourceEq
        · rename_i hp
          split
          · exact sourceEq
          · split
            · exact sourceEq
            · rename_i c cs hr
              apply ih _ _ _ (probe_frontier m c cs frontier hp hr)
              apply filter_advance raw m.state.searchAfter c cs
              · exact sourceEq.trans hr
              · exact frontier.2.1 c (by simp [hr])
              · simpa [hr] using frontier.1

theorem run_reconstructible (raw : List Candidate) (through fuel slots : Nat) (m : Machine)
    (frontier : Frontier m)
    (sourceEq : raw.filter (fun x => m.state.searchAfter < x.nominal) = m.rest) :
    restore raw (run through fuel slots m).machine.state = (run through fuel slots m).machine := by
  have suffixEq := loop_source_suffix raw through (rank m + 1) fuel slots m frontier sourceEq
  cases hr : (run through fuel slots m).machine with
  | mk state rest =>
    change raw.filter (fun x => (run through fuel slots m).machine.state.searchAfter < x.nominal) =
      (run through fuel slots m).machine.rest at suffixEq
    rw [hr] at suffixEq
    simp [restore, suffixEq]

def Bounds (m : Machine) : Prop :=
  m.state.origin ≤ m.state.disposedThrough ∧ m.state.disposedThrough ≤ m.state.searchAfter

theorem begin_bounds (identity : String) (revision lower after : Nat) (raw : List Candidate) :
    Bounds (beginMachine identity revision lower after raw) := by
  simp [Bounds, beginMachine, restore]

theorem loop_bounds (through ticks fuel slots : Nat) (m : Machine)
    (frontier : Frontier m) (bounds : Bounds m) : Bounds (loop through ticks fuel slots m).machine := by
  induction ticks generalizing fuel slots m with
  | zero => exact bounds
  | succ ticks ih =>
    simp only [loop]
    split
    · exact bounds
    · split
      · exact bounds
      · split
        · rename_i c hp
          split
          · exact bounds
          · have hc := frontier.2.2.2 c hp
            have nextBounds : Bounds { m with state := { m.state with pending := none, disposedThrough := c.nominal } } := by
              simp only [Bounds] at bounds ⊢
              omega
            split
            · exact ih _ _ _ (dispose_frontier m c frontier hp) nextBounds
            · exact ih _ _ _ (dispose_frontier m c frontier hp) nextBounds
        · rename_i hp
          split
          · exact bounds
          · split
            · exact bounds
            · rename_i c cs hr
              apply ih _ _ _ (probe_frontier m c cs frontier hp hr)
              have hc := frontier.2.1 c (by simp [hr])
              simp only [Bounds] at bounds ⊢
              omega

def authority (s : State) := (s.identity, s.revision, s.origin, s.fullRange)

theorem loop_authority (through ticks fuel slots : Nat) (m : Machine) :
    authority (loop through ticks fuel slots m).machine.state = authority m.state := by
  induction ticks generalizing fuel slots m with
  | zero => rfl
  | succ ticks ih =>
    simp only [loop]
    split
    · rfl
    · split
      · rfl
      · split
        · split
          · rfl
          · split
            · exact ih _ _ _
            · exact ih _ _ _
        · split
          · rfl
          · split
            · rfl
            · exact ih _ _ _

theorem pending_stable (through fuel slots : Nat) (m : Machine) (c : Candidate)
    (active : m.state.terminal = .active) (pending : m.state.pending = some c)
    (future : through < c.actual) (positiveSlots : 0 < slots) :
    run through fuel slots m = done m .waiting := by
  simp [run, loop, active, pending, future, show ¬slots = 0 by omega]

-- A complete source-to-output statement: hypotheses constrain source ordering,
-- resource positivity and the cursor, never the desired output or completion.
theorem initial_resumability (identity : String) (revision lower after through : Nat)
    (raw : List Candidate) (calls : List Call)
    (nominalOrder : raw.Pairwise (fun a b => a.nominal < b.nominal))
    (actualOrder : raw.Pairwise (fun a b => a.actual ≤ b.actual))
    (budgets : ∀ c ∈ calls, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through)
    (enough : rank (beginMachine identity revision lower after raw) < calls.length) :
    (resume calls (beginMachine identity revision lower after raw)).output =
      raw.filter (fun c => after < c.nominal && !c.excluded && c.actual ≤ through) ∧
    (resume calls (beginMachine identity revision lower after raw)).output.Pairwise
      (fun a b => a.nominal < b.nominal) := by
  let m := beginMachine identity revision lower after raw
  have frontier : Frontier m := begin_frontier identity revision lower after raw nominalOrder
  have actual : (remaining m).Pairwise (fun a b => a.actual ≤ b.actual) := by
    simpa [m, beginMachine, restore, remaining] using
      List.Pairwise.filter (fun c => after < c.nominal) actualOrder
  have complete := resumable_completeness through calls m budgets enough (Or.inl rfl)
  rw [expected_filter through _ actual] at complete
  constructor
  · simpa [m, beginMachine, restore, remaining, eligible, List.filter_filter, Bool.and_assoc, Bool.and_comm, Bool.and_left_comm] using complete
  · exact resume_unique calls m (List.Pairwise.filter _ (frontier_remaining_ordered m frontier))

end Scheduler2.Exact
