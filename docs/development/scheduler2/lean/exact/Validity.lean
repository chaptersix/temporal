import Controls
open Scheduler2.Exact
namespace Scheduler2.Wire

@[simp] theorem stop_beq (a b : Stop) : (a == b) = decide (a = b) := by
  cases a <;> cases b <;> rfl
@[simp] theorem stop_bne (a b : Stop) : (a != b) = decide (a ≠ b) := by
  cases a <;> cases b <;> rfl

-- These are input-stream assumptions, not conclusions about enumeration output.
def CandidateValid (revision : Nat) (c : Candidate) : Prop :=
  c.revision = revision ∧ supported c.nominal = true ∧ supported c.actual = true ∧
  (c.nominal - 1) % 1000000000 = 0 ∧ c.nominal ≤ c.actual ∧
  (c.excluded = true → c.actual = c.nominal)

def SourceValid (src : Source) : Prop :=
  src.identity.isEmpty = false ∧ 0 < src.revision ∧ supported src.lower = true ∧
  supported src.nominalEnd = true ∧ src.upper = upper ∧
  (∀ c ∈ src.raw, CandidateValid src.revision c) ∧
  src.raw.Pairwise (fun a b => a.nominal < b.nominal) ∧
  src.raw.Pairwise (fun a b => a.actual ≤ b.actual) ∧
  (src.hasInclusions = false → src.raw = [])

theorem initial_valid (src : Source) (after : Nat) (hsrc : SourceValid src)
    (ha : after ≤ upper) : validState (initial src after) = true := by
  rcases hsrc with ⟨hi, hr, _⟩
  simp [validState, initial, beginMachine, restore, hi, hr, ha]

theorem disposition_valid (s : State) (c : Candidate)
    (h : validState s = true) (hp : s.pending = some c) :
    validState { s with pending := none, disposedThrough := c.nominal } = true := by
  simp_all [validState, supported]
  omega

theorem probe_valid (s : State) (c : Candidate)
    (h : validState s = true) (active : s.terminal = .active)
    (hp : s.pending = none) (future : s.searchAfter < c.nominal)
    (hc : CandidateValid s.revision c) :
    validState { s with searchAfter := c.nominal, pending := some c, eligibleSeen := s.eligibleSeen || !c.excluded } = true := by
  rcases hc with ⟨rev, nom, act, aligned, forward, excluded⟩
  cases he : c.excluded <;> simp_all [validState, supported]
  all_goals omega

theorem terminal_valid (s : State)
    (h : validState s = true) (hp : s.pending = none) :
    validState { s with terminal := if s.fullRange && !s.eligibleSeen then Stop.impossible else Stop.exhausted } = true := by
  cases hf : s.fullRange <;> cases he : s.eligibleSeen <;>
    simp_all [validState, supported]
  all_goals omega

theorem loop_valid (through ticks fuel slots : Nat) (m : Machine)
    (hv : validState m.state = true) (hf : Frontier m)
    (hr : ∀ c ∈ m.rest, CandidateValid m.state.revision c) :
    validState (loop through ticks fuel slots m).machine.state = true := by
  induction ticks generalizing fuel slots m with
  | zero => exact hv
  | succ ticks ih =>
    simp only [loop]
    split
    · exact hv
    · rename_i active
      have active : m.state.terminal = .active := by simpa using active
      split
      · exact hv
      · split
        · rename_i c hp
          split
          · exact hv
          · split
            · exact ih _ _ _ (disposition_valid _ _ hv hp) (dispose_frontier _ _ hf hp) hr
            · exact ih _ _ _ (disposition_valid _ _ hv hp) (dispose_frontier _ _ hf hp) hr
        · rename_i hp
          split
          · exact hv
          · split
            · exact terminal_valid _ hv hp
            · rename_i c cs rest
              have hc := hr c (by simp [rest])
              have future := hf.2.1 c (by simp [rest])
              exact ih _ _ _ (probe_valid _ _ hv active hp future hc)
                (probe_frontier _ _ _ hf hp rest)
                (by intro x hx; exact hr x (by simp [rest, hx]))

theorem run_valid (through fuel slots : Nat) (m : Machine)
    (hv : validState m.state = true) (hf : Frontier m)
    (hr : ∀ c ∈ m.rest, CandidateValid m.state.revision c) :
    validState (run through fuel slots m).machine.state = true :=
  loop_valid _ _ _ _ _ hv hf hr

theorem dispose_valid (src : Source) (s next : State)
    (success : dispose src s = .ok next) : validState next = true := by
  have hs := dispose_success src s next success
  rcases hs with ⟨c, hp, _, rfl⟩
  apply disposition_valid _ _ ?_ hp
  unfold dispose at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · simpa using ‹¬(!validState s) = true›

theorem scan_active_success (src : Source) (s : State) (through probes outputs : Nat)
    (identity : src.identity = s.identity) (revision : src.revision = s.revision)
    (hv : validState s = true) (active : s.terminal = .active)
    (full : s.fullRange = decide (s.origin < src.lower))
    (inclusions : src.hasInclusions = true) (window : supported through = true)
    (budget : 1 ≤ probes ∧ probes ≤ 1024 ∧ 1 ≤ outputs ∧ outputs ≤ 128)
    (source : ∀ c ∈ src.raw, CandidateValid src.revision c) :
    scan src s through probes outputs false = .ok
      (run through probes outputs (restore
        (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s)) := by
  let m := restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s
  have safe : ∀ c ∈ m.rest, supported c.actual = true := by
    intro c hc
    have raw : c ∈ src.raw := (List.mem_filter.mp (List.mem_filter.mp hc).1).1
    exact (source c raw).2.2.1
  have noBad : (m.rest.take (m.rest.length - (run through probes outputs m).machine.rest.length)).any
      (fun c => !c.excluded && !supported c.actual) = false := by
    apply List.any_eq_false.mpr
    intro c hc
    simp [safe c (List.mem_of_mem_take hc)]
  have noLimit := run_no_internal_limit through probes outputs m (by simp [m, restore, active])
  dsimp [m] at noBad noLimit
  simp only [scan, identity, revision, hv, active, full, inclusions, window,
    show ¬probes < 1 by omega, show ¬probes > 1024 by omega,
    show ¬outputs < 1 by omega, show ¬outputs > 128 by omega,
    Bool.not_true, beq_self_eq_true, Bool.false_or, Bool.true_and,
    Bool.false_and, Bool.false_eq_true, ↓reduceIte, decide_false, decide_true,
    Bool.or_false, Bool.not_false, bne_self_eq_false, noBad, stop_beq, noLimit]
  rfl

theorem restore_frontier (raw : List Candidate) (s : State)
    (ordered : raw.Pairwise (fun a b => a.nominal < b.nominal))
    (valid : validState s = true) : Frontier (restore raw s) := by
  refine ⟨ordered.filter _, ?_, ?_, ?_⟩
  · intro c hc
    simpa using (List.mem_filter.mp hc).2
  · intro hp
    simp only [restore] at hp ⊢
    simp [validState, hp] at valid
    omega
  · intro c hp
    simp only [restore] at hp ⊢
    simp [validState, hp] at valid
    omega

theorem scan_active_valid (src : Source) (s : State) (through probes outputs : Nat)
    (result : Result) (source : SourceValid src) (valid : validState s = true)
    (revision : src.revision = s.revision) (active : s.terminal = .active)
    (inclusions : src.hasInclusions = true)
    (success : scan src s through probes outputs false = .ok result) :
    validState result.machine.state = true := by
  rw [scan_success_kernel _ _ _ _ _ _ active inclusions success]
  apply run_valid
  · exact valid
  · exact restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) valid
  · intro c hc
    have raw : c ∈ src.raw := (List.mem_filter.mp (List.mem_filter.mp hc).1).1
    simpa [revision, restore] using source.2.2.2.2.2.1 c raw

theorem replacement_valid (src : Source) (old next : State) (atTime : Option Nat)
    (superseded : Option Candidate) (source : SourceValid src)
    (success : replacement src old atTime = .ok (next, superseded)) :
    validState next = true := by
  have atBoundary : ∀ boundary, replacementAt src old boundary = .ok (next, superseded) →
      validState next = true := by
    intro boundary hs
    unfold replacementAt at hs
    split at hs
    · simp at hs
    · rename_i result hb
      simp only [Except.ok.injEq, Prod.mk.injEq] at hs
      rw [← hs.1]
      unfold begin at hb
      split at hb
      · simp at hb
      · rename_i bound
        simp only [Except.ok.injEq] at hb
        rw [← hb]
        exact initial_valid src boundary source (Nat.le_of_not_gt bound)
  unfold replacement at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · cases atTime with
      | none => exact atBoundary _ success
      | some time =>
        simp only at success
        split at success
        · simp at success
        · exact atBoundary _ success

theorem loop_zero_slots_state (through ticks fuel : Nat) (m : Machine) :
    (loop through ticks fuel 0 m).machine = m ∧ (loop through ticks fuel 0 m).output = [] := by
  cases ticks with
  | zero => simp [loop, done]
  | succ ticks => simp [loop, done]; split <;> simp [done]

theorem loop_one_output (through ticks fuel : Nat) (m : Machine) (c : Candidate) (tail : List Candidate)
    (frontier : Frontier m) (output : (loop through ticks fuel 1 m).output = c :: tail) :
    tail = [] ∧ (loop through ticks fuel 1 m).machine.state.pending = none ∧
    (loop through ticks fuel 1 m).machine.state.disposedThrough = c.nominal ∧
    (loop through ticks fuel 1 m).machine.state.searchAfter = c.nominal ∧
    (loop through ticks fuel 1 m).machine.state.terminal = .active ∧ c.excluded = false := by
  induction ticks generalizing fuel m with
  | zero => simp [loop, done] at output
  | succ ticks ih =>
    by_cases ht : m.state.terminal = .active
    · cases hp : m.state.pending with
      | some b =>
        by_cases wait : through < b.actual
        · simp [loop, ht, hp, wait, done] at output
        · cases he : b.excluded with
          | true =>
            simp only [loop, ht, hp, wait, he, ↓reduceIte, show (1 == 0) = false from rfl,
              Bool.false_eq_true] at output ⊢
            exact ih _ _ (dispose_frontier _ _ frontier hp) output
          | false =>
            have hz := loop_zero_slots_state through ticks fuel
              { m with state := { m.state with pending := none, disposedThrough := b.nominal } }
            simp only [loop, ht, hp, wait, he, ↓reduceIte, show (1 == 0) = false from rfl,
              Bool.false_eq_true, Nat.reduceSub, hz.2, List.cons.injEq] at output
            rcases output with ⟨rfl, rfl⟩
            simp only [ht] at hz
            simp [loop, ht, hp, wait, he, hz.1, hz.2]
            exact (frontier.2.2.2 c hp).2.symm
      | none =>
        by_cases fuel0 : fuel = 0
        · simp [loop, ht, hp, fuel0, done] at output
        · cases rest : m.rest with
          | nil => simp [loop, ht, hp, fuel0, rest] at output
          | cons b bs =>
            simp only [loop, ht, hp, fuel0, rest, ↓reduceIte,
              show (1 == 0) = false from rfl, Bool.false_eq_true,
              show (fuel == 0) = false from by simp [fuel0]] at output ⊢
            exact ih _ _ (probe_frontier _ _ _ frontier hp rest) output
    · simp [loop, ht, done] at output

theorem run_output_member (through fuel slots : Nat) (m : Machine) (c : Candidate)
    (member : c ∈ (run through fuel slots m).output) : c ∈ remaining m := by
  have hc : c ∈ eligible (remaining m) := by
    rw [← run_conservation through fuel slots m]
    exact List.mem_append_left _ member
  exact (List.mem_filter.mp hc).1

theorem remaining_after_disposed (m : Machine) (frontier : Frontier m)
    (valid : validState m.state = true) :
    ∀ c ∈ remaining m, m.state.disposedThrough < c.nominal := by
  intro c hc
  have bound : m.state.disposedThrough ≤ m.state.searchAfter := by
    cases hp : m.state.pending <;> simp [validState, hp] at valid <;> omega
  simp only [remaining, List.mem_append] at hc
  rcases hc with hp | hr
  · cases h : m.state.pending with
    | none => simp [h] at hp
    | some b =>
      simp only [h, Option.toList_some, List.mem_singleton] at hp
      subst c
      exact (frontier.2.2.2 b h).1
  · have hf := frontier.2.1 c hr
    omega

theorem fold_cursor_bounds (cs : List Candidate) (cursor top : Nat)
    (below : cursor < top) (members : ∀ c ∈ cs, cursor ≤ c.nominal ∧ c.nominal < top) :
    cursor ≤ cs.foldl (fun _ c => c.nominal) cursor ∧
      cs.foldl (fun _ c => c.nominal) cursor < top := by
  cases cs with
  | nil => simpa
  | cons c cs =>
    have last : ∀ (xs : List Candidate) (start : Nat),
        (cursor ≤ start ∧ start < top) →
        (∀ x ∈ xs, cursor ≤ x.nominal ∧ x.nominal < top) →
        cursor ≤ xs.foldl (fun _ c => c.nominal) start ∧ xs.foldl (fun _ c => c.nominal) start < top := by
      intro xs
      induction xs with
      | nil => intro start h _; exact h
      | cons x xs ih =>
        intro start h mem
        exact ih x.nominal (mem x (by simp)) (by intro y hy; exact mem y (by simp [hy]))
    exact last cs c.nominal (members c (by simp)) (by intro x hx; exact members x (by simp [hx]))

theorem pending_candidate_valid (s : State) (c : Candidate)
    (valid : validState s = true) (pending : s.pending = some c) :
    CandidateValid s.revision c := by
  cases excluded : c.excluded <;> simp_all [validState, CandidateValid, supported]
  all_goals omega

theorem loop_seen_true (through ticks fuel slots : Nat) (m : Machine)
    (seen : m.state.eligibleSeen = true) :
    (loop through ticks fuel slots m).machine.state.eligibleSeen = true := by
  induction ticks generalizing fuel slots m with
  | zero => exact seen
  | succ ticks ih =>
    simp only [loop]
    split
    · exact seen
    · split
      · exact seen
      · split
        · split
          · exact seen
          · split <;> exact ih _ _ _ seen
        · split
          · exact seen
          · split
            · exact seen
            · apply ih; simp [seen]

theorem loop_seen (through ticks fuel slots : Nat) (m : Machine)
    (seen : ∀ c, m.state.pending = some c → c.excluded = false → m.state.eligibleSeen = true)
    (output : (loop through ticks fuel slots m).output ≠ []) :
    (loop through ticks fuel slots m).machine.state.eligibleSeen = true := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done] at output
  | succ ticks ih =>
    simp only [loop] at output ⊢
    split
    · simp_all [done]
    · rename_i ht
      split
      · simp_all [done]
      · split
        · rename_i c hp
          split
          · simp_all [done]
          · split
            · apply ih
              · simp
              · simpa [ht, hp, *] using output
            · have hs := seen c hp (by simpa using ‹¬c.excluded = true›)
              exact loop_seen_true _ _ _ _ _ hs
        · rename_i hp
          split
          · simp_all [done]
          · split
            · simp_all
            · rename_i c cs hr
              apply ih
              · intro b hb he
                have eq : c = b := Option.some.inj hb
                subst b
                simp [he]
              · simpa [ht, hp, hr, *] using output

theorem peek_run_valid (through fuel : Nat) (m : Machine)
    (valid : validState m.state = true) (frontier : Frontier m)
    (raw : ∀ c ∈ m.rest, CandidateValid m.state.revision c) :
    validState (asPeek m (run through fuel 1 m)).machine.state = true := by
  have runValid := run_valid through fuel 1 m valid frontier raw
  cases output : (run through fuel 1 m).output with
  | nil => simpa [asPeek, output] using runValid
  | cons c cs =>
    have seen := loop_seen through (2 * m.rest.length + m.state.pending.toList.length + 1) fuel 1 m
      (by intro x hp he; simp [validState, hp, he] at valid; exact valid.2.2)
      (by simp [run] at output; rw [output]; simp)
    change (run through fuel 1 m).machine.state.eligibleSeen = true at seen
    have shape := loop_one_output through _ fuel m c cs frontier output
    change cs = [] ∧ (run through fuel 1 m).machine.state.pending = none ∧
      (run through fuel 1 m).machine.state.disposedThrough = c.nominal ∧
      (run through fuel 1 m).machine.state.searchAfter = c.nominal ∧
      (run through fuel 1 m).machine.state.terminal = .active ∧ c.excluded = false at shape
    have mem := run_output_member through fuel 1 m c (by simp [output])
    have after := remaining_after_disposed m frontier valid c mem
    let earlier := (remaining m).takeWhile (fun x => x.nominal < c.nominal)
    have cb := fold_cursor_bounds earlier m.state.disposedThrough c.nominal after (by
      intro x hx
      have hm := List.takeWhile_subset _ hx
      have low := remaining_after_disposed m frontier valid x hm
      have high := List.all_eq_true.mp (List.all_takeWhile (l := remaining m)) x hx
      exact ⟨by omega, by simpa using high⟩)
    have cv : CandidateValid m.state.revision c := by
      simp only [remaining, List.mem_append] at mem
      rcases mem with hp | hr
      · cases h : m.state.pending with
        | none => simp [h] at hp
        | some b =>
          simp only [h, Option.toList_some, List.mem_singleton] at hp
          subst c
          exact pending_candidate_valid _ _ valid h
      · exact raw c hr
    have authority := loop_authority through (2 * m.rest.length + m.state.pending.toList.length + 1) fuel 1 m
    have revision : (run through fuel 1 m).machine.state.revision = m.state.revision := congrArg (fun a => a.2.1) authority
    have origin : (run through fuel 1 m).machine.state.origin = m.state.origin := congrArg (fun a => a.2.2.1) authority
    have originBound : m.state.origin ≤ m.state.disposedThrough := by
      cases hp : m.state.pending <;> simp [validState, hp] at valid <;> omega
    rcases cv with ⟨crev, nom, act, aligned, forward, excluded⟩
    simp only [asPeek, output]
    change validState { (run through fuel 1 m).machine.state with pending := some c, disposedThrough := earlier.foldl (fun _ x => x.nominal) m.state.disposedThrough } = true
    simp [validState, shape.2.2.2.1, shape.2.2.2.2.1, shape.2.2.2.2.2,
      revision, crev, nom, act, aligned, forward, origin, seen] at runValid ⊢
    simp_all [validState, shape.2.1, shape.2.2.1, revision, origin]
    all_goals omega

-- Peek retains the due occurrence and rewinds only its disposition. Committing
-- that occurrence returns the entire state produced by a one-output Run.
theorem peek_dispose_run (src : Source) (through fuel : Nat) (m : Machine)
    (valid : validState m.state = true) (frontier : Frontier m)
    (raw : ∀ c ∈ m.rest, CandidateValid m.state.revision c)
    (identity : src.identity = m.state.identity) (revision : src.revision = m.state.revision)
    (nonempty : (run through fuel 1 m).output ≠ []) :
    dispose src (asPeek m (run through fuel 1 m)).machine.state =
      .ok (run through fuel 1 m).machine.state := by
  have pv := peek_run_valid through fuel m valid frontier raw
  cases output : (run through fuel 1 m).output with
  | nil => exact False.elim (nonempty output)
  | cons c cs =>
    have shape := loop_one_output through _ fuel m c cs frontier output
    change cs = [] ∧ (run through fuel 1 m).machine.state.pending = none ∧
      (run through fuel 1 m).machine.state.disposedThrough = c.nominal ∧
      (run through fuel 1 m).machine.state.searchAfter = c.nominal ∧
      (run through fuel 1 m).machine.state.terminal = .active ∧ c.excluded = false at shape
    have auth := loop_authority through (2 * m.rest.length + m.state.pending.toList.length + 1) fuel 1 m
    have id : (run through fuel 1 m).machine.state.identity = src.identity :=
      (congrArg (fun a => a.1) auth).trans identity.symm
    have rev : (run through fuel 1 m).machine.state.revision = src.revision :=
      (congrArg (fun a => a.2.1) auth).trans revision.symm
    simp only [dispose, pv, Bool.not_true]
    simp only [asPeek, output, id, rev, bne_self_eq_false, Bool.false_or,
      Bool.false_eq_true, ↓reduceIte, shape.2.2.2.2.2]
    congr 1
    have hp := shape.2.1
    have hd := shape.2.2.1
    cases hs : (run through fuel 1 m).machine.state
    simp_all

theorem scan_peek_of_run (src : Source) (s : State) (through probes : Nat) (result : Result)
    (active : s.terminal = .active) (inclusions : src.hasInclusions = true)
    (success : scan src s through probes 1 false = .ok result) :
    scan src s through probes 1 true = .ok
      (asPeek (restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s) result) := by
  unfold scan at success ⊢
  split at success
  · simp at success
  · rename_i hi
    rw [if_neg hi]
    split at success
    · simp at success
    · rename_i hb
      rw [if_neg hb]
      split at success
      · simp at success
      · rename_i hv
        rw [if_neg hv]
        simp only [active, inclusions, show (Stop.active != Stop.active) = false from rfl,
          Bool.not_true, Bool.false_eq_true, ↓reduceIte] at success ⊢
        split at success
        · simp at success
        · rename_i bad
          rw [if_neg bad]
          split at success
          · simp at success
          · rename_i limit
            rw [if_neg limit]
            simpa only [Except.ok.injEq] using congrArg (asPeek
              (restore (src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)) s))
              (Except.ok.inj success)

end Scheduler2.Wire
