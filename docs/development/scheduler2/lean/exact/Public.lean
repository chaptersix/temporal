import Validity
open Scheduler2.Exact
namespace Scheduler2.Wire

def bounded (src : Source) := src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)

def Ready (src : Source) (s : State) : Prop :=
  validState s = true ∧ src.identity = s.identity ∧ src.revision = s.revision ∧
  s.fullRange = decide (s.origin < src.lower) ∧ s.terminal ≠ .empty ∧
  (s.terminal = .active ∨ remaining (restore (bounded src) s) = [])

theorem loop_not_empty (through ticks fuel slots : Nat) (m : Machine)
    (h : m.state.terminal ≠ .empty) :
    (loop through ticks fuel slots m).machine.state.terminal ≠ .empty := by
  induction ticks generalizing fuel slots m with
  | zero => exact h
  | succ ticks ih =>
    simp only [loop]
    split
    · exact h
    · split
      · exact h
      · split
        · split
          · exact h
          · split <;> exact ih _ _ _ h
        · split
          · exact h
          · split
            · dsimp; split <;> simp
            · exact ih _ _ _ h

theorem ready_run (src : Source) (s : State) (through probes outputs : Nat)
    (source : SourceValid src) (ready : Ready src s) :
    Ready src (run through probes outputs (restore (bounded src) s)).machine.state := by
  rcases ready with ⟨hv, hi, hr, hf, he, hc⟩
  let m := restore (bounded src) s
  have frontier : Frontier m := restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) hv
  have raw : ∀ c ∈ m.rest, CandidateValid m.state.revision c := by
    intro c hm
    have mem : c ∈ src.raw := (List.mem_filter.mp (List.mem_filter.mp hm).1).1
    simpa [m, restore, hr] using source.2.2.2.2.2.1 c mem
  have auth := loop_authority through (2 * m.rest.length + m.state.pending.toList.length + 1) probes outputs m
  have aid : (run through probes outputs m).machine.state.identity = s.identity := congrArg (fun a => a.1) auth
  have arev : (run through probes outputs m).machine.state.revision = s.revision := congrArg (fun a => a.2.1) auth
  have aorigin : (run through probes outputs m).machine.state.origin = s.origin := congrArg (fun a => a.2.2.1) auth
  have afull : (run through probes outputs m).machine.state.fullRange = s.fullRange := congrArg (fun a => a.2.2.2) auth
  refine ⟨run_valid _ _ _ _ hv frontier raw, hi.trans aid.symm, hr.trans arev.symm, ?_,
    loop_not_empty _ _ _ _ _ he, ?_⟩
  · change (run through probes outputs m).machine.state.fullRange = decide ((run through probes outputs m).machine.state.origin < src.lower)
    simpa only [afull, aorigin] using hf
  · have cached := loop_cache_preserved through (2 * m.rest.length + m.state.pending.toList.length + 1) probes outputs m hc
    rw [run_reconstructible _ _ _ _ _ frontier rfl]
    exact cached

theorem ready_scan_success (src : Source) (s : State) (through probes outputs : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (window : supported through = true)
    (budget : 1 ≤ probes ∧ probes ≤ 1024 ∧ 1 ≤ outputs ∧ outputs ≤ 128) :
    scan src s through probes outputs false =
      .ok (run through probes outputs (restore (bounded src) s)) := by
  rcases ready with ⟨hv, hi, hr, hf, he, hc⟩
  by_cases active : s.terminal = .active
  · exact scan_active_success _ _ _ _ _ hi hr hv active hf inclusions window budget source.2.2.2.2.2.1
  · have empty := hc.resolve_left active
    have rest : (restore (bounded src) s).rest = [] := by
      exact (List.append_eq_nil_iff.mp empty).2
    simp [scan, hi, hr, hv, hf, he, active, window, budget,
      show ¬probes < 1 by omega, show ¬probes > 1024 by omega,
      show ¬outputs < 1 by omega, show ¬outputs > 128 by omega,
      run, loop, restore, bounded, done] at rest ⊢
    exact rest

theorem initial_ready (src : Source) (after : Nat) (source : SourceValid src)
    (bound : after ≤ upper) : Ready src (initial src after) := by
  refine ⟨initial_valid _ _ source bound, ?_, ?_, ?_, ?_, Or.inl ?_⟩ <;>
    simp [initial, beginMachine, restore]

def PublicCallValid (c : Call) : Prop :=
  supported c.through = true ∧ 1 ≤ c.probes ∧ c.probes ≤ 1024 ∧ 1 ≤ c.outputs ∧ c.outputs ≤ 128

-- Each continuation consumes only the serialized public state. The private
-- source suffix is deliberately discarded and reconstructed by the next scan.
def publicResume (src : Source) : List Call → State → Except String Result
  | [], s => .ok (done (restore (bounded src) s) s.terminal)
  | c :: calls, s =>
    match scan src s c.through c.probes c.outputs false with
    | .error e => .error e
    | .ok first =>
      match publicResume src calls first.machine.state with
      | .error e => .error e
      | .ok rest => .ok { rest with output := first.output ++ rest.output, probes := first.probes + rest.probes }

theorem public_resume_kernel (src : Source) (calls : List Call) (s : State)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (valid : ∀ c ∈ calls, PublicCallValid c) :
    publicResume src calls s = .ok (resume calls (restore (bounded src) s)) := by
  induction calls generalizing s with
  | nil => rfl
  | cons c calls ih =>
    have head := valid c (by simp)
    have tail : ∀ c ∈ calls, PublicCallValid c := by
      intro c hc; exact valid c (by simp [hc])
    have hs := ready_scan_success src s c.through c.probes c.outputs source ready inclusions head.1 head.2
    have readyNext := ready_run src s c.through c.probes c.outputs source ready
    have frontier := restore_frontier (bounded src) s (source.2.2.2.2.2.2.1.filter _) ready.1
    have reconstruct := run_reconstructible (bounded src) c.through c.probes c.outputs
      (restore (bounded src) s) frontier rfl
    simp [publicResume, hs, ih _ readyNext tail, reconstruct, resume]

theorem public_resume_complete (src : Source) (calls : List Call) (s : State) (through : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (valid : ∀ c ∈ calls, PublicCallValid c)
    (windows : ∀ c ∈ calls, c.through = through)
    (enough : rank (restore (bounded src) s) < calls.length) :
    ∃ result, publicResume src calls s = .ok result ∧
      result.output = expected through (remaining (restore (bounded src) s)) ∧
      result.output.Pairwise (fun a b => a.nominal < b.nominal) := by
  refine ⟨resume calls (restore (bounded src) s), public_resume_kernel _ _ _ source ready inclusions valid, ?_, ?_⟩
  · apply resumable_completeness through calls _ _ enough ready.2.2.2.2.2
    intro c hc
    have h := valid c hc
    exact ⟨h.2.1, h.2.2.2.1, windows c hc⟩
  · apply resume_unique
    apply List.Pairwise.filter
    exact frontier_remaining_ordered _ (restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) ready.1)

theorem public_initial_complete (src : Source) (calls : List Call) (after through : Nat)
    (source : SourceValid src) (inclusions : src.hasInclusions = true) (bound : after ≤ upper)
    (valid : ∀ c ∈ calls, PublicCallValid c)
    (windows : ∀ c ∈ calls, c.through = through)
    (enough : rank (restore (bounded src) (initial src after)) < calls.length) :
    ∃ result, publicResume src calls (initial src after) = .ok result ∧
      result.output = expected through (remaining (restore (bounded src) (initial src after))) ∧
      result.output.Pairwise (fun a b => a.nominal < b.nominal) :=
  public_resume_complete _ _ _ _ source (initial_ready _ _ source bound) inclusions valid windows enough

theorem peek_reconstructible (raw : List Candidate) (initial : Machine) (r : Result)
    (reconstruct : restore raw r.machine.state = r.machine) :
    restore raw (asPeek initial r).machine.state = (asPeek initial r).machine := by
  cases output : r.output with
  | nil => simpa [asPeek, output] using reconstruct
  | cons c cs =>
    have rest := congrArg Machine.rest reconstruct
    simp only [restore] at rest
    simp only [asPeek, output, restore]
    rw [rest]

theorem ready_peek_run (src : Source) (s : State) (through probes : Nat)
    (source : SourceValid src) (ready : Ready src s) :
    Ready src (asPeek (restore (bounded src) s)
      (run through probes 1 (restore (bounded src) s))).machine.state := by
  let m := restore (bounded src) s
  let r := run through probes 1 m
  have next := ready_run src s through probes 1 source ready
  have frontier : Frontier m := restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) ready.1
  have raw : ∀ c ∈ m.rest, CandidateValid m.state.revision c := by
    intro c hm
    have mem : c ∈ src.raw := (List.mem_filter.mp (List.mem_filter.mp hm).1).1
    simpa [m, restore, ready.2.2.1] using source.2.2.2.2.2.1 c mem
  have pv := peek_run_valid through probes m ready.1 frontier raw
  change Ready src (asPeek m r).machine.state
  change Ready src r.machine.state at next
  cases output : r.output with
  | nil => simpa only [asPeek, output] using next
  | cons c cs =>
    have shape := loop_one_output through _ probes m c cs frontier output
    have active : r.machine.state.terminal = .active := shape.2.2.2.2.1
    change Ready src (asPeek m r).machine.state
    refine ⟨pv, ?_, ?_, ?_, ?_, Or.inl ?_⟩
    · simpa only [asPeek, output] using next.2.1
    · simpa only [asPeek, output] using next.2.2.1
    · simpa only [asPeek, output] using next.2.2.2.1
    · simpa only [asPeek, output] using next.2.2.2.2.1
    · simpa only [asPeek, output] using active

def EmptyReady (src : Source) (s : State) : Prop :=
  validState s = true ∧ src.identity = s.identity ∧ src.revision = s.revision ∧
  s.fullRange = decide (s.origin < src.lower) ∧ s.pending = none ∧
  (s.terminal = .active ∨ s.terminal = .empty)

theorem initial_empty_ready (src : Source) (after : Nat) (source : SourceValid src)
    (bound : after ≤ upper) : EmptyReady src (initial src after) := by
  refine ⟨initial_valid _ _ source bound, ?_, ?_, ?_, ?_, Or.inl ?_⟩ <;>
    simp [initial, beginMachine, restore]

theorem empty_ready_next (src : Source) (s : State) (ready : EmptyReady src s) :
    EmptyReady src { s with terminal := .empty } := by
  rcases ready with ⟨hv, hi, hr, hf, hp, ht⟩
  refine ⟨?_, hi, hr, hf, hp, Or.inr rfl⟩
  simp_all [validState]
  omega

theorem empty_scan_success (src : Source) (s : State) (c : Call)
    (ready : EmptyReady src s) (inclusions : src.hasInclusions = false)
    (valid : PublicCallValid c) (peek : Bool) :
    scan src s c.through c.probes c.outputs peek =
      .ok (done { state := { s with terminal := .empty }, rest := [] } .empty) := by
  rcases ready with ⟨hv, hi, hr, hf, hp, ht⟩
  rcases valid with ⟨window, pb, pe, ob, oe⟩
  rcases ht with ht | ht
  · simp [scan, hv, hi, hr, hf, hp, ht, inclusions, window,
      show ¬c.probes < 1 by omega, show ¬c.probes > 1024 by omega,
      show ¬c.outputs < 1 by omega, show ¬c.outputs > 128 by omega]
  · simp [scan, hv, hi, hr, hf, hp, ht, inclusions, window,
      show ¬c.probes < 1 by omega, show ¬c.probes > 1024 by omega,
      show ¬c.outputs < 1 by omega, show ¬c.outputs > 128 by omega]
    cases s; simp_all

theorem public_empty_resume (src : Source) (calls : List Call) (s : State)
    (ready : EmptyReady src s) (inclusions : src.hasInclusions = false)
    (valid : ∀ c ∈ calls, PublicCallValid c) :
    ∃ result, publicResume src calls s = .ok result ∧ result.output = [] ∧
      EmptyReady src result.machine.state := by
  induction calls generalizing s with
  | nil => exact ⟨done (restore (bounded src) s) s.terminal, rfl, rfl, ready⟩
  | cons c calls ih =>
    have hs := empty_scan_success src s c ready inclusions (valid c (by simp)) false
    have tail : ∀ c ∈ calls, PublicCallValid c := by intro c hc; exact valid c (by simp [hc])
    obtain ⟨result, success, empty, readyNext⟩ := ih _ (empty_ready_next src s ready) tail
    refine ⟨result, ?_, empty, readyNext⟩
    simp [publicResume, hs, success, done]

theorem replacement_initial (src : Source) (old next : State) (atTime : Option Nat)
    (superseded : Option Candidate)
    (success : replacement src old atTime = .ok (next, superseded)) :
    ∃ boundary, boundary ≤ upper ∧ next = initial src boundary := by
  have atBoundary : ∀ boundary, replacementAt src old boundary = .ok (next, superseded) →
      boundary ≤ upper ∧ next = initial src boundary := by
    intro boundary hs
    unfold replacementAt at hs
    split at hs
    · simp at hs
    · rename_i result hb
      simp only [Except.ok.injEq, Prod.mk.injEq] at hs
      unfold begin at hb
      split at hb
      · simp at hb
      · rename_i bound
        simp only [Except.ok.injEq] at hb
        exact ⟨Nat.le_of_not_gt bound, hs.1.symm.trans hb.symm⟩
  unfold replacement at success
  split at success
  · simp at success
  · split at success
    · simp at success
    · cases atTime with
      | none => exact ⟨_, atBoundary _ success⟩
      | some time =>
        simp only at success
        split at success
        · simp at success
        · exact ⟨_, atBoundary _ success⟩

theorem replacement_ready (src : Source) (old next : State) (atTime : Option Nat)
    (superseded : Option Candidate) (source : SourceValid src)
    (success : replacement src old atTime = .ok (next, superseded)) :
    Ready src next ∧ EmptyReady src next := by
  obtain ⟨boundary, bound, rfl⟩ := replacement_initial src old next atTime superseded success
  exact ⟨initial_ready _ _ source bound, initial_empty_ready _ _ source bound⟩

-- A due Peek followed by Dispose rejoins the exact serialized Run state, so
-- arbitrary subsequent supported Run continuations inherit Ready and completeness.
theorem public_peek_dispose (src : Source) (s : State) (through probes : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (window : supported through = true) (budget : 1 ≤ probes ∧ probes ≤ 1024)
    (due : (run through probes 1 (restore (bounded src) s)).output ≠ []) :
    ∃ peeked, scan src s through probes 1 true = .ok peeked ∧
      Ready src peeked.machine.state ∧
      dispose src peeked.machine.state = .ok (run through probes 1 (restore (bounded src) s)).machine.state ∧
      Ready src (run through probes 1 (restore (bounded src) s)).machine.state := by
  let m := restore (bounded src) s
  let r := run through probes 1 m
  have success := ready_scan_success src s through probes 1 source ready inclusions window
    ⟨budget.1, budget.2, by omega, by omega⟩
  have frontier : Frontier m := restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) ready.1
  have raw : ∀ c ∈ m.rest, CandidateValid m.state.revision c := by
    intro c hm
    have mem : c ∈ src.raw := (List.mem_filter.mp (List.mem_filter.mp hm).1).1
    simpa [m, restore, ready.2.2.1] using source.2.2.2.2.2.1 c mem
  have active : s.terminal = .active := by
    by_cases h : s.terminal = .active
    · exact h
    · simp [run, loop, m, restore, h, done] at due
  refine ⟨asPeek m r, scan_peek_of_run src s through probes r active inclusions success,
    ready_peek_run _ _ _ _ source ready, ?_, ready_run _ _ _ _ _ source ready⟩
  exact peek_dispose_run src through probes m ready.1 frontier raw ready.2.1 ready.2.2.1 due

theorem peek_pending_stable (through probes : Nat) (m : Machine) (c : Candidate)
    (active : m.state.terminal = .active) (pending : m.state.pending = some c)
    (eligible : c.excluded = false) (due : c.actual ≤ through) :
    asPeek m (run through probes 1 m) = done m .active := by
  simp [run, loop, pending, active, eligible, show ¬through < c.actual by omega,
    asPeek, remaining, done]
  cases m with
  | mk s rest => cases s; simp_all

theorem public_capacity_wait (src : Source) (s : State) (c : Candidate) (through probes : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (window : supported through = true) (budget : 1 ≤ probes ∧ probes ≤ 1024)
    (pending : s.pending = some c) (eligible : c.excluded = false) (due : c.actual ≤ through) :
    scan src s through probes 1 true = .ok (done (restore (bounded src) s) .active) := by
  have active : s.terminal = .active := by
    have valid := ready.1
    simp [validState, pending] at valid
    exact valid.2.1.1.1.1.1.1.1.1.1
  have success := ready_scan_success src s through probes 1 source ready inclusions window
    ⟨budget.1, budget.2, by omega, by omega⟩
  have peek := scan_peek_of_run src s through probes _ active inclusions success
  change scan src s through probes 1 true = .ok (asPeek (restore (bounded src) s) (run through probes 1 (restore (bounded src) s))) at peek
  rw [peek_pending_stable through probes (restore (bounded src) s) c active pending eligible due] at peek
  exact peek

theorem loop_all_excluded_impossible (through ticks fuel slots : Nat) (m : Machine)
    (active : m.state.terminal = .active) (full : m.state.fullRange = true)
    (unseen : m.state.eligibleSeen = false)
    (allExcluded : ∀ c ∈ remaining m, c.excluded = true ∧ c.actual ≤ through)
    (enough : rank m < ticks) (probes : m.rest.length < fuel) (outputs : 0 < slots) :
    (loop through ticks fuel slots m).machine.state.terminal = .impossible ∧
      (loop through ticks fuel slots m).stop = .impossible := by
  induction ticks generalizing fuel slots m with
  | zero => omega
  | succ ticks ih =>
    cases hp : m.state.pending with
    | some c =>
      have hc := allExcluded c (by simp [remaining, hp])
      have smaller : rank { m with state := { m.state with pending := none, disposedThrough := c.nominal } } < ticks := by
        simp [rank, hp] at enough ⊢; omega
      have tail : ∀ x ∈ remaining { m with state := { m.state with pending := none, disposedThrough := c.nominal } },
          x.excluded = true ∧ x.actual ≤ through := by
        intro x hx; exact allExcluded x (by simpa [remaining, hp] using List.mem_cons_of_mem c hx)
      simpa [loop, active, hp, hc.1, show ¬through < c.actual by omega, show ¬slots = 0 by omega] using
        ih fuel slots { m with state := { m.state with pending := none, disposedThrough := c.nominal } } active full unseen tail smaller probes outputs
    | none =>
      cases hr : m.rest with
      | nil => simp [loop, active, hp, hr, full, unseen, show ¬slots = 0 by omega, show ¬fuel = 0 by simpa [hr] using Nat.ne_of_gt probes]
      | cons c cs =>
        have hc := allExcluded c (by simp [remaining, hp, hr])
        have smaller : rank { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs } < ticks := by
          simp [rank, hp, hr] at enough ⊢; omega
        have tail : ∀ x ∈ remaining { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs },
            x.excluded = true ∧ x.actual ≤ through := by
          intro x hx; exact allExcluded x (by simpa [remaining, hp, hr] using hx)
        have nextProbes : cs.length < fuel - 1 := by simp [hr] at probes; omega
        simpa [loop, active, hp, hr, show ¬slots = 0 by omega, show ¬fuel = 0 by simp [hr] at probes; omega] using
          ih (fuel - 1) slots { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs } active full (by simp [unseen, hc.1]) tail smaller nextProbes outputs

theorem run_impossible_no_eligible (src : Source) (s : State) (through probes outputs : Nat)
    (source : SourceValid src) (ready : Ready src s)
    (impossible : (run through probes outputs (restore (bounded src) s)).machine.state.terminal = .impossible) :
    eligible (remaining (restore (bounded src) s)) = [] := by
  let m := restore (bounded src) s
  let r := run through probes outputs m
  have next := ready_run src s through probes outputs source ready
  have empty : remaining r.machine = [] := by
    have cached := next.2.2.2.2.2
    rw [run_reconstructible (bounded src) through probes outputs m
      (restore_frontier _ _ (source.2.2.2.2.2.2.1.filter _) ready.1) rfl] at cached
    exact cached.resolve_left (by simp [impossible])
  have output : r.output = [] := by
    by_cases h : r.output = []
    · exact h
    · have seen := loop_seen through (2 * m.rest.length + m.state.pending.toList.length + 1) probes outputs m
        (by intro x hp he; change s.pending = some x at hp; have valid := ready.1; simp [validState, hp, he] at valid; exact valid.2.2) h
      have valid := next.1
      change r.machine.state.eligibleSeen = true at seen
      change r.machine.state.terminal = .impossible at impossible
      change validState r.machine.state = true at valid
      simp [validState, impossible, seen] at valid
  have conserved := run_conservation through probes outputs m
  change r.output ++ eligible (remaining r.machine) = eligible (remaining m) at conserved
  simpa [output, empty, eligible] using conserved.symm

theorem full_initial_remaining (src : Source) (after : Nat) (full : after < src.lower) :
    remaining (restore (bounded src) (initial src after)) = bounded src := by
  simp only [remaining, restore, initial, beginMachine, Option.toList_none, List.nil_append]
  apply List.filter_eq_self.mpr
  intro c hc
  have lower := (List.mem_filter.mp hc).2
  simp only [Bool.and_eq_true, decide_eq_true_eq] at lower ⊢
  omega

theorem initial_impossible_classification (src : Source) (after through probes outputs : Nat)
    (source : SourceValid src) (bound : after ≤ upper) (full : after < src.lower)
    (impossible : (run through probes outputs (restore (bounded src) (initial src after))).machine.state.terminal = .impossible) :
    eligible (bounded src) = [] := by
  have h := run_impossible_no_eligible src (initial src after) through probes outputs source
    (initial_ready _ _ source bound) impossible
  simpa only [full_initial_remaining src after full] using h

theorem initial_all_excluded_impossible (src : Source) (after fuel slots : Nat)
    (source : SourceValid src) (full : after < src.lower)
    (allExcluded : ∀ c ∈ bounded src, c.excluded = true)
    (probes : (restore (bounded src) (initial src after)).rest.length < fuel)
    (outputs : 0 < slots) :
    (run upper fuel slots (restore (bounded src) (initial src after))).machine.state.terminal = .impossible ∧
    (run upper fuel slots (restore (bounded src) (initial src after))).stop = .impossible := by
  apply loop_all_excluded_impossible
  · rfl
  · simp [restore, initial, beginMachine, full]
  · rfl
  · intro c hc
    rw [full_initial_remaining src after full] at hc
    refine ⟨allExcluded c hc, ?_⟩
    have valid := source.2.2.2.2.2.1 c (List.mem_filter.mp hc).1
    have supported := valid.2.2.1
    simp only [Wire.supported, Bool.and_eq_true, decide_eq_true_eq] at supported
    exact supported.2
  · simp [rank]
  · exact probes
  · exact outputs

theorem scan_invalid_state (src : Source) (s : State) (c : Call) (peek : Bool)
    (identity : src.identity = s.identity) (revision : src.revision = s.revision)
    (call : PublicCallValid c) (invalid : validState s = false) :
    scan src s c.through c.probes c.outputs peek = .error "state-invalid" := by
  rcases call with ⟨window, pb, pe, ob, oe⟩
  simp [scan, identity, revision, window, invalid,
    show ¬c.probes < 1 by omega, show ¬c.probes > 1024 by omega,
    show ¬c.outputs < 1 by omega, show ¬c.outputs > 128 by omega]

theorem dispose_invalid_state (src : Source) (s : State)
    (identity : src.identity = s.identity) (revision : src.revision = s.revision)
    (invalid : validState s = false) : dispose src s = .error "state-invalid" := by
  simp [dispose, identity, revision, invalid]

theorem replacement_invalid_state (src : Source) (s : State) (atTime : Option Nat)
    (invalid : validState s = false) : replacement src s atTime = .error "state-invalid" := by
  simp [replacement, invalid]

end Scheduler2.Wire
