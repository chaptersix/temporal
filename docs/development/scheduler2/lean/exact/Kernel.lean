import Std

namespace Scheduler2.Exact

structure Candidate where
  nominal : Nat
  actual : Nat
  revision : Nat
  excluded : Bool
  deriving Repr, BEq, DecidableEq

inductive Stop where
  | active | empty | impossible | exhausted | waiting | yielded | limited | internalLimit
  deriving Repr, BEq, DecidableEq

structure State where
  identity : String
  revision : Nat
  origin : Nat
  searchAfter : Nat
  disposedThrough : Nat
  pending : Option Candidate := none
  fullRange : Bool
  eligibleSeen : Bool := false
  terminal : Stop := .active
  deriving Repr, BEq, DecidableEq

-- `rest` is an execution-local view of the immutable source suffix. It is not
-- serialized in the continuation, and is reconstructed on each public call.
structure Machine where
  state : State
  rest : List Candidate
  deriving Repr, BEq, DecidableEq

structure Result where
  machine : Machine
  output : List Candidate := []
  probes : Nat := 0
  stop : Stop
  deriving Repr, BEq, DecidableEq

def remaining (m : Machine) : List Candidate := m.state.pending.toList ++ m.rest
def eligible (cs : List Candidate) : List Candidate := cs.filter (! ·.excluded)

def done (m : Machine) (stop : Stop) : Result := { machine := m, stop }

-- Each raw probe moves exactly one candidate from rest to pending; disposition
-- removes only pending. Candidate identity and jitter are never recomputed.
def loop (through : Nat) : Nat → Nat → Nat → Machine → Result
  | 0, _, _, m => done m .internalLimit
  | ticks + 1, fuel, slots, m =>
    if m.state.terminal ≠ .active then done m m.state.terminal
    else if slots == 0 then done m .limited
    else match m.state.pending with
    | some c =>
      if through < c.actual then done m .waiting
      else
        let next := { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
        if c.excluded then loop through ticks fuel slots next
        else
          let r := loop through ticks fuel (slots - 1) next
          { r with output := c :: r.output }
    | none =>
      if fuel == 0 then done m .yielded
      else match m.rest with
      | [] =>
        let stop := if m.state.fullRange && !m.state.eligibleSeen then Stop.impossible else Stop.exhausted
        { machine := { m with state := { m.state with terminal := stop } }, probes := 1, stop }
      | c :: cs =>
        let next := { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs }
        let r := loop through ticks (fuel - 1) slots next
        { r with probes := r.probes + 1 }

-- Finite raw streams make this structural recursion bound independent of the
-- caller's budgets. One extra iteration observes the final stop.
def run (through fuel slots : Nat) (m : Machine) : Result :=
  loop through (2 * m.rest.length + m.state.pending.toList.length + 1) fuel slots m

-- Conservation is about the executable loop, not an assumed equality with an
-- oracle: every unconsumed eligible candidate is either emitted or retained.
theorem loop_conservation (through ticks fuel slots : Nat) (m : Machine) :
    (loop through ticks fuel slots m).output ++
      eligible (remaining (loop through ticks fuel slots m).machine) =
      eligible (remaining m) := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done]
  | succ ticks ih =>
    simp only [loop]
    split
    · simp [done]
    · split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · split
            · rename_i he
              simpa [remaining, eligible, hp, he] using
                ih fuel slots { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
            · rename_i he
              have h := ih fuel (slots - 1) { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
              simpa [remaining, eligible, hp, he] using congrArg (List.cons c) h
        · rename_i hp
          split
          · simp [done]
          · split
            · rename_i hr
              simp [remaining, eligible, hp, hr]
            · rename_i c cs hr
              simpa [remaining, eligible, hp, hr] using
                ih (fuel - 1) slots
                  { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs }

theorem run_conservation (through fuel slots : Nat) (m : Machine) :
    (run through fuel slots m).output ++ eligible (remaining (run through fuel slots m).machine) =
      eligible (remaining m) := loop_conservation through _ fuel slots m

theorem loop_probe_bound (through ticks fuel slots : Nat) (m : Machine) :
    (loop through ticks fuel slots m).probes ≤ fuel := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done]
  | succ ticks ih =>
    simp only [loop]
    split
    · simp [done]
    · split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · split
            · exact ih _ _ _
            · exact ih _ _ _
        · split
          · simp [done]
          · rename_i hf
            split
            · simp_all only [beq_iff_eq]
              omega
            · rename_i c cs hr
              have h := ih (fuel - 1) slots
                { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs }
              dsimp only at h ⊢
              have hfuel : fuel ≠ 0 := by simpa using hf
              simpa only [Nat.sub_add_cancel (show 1 ≤ fuel by omega)] using Nat.add_le_add_right h 1

theorem run_probe_bound (through fuel slots : Nat) (m : Machine) :
    (run through fuel slots m).probes ≤ fuel := loop_probe_bound through _ fuel slots m

def rank (m : Machine) : Nat := 2 * m.rest.length + m.state.pending.toList.length

theorem loop_no_internal_limit (through ticks fuel slots : Nat) (m : Machine)
    (enough : rank m < ticks) (cache : m.state.terminal ≠ .internalLimit) : (loop through ticks fuel slots m).stop ≠ .internalLimit
    := by
  induction ticks generalizing fuel slots m with
  | zero => omega
  | succ ticks ih =>
    simp only [loop]
    split
    · rename_i ht
      exact cache
    · split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · have smaller : rank { m with state := { m.state with pending := none, disposedThrough := c.nominal } } < ticks := by
              simp [rank, hp] at enough ⊢
              omega
            split
            · exact ih _ _ _ smaller cache
            · exact ih _ _ _ smaller cache
        · rename_i hp
          split
          · simp [done]
          · split
            · simp only
              split <;> decide
            · rename_i c cs hr
              apply ih
              simp [rank, hp, hr] at enough ⊢
              omega
              exact cache

theorem run_no_internal_limit (through fuel slots : Nat) (m : Machine)
    (cache : m.state.terminal ≠ .internalLimit) : (run through fuel slots m).stop ≠ .internalLimit := by
  apply loop_no_internal_limit
  · simp [rank]
  · exact cache

structure Call where
  through : Nat
  probes : Nat
  outputs : Nat
  deriving Repr

def resume : List Call → Machine → Result
  | [], m => done m m.state.terminal
  | call :: calls, m =>
    let first := run call.through call.probes call.outputs m
    let rest := resume calls first.machine
    { rest with output := first.output ++ rest.output, probes := first.probes + rest.probes }

-- No restrictions on budget/window choices are needed for losslessness. Progress
-- and window completeness are additional theorems, not hidden hypotheses here.
theorem resume_conservation (calls : List Call) (m : Machine) :
    (resume calls m).output ++ eligible (remaining (resume calls m).machine) =
      eligible (remaining m) := by
  induction calls generalizing m with
  | nil => simp [resume, done]
  | cons call calls ih =>
    simp only [resume, List.append_assoc]
    rw [ih]
    exact run_conservation _ _ _ _

theorem resume_unique (calls : List Call) (m : Machine)
    (ordered : (eligible (remaining m)).Pairwise (fun a b => a.nominal < b.nominal)) :
    (resume calls m).output.Pairwise (fun a b => a.nominal < b.nominal) := by
  rw [← resume_conservation calls m] at ordered
  exact (List.pairwise_append.mp ordered).1

theorem resume_complete_when_consumed (calls : List Call) (m : Machine)
    (consumed : eligible (remaining (resume calls m).machine) = []) :
    (resume calls m).output = eligible (remaining m) := by
  have h := resume_conservation calls m
  simpa [consumed] using h

-- A declarative due prefix does not reproduce the loop's decisions about probe
-- or output budgets. Ordered actual times make this exactly all due candidates.
def expected (through : Nat) (cs : List Candidate) : List Candidate :=
  eligible (cs.takeWhile (fun c => c.actual ≤ through))

theorem loop_sufficient (through ticks fuel slots : Nat) (m : Machine)
    (hticks : rank m < ticks)
    (hfuel : m.rest.length < fuel)
    (hslots : (remaining m).length < slots)
    (hcache : m.state.terminal = .active ∨ remaining m = []) :
    (loop through ticks fuel slots m).output = expected through (remaining m) := by
  induction ticks generalizing fuel slots m with
  | zero => omega
  | succ ticks ih =>
    simp only [loop]
    split
    · rename_i ht
      have hr : remaining m = [] := hcache.resolve_left (by simpa using ht)
      simp [done, hr, expected, eligible]
    · rename_i ht
      have active : m.state.terminal = .active := by simpa using ht
      split
      · rename_i hs
        have hz : slots = 0 := by simpa using hs
        omega
      · split
        · rename_i c hp
          split
          · rename_i hw
            simp [done, expected, remaining, hp, show ¬c.actual ≤ through by omega, eligible]
          · rename_i hw
            have due : c.actual ≤ through := by omega
            let next : Machine := { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
            have nt : rank next < ticks := by
              simp [next, rank, hp] at hticks ⊢
              omega
            have ns : (remaining next).length + 1 < slots := by
              simpa [next, remaining, hp] using hslots
            have nc : next.state.terminal = .active ∨ remaining next = [] := Or.inl active
            split
            · rename_i he
              have stepProof := ih fuel slots next nt hfuel (by omega) nc
              simpa [next, expected, remaining, hp, List.takeWhile_cons, due, eligible, he] using stepProof
            · rename_i he
              have stepProof := ih fuel (slots - 1) next nt hfuel (by omega) nc
              simpa [next, expected, remaining, hp, List.takeWhile_cons, due, eligible, he] using congrArg (List.cons c) stepProof
        · rename_i hp
          split
          · rename_i hf
            have hz : fuel = 0 := by simpa using hf
            omega
          · split
            · rename_i hr
              simp [expected, remaining, hp, hr, eligible]
            · rename_i c cs hr
              let next : Machine := { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs }
              have nt : rank next < ticks := by
                simp [next, rank, hp, hr] at hticks ⊢
                omega
              have nf : next.rest.length < fuel - 1 := by
                simp [next, hr] at hfuel ⊢
                omega
              have ns : (remaining next).length < slots := by
                simpa [next, remaining, hp, hr] using hslots
              have stepProof := ih (fuel - 1) slots next nt nf ns (Or.inl active)
              simpa [next, remaining, hp, hr] using stepProof

theorem sufficient_completeness (through : Nat) (m : Machine)
    (hcache : m.state.terminal = .active ∨ remaining m = []) :
    (run through (m.rest.length + 1) ((remaining m).length + 1) m).output =
      expected through (remaining m) := by
  apply loop_sufficient
  · simp [rank]
  · omega
  · omega
  · exact hcache

theorem loop_rank_le (through ticks fuel slots : Nat) (m : Machine) :
    rank (loop through ticks fuel slots m).machine ≤ rank m := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done]
  | succ ticks ih =>
    simp only [loop]
    split
    · simp [done]
    · split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · have smaller : rank { m with state := { m.state with pending := none, disposedThrough := c.nominal } } ≤ rank m := by
              simp [rank, hp]
            split
            · exact Nat.le_trans (ih _ _ _) smaller
            · exact Nat.le_trans (ih _ _ _) smaller
        · rename_i hp
          split
          · simp [done]
          · split
            · simp [rank]
            · rename_i c cs hr
              apply Nat.le_trans (ih _ _ _)
              simp [rank, hp, hr]
              omega

theorem loop_cache_preserved (through ticks fuel slots : Nat) (m : Machine)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (loop through ticks fuel slots m).machine.state.terminal = .active ∨
      remaining (loop through ticks fuel slots m).machine = [] := by
  induction ticks generalizing fuel slots m with
  | zero => simpa [loop, done] using cache
  | succ ticks ih =>
    simp only [loop]
    split
    · simpa [done] using cache
    · rename_i ht
      have active : m.state.terminal = .active := by simpa using ht
      split
      · simpa [done] using cache
      · split
        · rename_i c hp
          split
          · simpa [done] using cache
          · split
            · exact ih _ _ _ (Or.inl active)
            · exact ih _ _ _ (Or.inl active)
        · rename_i hp
          split
          · simpa [done] using cache
          · split
            · rename_i hr
              right
              simp [remaining, hp, hr]
            · exact ih _ _ _ (Or.inl active)

theorem loop_window_conservation (through ticks fuel slots : Nat) (m : Machine)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (loop through ticks fuel slots m).output ++
      expected through (remaining (loop through ticks fuel slots m).machine) =
      expected through (remaining m) := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done]
  | succ ticks ih =>
    simp only [loop]
    split
    · simp [done]
    · rename_i ht
      have active : m.state.terminal = .active := by simpa using ht
      split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · rename_i hw
            have due : c.actual ≤ through := by omega
            split
            · rename_i he
              simpa [remaining, expected, eligible, hp, he, due] using
                ih fuel slots { m with state := { m.state with pending := none, disposedThrough := c.nominal } } (Or.inl active)
            · rename_i he
              have h := ih fuel (slots - 1) { m with state := { m.state with pending := none, disposedThrough := c.nominal } } (Or.inl active)
              simpa [remaining, expected, eligible, hp, he, due] using congrArg (List.cons c) h
        · rename_i hp
          split
          · simp [done]
          · split
            · rename_i hr
              simp [remaining, expected, eligible, hp, hr]
            · rename_i c cs hr
              simpa [remaining, hp, hr] using
                ih (fuel - 1) slots
                  { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs } (Or.inl active)

theorem run_progress (through fuel slots : Nat) (m : Machine)
    (positiveFuel : 0 < fuel) (positiveSlots : 0 < slots)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    rank (run through fuel slots m).machine < rank m ∨
      expected through (remaining (run through fuel slots m).machine) = [] := by
  unfold run
  simp only [loop]
  split
  · rename_i ht
    right
    have hr : remaining m = [] := cache.resolve_left ht
    simp [done, hr, expected, eligible]
  · split
    · rename_i hs
      have hz : slots = 0 := by simpa using hs
      omega
    · split
      · rename_i c hp
        split
        · rename_i hw
          right
          simp [done, remaining, expected, eligible, hp, show ¬c.actual ≤ through by omega]
        · left
          have smaller : rank { m with state := { m.state with pending := none, disposedThrough := c.nominal } } < rank m := by
            simp [rank, hp]
          split
          · exact Nat.lt_of_le_of_lt (loop_rank_le _ _ _ _ _) smaller
          · exact Nat.lt_of_le_of_lt (loop_rank_le _ _ _ _ _) smaller
      · rename_i hp
        split
        · rename_i hf
          have hz : fuel = 0 := by simpa using hf
          omega
        · split
          · rename_i hr
            right
            simp [remaining, hp, hr, expected, eligible]
          · rename_i c cs hr
            left
            apply Nat.lt_of_le_of_lt (loop_rank_le _ _ _ _ _)
            simp [rank, hp, hr]
            omega

theorem resume_cache_preserved (calls : List Call) (m : Machine)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (resume calls m).machine.state.terminal = .active ∨ remaining (resume calls m).machine = [] := by
  induction calls generalizing m with
  | nil => simpa [resume, done] using cache
  | cons call calls ih =>
    exact ih _ (loop_cache_preserved _ _ _ _ _ cache)

theorem resume_window_conservation (through : Nat) (calls : List Call) (m : Machine)
    (windows : ∀ c ∈ calls, c.through = through)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (resume calls m).output ++ expected through (remaining (resume calls m).machine) =
      expected through (remaining m) := by
  induction calls generalizing m with
  | nil => simp [resume, done]
  | cons call calls ih =>
    have hc := windows call (by simp)
    have tail : ∀ c ∈ calls, c.through = through := by
      intro c hm
      exact windows c (by simp [hm])
    simp only [resume, List.append_assoc]
    have nextCache : (run call.through call.probes call.outputs m).machine.state.terminal = .active ∨
        remaining (run call.through call.probes call.outputs m).machine = [] := loop_cache_preserved _ _ _ _ _ cache
    rw [ih (run call.through call.probes call.outputs m).machine tail nextCache]
    rw [← hc]
    exact loop_window_conservation _ _ _ _ _ cache

theorem resume_positive_settles (through : Nat) (calls : List Call) (m : Machine)
    (valid : ∀ c ∈ calls, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through)
    (enough : rank m < calls.length)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    expected through (remaining (resume calls m).machine) = [] := by
  induction calls generalizing m with
  | nil => simp at enough
  | cons call calls ih =>
    have head := valid call (by simp)
    have tail : ∀ c ∈ calls, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through := by
      intro c hm
      exact valid c (by simp [hm])
    have windows : ∀ c ∈ calls, c.through = through := by
      intro c hm
      exact (tail c hm).2.2
    let first := run call.through call.probes call.outputs m
    have nextCache : first.machine.state.terminal = .active ∨ remaining first.machine = [] :=
      loop_cache_preserved _ _ _ _ _ cache
    have progress := run_progress call.through call.probes call.outputs m head.1 head.2.1 cache
    change rank first.machine < rank m ∨ expected call.through (remaining first.machine) = [] at progress
    rcases progress with decrease | settled
    · have bound : rank first.machine < calls.length := by
        simp only [List.length_cons] at enough
        omega
      exact ih first.machine tail bound nextCache
    · rw [head.2.2] at settled
      have conservation := resume_window_conservation through calls first.machine windows nextCache
      rw [settled] at conservation
      exact (List.append_eq_nil_iff.mp conservation).2

-- Arbitrary positive probe/output budgets, including repeated output-limited
-- resumptions, terminate the finite due prefix within this conservative bound.
-- No desired-output equation or final-consumed assumption is a hypothesis.
theorem resumable_completeness (through : Nat) (calls : List Call) (m : Machine)
    (valid : ∀ c ∈ calls, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through)
    (enough : rank m < calls.length)
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (resume calls m).output = expected through (remaining m) := by
  have windows : ∀ c ∈ calls, c.through = through := by
    intro c hm
    exact (valid c hm).2.2
  have conservation := resume_window_conservation through calls m windows cache
  rw [resume_positive_settles through calls m valid enough cache] at conservation
  simpa using conservation

theorem loop_suffix (through ticks fuel slots : Nat) (m : Machine) :
    ∃ disposed, remaining m = disposed ++ remaining (loop through ticks fuel slots m).machine := by
  induction ticks generalizing fuel slots m with
  | zero => exact ⟨[], by simp [loop, done]⟩
  | succ ticks ih =>
    simp only [loop]
    split
    · exact ⟨[], by simp [done]⟩
    · split
      · exact ⟨[], by simp [done]⟩
      · split
        · rename_i c hp
          split
          · exact ⟨[], by simp [done]⟩
          · split
            · obtain ⟨disposed, hd⟩ := ih fuel slots { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
              exact ⟨c :: disposed, by simpa [remaining, hp] using congrArg (List.cons c) hd⟩
            · obtain ⟨disposed, hd⟩ := ih fuel (slots - 1) { m with state := { m.state with pending := none, disposedThrough := c.nominal } }
              exact ⟨c :: disposed, by simpa [remaining, hp] using congrArg (List.cons c) hd⟩
        · rename_i hp
          split
          · exact ⟨[], by simp [done]⟩
          · split
            · exact ⟨[], by simp [remaining]⟩
            · rename_i c cs hr
              obtain ⟨disposed, hd⟩ := ih (fuel - 1) slots
                { state := { m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded }, rest := cs }
              exact ⟨disposed, by simpa [remaining, hp, hr] using hd⟩

theorem resume_suffix (calls : List Call) (m : Machine) :
    ∃ disposed, remaining m = disposed ++ remaining (resume calls m).machine := by
  induction calls generalizing m with
  | nil => exact ⟨[], by simp [resume, done]⟩
  | cons call calls ih =>
    obtain ⟨first, hf⟩ := loop_suffix call.through (rank m + 1) call.probes call.outputs m
    obtain ⟨rest, hr⟩ := ih (run call.through call.probes call.outputs m).machine
    refine ⟨first ++ rest, ?_⟩
    change remaining m = (first ++ rest) ++ remaining (resume calls (run call.through call.probes call.outputs m).machine).machine
    change remaining m = first ++ remaining (run call.through call.probes call.outputs m).machine at hf
    rw [hf, hr, List.append_assoc]

theorem resume_ordered (calls : List Call) (m : Machine) (R : Candidate → Candidate → Prop)
    (ordered : (remaining m).Pairwise R) : (remaining (resume calls m).machine).Pairwise R := by
  obtain ⟨disposed, hd⟩ := resume_suffix calls m
  rw [hd] at ordered
  exact (List.pairwise_append.mp ordered).2.1

theorem ordered_takeWhile (through : Nat) (cs : List Candidate)
    (ordered : cs.Pairwise (fun a b => a.actual ≤ b.actual)) :
    cs.takeWhile (fun c => c.actual ≤ through) = cs.filter (fun c => c.actual ≤ through) := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
    have parts := List.pairwise_cons.mp ordered
    by_cases due : c.actual ≤ through
    · simpa [due] using congrArg (List.cons c) (ih parts.2)
    · have future : cs.filter (fun c => c.actual ≤ through) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro b hb
        have monotone := parts.1 b hb
        simpa using (show ¬b.actual ≤ through by omega)
      simp [due, future]

theorem expected_filter (through : Nat) (cs : List Candidate)
    (ordered : cs.Pairwise (fun a b => a.actual ≤ b.actual)) :
    expected through cs = (eligible cs).filter (fun c => c.actual ≤ through) := by
  unfold expected
  rw [ordered_takeWhile through cs ordered]
  simp only [eligible, List.filter_filter]
  congr 1
  funext c
  exact Bool.and_comm _ _

theorem loop_output_due (through ticks fuel slots : Nat) (m : Machine) :
    ∀ c ∈ (loop through ticks fuel slots m).output, c.actual ≤ through := by
  induction ticks generalizing fuel slots m with
  | zero => simp [loop, done]
  | succ ticks ih =>
    simp only [loop]
    split
    · simp [done]
    · split
      · simp [done]
      · split
        · rename_i c hp
          split
          · simp [done]
          · rename_i hw
            split
            · exact ih _ _ _
            · intro b hb
              simp only [List.mem_cons] at hb
              rcases hb with rfl | hb
              · omega
              · exact ih _ _ _ b hb
        · split
          · simp [done]
          · split
            · simp
            · exact ih _ _ _

theorem resume_output_due (through : Nat) (calls : List Call) (m : Machine)
    (windows : ∀ c ∈ calls, c.through ≤ through) :
    ∀ c ∈ (resume calls m).output, c.actual ≤ through := by
  induction calls generalizing m with
  | nil => simp [resume, done]
  | cons call calls ih =>
    intro c hc
    simp only [resume, List.mem_append] at hc
    rcases hc with first | rest
    · exact Nat.le_trans (loop_output_due _ _ _ _ _ c first) (windows call (by simp))
    · apply ih _ (fun call hc => windows call (by simp [hc])) c rest

-- Any finite sequence of windows below the final endpoint has the same emitted
-- prefix plus retained final-window work as the unsplit declarative stream.
theorem finite_window_conservation (through : Nat) (calls : List Call) (m : Machine)
    (windows : ∀ c ∈ calls, c.through ≤ through)
    (ordered : (remaining m).Pairwise (fun a b => a.actual ≤ b.actual)) :
    (resume calls m).output ++ expected through (remaining (resume calls m).machine) =
      expected through (remaining m) := by
  have conserved := congrArg (List.filter (fun c : Candidate => c.actual ≤ through)) (resume_conservation calls m)
  have outputFilter : (resume calls m).output.filter (fun c => c.actual ≤ through) = (resume calls m).output := by
    apply List.filter_eq_self.mpr
    intro c hc
    simpa using resume_output_due through calls m windows c hc
  rw [List.filter_append, outputFilter] at conserved
  rw [expected_filter through _ ordered,
      expected_filter through _ (resume_ordered calls m _ ordered)]
  exact conserved

-- SP3: arbitrary finite nondecreasing partitions are included by this stronger
-- theorem: every earlier window may be any endpoint ≤ the final one. A finite
-- suffix of arbitrary positive budgets at the final window completes the answer.
theorem finite_window_batching (through : Nat) (earlier suffix : List Call) (m : Machine)
    (windows : ∀ c ∈ earlier, c.through ≤ through)
    (positive : ∀ c ∈ suffix, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through)
    (enough : rank (resume earlier m).machine < suffix.length)
    (ordered : (remaining m).Pairwise (fun a b => a.actual ≤ b.actual))
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (resume earlier m).output ++ (resume suffix (resume earlier m).machine).output =
      expected through (remaining m) := by
  rw [resumable_completeness through suffix (resume earlier m).machine positive enough
      (resume_cache_preserved earlier m cache)]
  exact finite_window_conservation through earlier m windows ordered

theorem adjacent_window_batching (middle through probes slots : Nat) (suffix : List Call)
    (m : Machine) (increasing : middle ≤ through)
    (positive : ∀ c ∈ suffix, 0 < c.probes ∧ 0 < c.outputs ∧ c.through = through)
    (enough : rank (run middle probes slots m).machine < suffix.length)
    (ordered : (remaining m).Pairwise (fun a b => a.actual ≤ b.actual))
    (cache : m.state.terminal = .active ∨ remaining m = []) :
    (run middle probes slots m).output ++ (resume suffix (run middle probes slots m).machine).output =
      (run through (m.rest.length + 1) ((remaining m).length + 1) m).output := by
  rw [sufficient_completeness through m cache]
  have h := finite_window_batching through [{ through := middle, probes, outputs := slots }]
    suffix m (by simpa using increasing) positive enough ordered cache
  simpa [resume, done] using h

end Scheduler2.Exact
