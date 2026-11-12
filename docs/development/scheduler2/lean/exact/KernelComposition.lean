import Kernel

namespace Scheduler2.Exact

-- Structural ticks are an implementation termination measure, not a resource
-- visible to callers. Any bound above the remaining rank gives the same result.
theorem loop_ticks_irrelevant (through a b fuel slots : Nat) (m : Machine)
    (ha : rank m < a) (hb : rank m < b) :
    loop through a fuel slots m = loop through b fuel slots m := by
  induction a generalizing b fuel slots m with
  | zero => omega
  | succ a ih =>
    cases b with
    | zero => omega
    | succ b =>
      simp only [loop]
      split
      · rfl
      · split
        · rfl
        · split
          · rename_i c hp
            split
            · rfl
            · have ha' : rank {m with state := {m.state with pending := none, disposedThrough := c.nominal}} < a := by
                simp [rank, hp] at ha ⊢; omega
              have hb' : rank {m with state := {m.state with pending := none, disposedThrough := c.nominal}} < b := by
                simp [rank, hp] at hb ⊢; omega
              split
              · exact ih _ _ _ _ ha' hb'
              · rw [ih _ _ _ _ ha' hb']
          · rename_i hp
            split
            · rfl
            · split
              · rfl
              · rename_i c cs hr
                have ha' : rank {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs} < a := by
                  simp [rank, hp, hr] at ha ⊢; omega
                have hb' : rank {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs} < b := by
                  simp [rank, hp, hr] at hb ⊢; omega
                rw [ih _ _ _ _ ha' hb']

theorem loop_eq_run (through ticks fuel slots : Nat) (m : Machine)
    (h : rank m < ticks) : loop through ticks fuel slots m = run through fuel slots m :=
  loop_ticks_irrelevant through ticks _ fuel slots m h (by simp [rank])

-- A single semantic step, with the structurally bounded recursive call folded
-- back into run. All public state fields and accounting remain in Result.
theorem run_unfold (through fuel slots : Nat) (m : Machine) :
    run through fuel slots m =
    if m.state.terminal ≠ .active then done m m.state.terminal
    else if slots == 0 then done m .limited
    else match m.state.pending with
    | some c =>
      if through < c.actual then done m .waiting
      else
        let next := {m with state := {m.state with pending := none, disposedThrough := c.nominal}}
        if c.excluded then run through fuel slots next
        else let r := run through fuel (slots - 1) next; {r with output := c :: r.output}
    | none =>
      if fuel == 0 then done m .yielded
      else match m.rest with
      | [] =>
        let stop := if m.state.fullRange && !m.state.eligibleSeen then Stop.impossible else Stop.exhausted
        {machine := {m with state := {m.state with terminal := stop}}, probes := 1, stop}
      | c :: cs =>
        let next := {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs}
        let r := run through (fuel - 1) slots next
        {r with probes := r.probes + 1} := by
  conv => lhs; unfold run
  rw [loop]
  split
  · rfl
  · split
    · rfl
    · split
      · rename_i c hp
        simp only [hp]
        split
        · rfl
        · have h : rank {m with state := {m.state with pending := none, disposedThrough := c.nominal}} < 2 * m.rest.length + m.state.pending.toList.length := by simp [rank, hp]
          simp only [hp] at h
          split <;> rw [loop_eq_run _ _ _ _ _ h]
      · rename_i hp
        simp only [hp]
        split
        · rfl
        · split
          · rename_i hr
            simp only [hr]
          · rename_i c cs hr
            simp only [hr]
            rw [loop_eq_run]
            simp [rank]; omega

def joinResult (first second : Result) : Result :=
  {second with output := first.output ++ second.output, probes := first.probes + second.probes}

-- Exact probe-budget resumption: includes every state field, private suffix,
-- output, total probes, and final stop. The first call must actually yield.
theorem run_probe_resume (through firstFuel secondFuel slots : Nat) (m : Machine)
    (cache : m.state.terminal ≠ .yielded)
    (yielded : (run through firstFuel slots m).stop = .yielded) :
    joinResult (run through firstFuel slots m)
      (run through secondFuel (slots - (run through firstFuel slots m).output.length)
        (run through firstFuel slots m).machine) =
      run through (firstFuel + secondFuel) slots m := by
  generalize hn : rank m = n
  induction n using Nat.strongRecOn generalizing m firstFuel secondFuel slots with
  | ind n ih =>
    rw [run_unfold through firstFuel slots m] at yielded ⊢
    rw [run_unfold through (firstFuel + secondFuel) slots m]
    by_cases ht : m.state.terminal ≠ .active
    · simp [ht, done] at yielded
      exact False.elim (cache yielded)
    simp only [ht, ↓reduceIte] at yielded ⊢
    by_cases hs : slots == 0
    · simp [hs, done] at yielded
    simp only [hs] at yielded ⊢
    cases hp : m.state.pending with
    | some c =>
      simp only [hp] at yielded ⊢
      by_cases hw : through < c.actual
      · simp [hw, done] at yielded
      simp only [hw, ↓reduceIte] at yielded ⊢
      let next : Machine := {m with state := {m.state with pending := none, disposedThrough := c.nominal}}
      have smaller : rank next < n := by simp [rank, next, hp] at hn ⊢; omega
      cases he : c.excluded with
      | true =>
        simp only [he, ↓reduceIte] at yielded ⊢
        exact ih (rank next) smaller firstFuel secondFuel slots next cache yielded rfl
      | false =>
        simp only [he, Bool.false_eq_true, ↓reduceIte] at yielded ⊢
        have eq := ih (rank next) smaller firstFuel secondFuel (slots - 1) next cache yielded rfl
        simpa only [joinResult, next, Nat.sub_sub, Nat.add_comm, List.cons_append, List.length_cons] using
          congrArg (fun r : Result => {r with output := c :: r.output}) eq
    | none =>
      simp only [hp] at yielded ⊢
      by_cases hf : firstFuel == 0
      · have hz : firstFuel = 0 := by simpa using hf
        subst firstFuel
        simpa [ht, hs, hp, done, joinResult] using (run_unfold through secondFuel slots m)

      have pos : 0 < firstFuel := by simp only [beq_iff_eq] at hf; omega
      have total : (firstFuel + secondFuel == 0) = false := by simp; omega
      simp only [hf, total, Bool.false_eq_true, ↓reduceIte] at yielded ⊢
      cases hr : m.rest with
      | nil =>
        simp only [hr] at yielded
        split at yielded <;> cases yielded
      | cons c cs =>
        simp only [hr] at yielded ⊢
        let next : Machine := {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs}
        have smaller : rank next < n := by simp [rank, next, hp, hr] at hn ⊢; omega
        have eq := ih (rank next) smaller (firstFuel - 1) secondFuel slots next cache yielded rfl
        have fuel : firstFuel + secondFuel - 1 = firstFuel - 1 + secondFuel := by omega
        simp only [fuel]
        have lifted := congrArg (fun r : Result => {r with probes := r.probes + 1}) eq
        dsimp only [joinResult] at lifted ⊢
        simpa only [next, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using lifted

-- Resource-free normal form used only for proving sufficient-budget calls.
-- It uses the actual state transitions, including probe lookahead and caches.
def complete (through : Nat) (m : Machine) : Result :=
  if m.state.terminal ≠ .active then done m m.state.terminal
  else match hp : m.state.pending with
  | some c =>
    if through < c.actual then done m .waiting
    else
      let next := {m with state := {m.state with pending := none, disposedThrough := c.nominal}}
      let r := complete through next
      if c.excluded then r else {r with output := c :: r.output}
  | none => match hr : m.rest with
    | [] =>
      let stop := if m.state.fullRange && !m.state.eligibleSeen then Stop.impossible else Stop.exhausted
      {machine := {m with state := {m.state with terminal := stop}}, probes := 1, stop}
    | c :: cs =>
      let next := {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs}
      let r := complete through next
      {r with probes := r.probes + 1}
termination_by rank m
decreasing_by
  · simp [rank, hp]
  · simp [rank, hp, hr]; omega

-- Budgets count raw probes (including the terminal probe) and eligible output.
-- This bound matches the originally proposed raw-length-plus-one call.
theorem run_sufficient_exact (through fuel slots : Nat) (m : Machine)
    (hf : m.rest.length < fuel) (hs : (remaining m).length < slots) :
    run through fuel slots m = complete through m := by
  generalize hn : rank m = n
  induction n using Nat.strongRecOn generalizing m fuel slots with
  | ind n ih =>
    rw [run_unfold, complete]
    by_cases ht : m.state.terminal ≠ .active
    · simp [ht]
    simp only [ht, ↓reduceIte]
    have slot : (slots == 0) = false := by simp; omega
    simp only [slot, Bool.false_eq_true, ↓reduceIte]
    cases hp : m.state.pending with
    | some c =>
      simp only [hp]
      split
      · rfl
      · let next : Machine := {m with state := {m.state with pending := none, disposedThrough := c.nominal}}
        have smaller : rank next < n := by simp [rank, next, hp] at hn ⊢; omega
        have ss : (remaining next).length < slots := by simp [remaining, next, hp] at hs ⊢; omega
        have ss' : (remaining next).length < slots - 1 := by simp [remaining, next, hp] at hs ⊢; omega
        split
        · exact ih _ smaller fuel slots next hf ss rfl
        · rw [ih _ smaller fuel (slots-1) next hf ss' rfl]
    | none =>
      simp only [hp]
      have fuelNZ : (fuel == 0) = false := by simp; omega
      simp only [fuelNZ, Bool.false_eq_true, ↓reduceIte]
      cases hr : m.rest with
      | nil => simp only [hr]
      | cons c cs =>
        simp only [hr]
        let next : Machine := {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs}
        have smaller : rank next < n := by simp [rank, next, hp, hr] at hn ⊢; omega
        have ff : next.rest.length < fuel - 1 := by simp [next, hr] at hf ⊢; omega
        have ss : (remaining next).length < slots := by simpa [remaining, next, hp, hr] using hs
        rw [ih _ smaller (fuel-1) slots next ff ss rfl]

theorem complete_batching (middle through : Nat) (m : Machine) (inc : middle ≤ through) :
    joinResult (complete middle m) (complete through (complete middle m).machine) =
      complete through m := by
  generalize hn : rank m = n
  induction n using Nat.strongRecOn generalizing m with
  | ind n ih =>
    rw [complete.eq_1 middle m, complete.eq_1 through m]
    by_cases ht : m.state.terminal ≠ .active
    · simp only [ht, ↓reduceIte, done, joinResult]
      rw [complete]
      simp [ht, done]
    simp only [ht, ↓reduceIte]
    cases hp : m.state.pending with
    | some c =>
      simp only [hp]
      by_cases hw : middle < c.actual
      · simp only [hw, ↓reduceIte, done, joinResult]
        have unfold := complete.eq_1 through m
        rw [hp] at unfold
        simpa only [ht, ↓reduceIte, List.nil_append, Nat.zero_add] using unfold
      have due : ¬through < c.actual := by omega
      simp only [hw, due, ↓reduceIte]
      let next : Machine := {m with state := {m.state with pending := none, disposedThrough := c.nominal}}
      have smaller : rank next < n := by simp [rank, next, hp] at hn ⊢; omega
      have eq := ih _ smaller next rfl
      cases he : c.excluded with
      | true => simpa only [he, ↓reduceIte] using eq
      | false =>
        simp only [he, Bool.false_eq_true, ↓reduceIte]
        simpa only [joinResult, next, List.cons_append] using
          congrArg (fun r : Result => {r with output := c :: r.output}) eq
    | none =>
      simp only [hp]
      cases hr : m.rest with
      | nil =>
        simp only [hr]
        split <;> simp only [joinResult] <;> rw [complete] <;> simp [done]
      | cons c cs =>
        simp only [hr]
        let next : Machine := {state := {m.state with searchAfter := c.nominal, pending := some c, eligibleSeen := m.state.eligibleSeen || !c.excluded}, rest := cs}
        have smaller : rank next < n := by simp [rank, next, hp, hr] at hn ⊢; omega
        have eq := ih _ smaller next rfl
        have lifted := congrArg (fun r : Result => {r with probes := r.probes + 1}) eq
        dsimp only [joinResult] at lifted ⊢
        simpa only [next, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using lifted

-- General sufficient budgets, with no hypothesis that assumes the desired
-- state equality. In particular fullRange/eligibleSeen/terminal/pending survive.
theorem run_window_batching_exact (middle through f₁ f₂ fw s₁ s₂ sw : Nat) (m : Machine)
    (inc : middle ≤ through)
    (hf₁ : m.rest.length < f₁) (hs₁ : (remaining m).length < s₁)
    (hf₂ : (run middle f₁ s₁ m).machine.rest.length < f₂)
    (hs₂ : (remaining (run middle f₁ s₁ m).machine).length < s₂)
    (hfw : m.rest.length < fw) (hsw : (remaining m).length < sw) :
    joinResult (run middle f₁ s₁ m) (run through f₂ s₂ (run middle f₁ s₁ m).machine) =
      run through fw sw m := by
  rw [run_sufficient_exact through f₂ s₂ _ hf₂ hs₂,
      run_sufficient_exact middle f₁ s₁ m hf₁ hs₁,
      run_sufficient_exact through fw sw m hfw hsw]
  exact complete_batching middle through m inc

-- The executable API has no disposition journal. The exact ghost journal is
-- the raw consumed prefix, including excluded candidates, recovered uniquely
-- from the initial stream and final suffix. It is not just eligible output.
def disposedTrace (before after : Machine) : List Candidate :=
  (remaining before).take ((remaining before).length - (remaining after).length)

theorem disposedTrace_spec (before after : Machine)
    (suffix : ∃ ds, remaining before = ds ++ remaining after) :
    remaining before = disposedTrace before after ++ remaining after := by
  obtain ⟨ds, hd⟩ := suffix
  simp [disposedTrace, hd]

theorem run_disposedTrace_spec (through fuel slots : Nat) (m : Machine) :
    remaining m = disposedTrace m (run through fuel slots m).machine ++
      remaining (run through fuel slots m).machine :=
  disposedTrace_spec _ _ (loop_suffix through _ fuel slots m)

theorem disposedTrace_compose (a b c : Machine)
    (ab : ∃ ds, remaining a = ds ++ remaining b)
    (bc : ∃ ds, remaining b = ds ++ remaining c) :
    disposedTrace a b ++ disposedTrace b c = disposedTrace a c := by
  have ac : ∃ ds, remaining a = ds ++ remaining c := by
    obtain ⟨xs, hx⟩ := ab
    obtain ⟨ys, hy⟩ := bc
    exact ⟨xs ++ ys, by rw [hx, hy, List.append_assoc]⟩
  apply List.append_cancel_right (bs := remaining c)
  rw [List.append_assoc, ← disposedTrace_spec b c bc, ← disposedTrace_spec a b ab,
    ← disposedTrace_spec a c ac]

-- Original resume obligations, now with the complete consumed raw-prefix
-- journal reconstructed above, rather than inventing a public result field.
theorem run_probe_resume_dispositions (through f₁ f₂ slots : Nat) (m : Machine)
    (cache : m.state.terminal ≠ .yielded)
    (yielded : (run through f₁ slots m).stop = .yielded) :
    let first := run through f₁ slots m
    let second := run through f₂ (slots - first.output.length) first.machine
    disposedTrace m first.machine ++ disposedTrace first.machine second.machine =
      disposedTrace m (run through (f₁ + f₂) slots m).machine := by
  dsimp only
  have eq := congrArg Result.machine (run_probe_resume through f₁ f₂ slots m cache yielded)
  change (run through f₂ (slots - (run through f₁ slots m).output.length)
    (run through f₁ slots m).machine).machine = (run through (f₁ + f₂) slots m).machine at eq
  rw [← eq]
  exact disposedTrace_compose _ _ _ (loop_suffix through _ f₁ slots m)
    (loop_suffix through _ f₂ _ _)

theorem run_remaining_length_le (through fuel slots : Nat) (m : Machine) :
    (remaining (run through fuel slots m).machine).length ≤ (remaining m).length := by
  obtain ⟨ds, hd⟩ := loop_suffix through (rank m + 1) fuel slots m
  change remaining m = ds ++ remaining (run through fuel slots m).machine at hd
  rw [hd, List.length_append]
  omega

theorem rest_length_le_remaining (m : Machine) : m.rest.length ≤ (remaining m).length := by
  simp [remaining]

-- A concrete uniform budget, not a completion/equality premise. For initial
-- states (pending = none), this is exactly source suffix length plus one.
theorem run_window_batching_uniform (middle through : Nat) (m : Machine)
    (inc : middle ≤ through) :
    let budget := (remaining m).length + 1
    joinResult (run middle budget budget m)
      (run through budget budget (run middle budget budget m).machine) =
      run through budget budget m := by
  dsimp only
  have firstRest := rest_length_le_remaining m
  have len := run_remaining_length_le middle ((remaining m).length+1) ((remaining m).length+1) m
  have secondRest := rest_length_le_remaining (run middle ((remaining m).length+1) ((remaining m).length+1) m).machine
  apply run_window_batching_exact _ _ _ _ _ _ _ _ m inc <;> omega

-- Every earlier call has a directly checkable resource bound and endpoint;
-- no assumed output or state equality is part of this predicate.
def SufficientWindows (through : Nat) : List Call → Machine → Prop
  | [], _ => True
  | call :: calls, m =>
    call.through ≤ through ∧ m.rest.length < call.probes ∧
      (remaining m).length < call.outputs ∧
      SufficientWindows through calls (run call.through call.probes call.outputs m).machine

theorem joinResult_assoc (a b c : Result) :
    joinResult (joinResult a b) c = joinResult a (joinResult b c) := by
  simp [joinResult, List.append_assoc, Nat.add_assoc]

-- Finite-window full-state equivalence. The final call is at the final window;
-- each earlier endpoint may be any endpoint below it, so nondecreasing finite
-- partitions are a special case. This also preserves summed probe accounting.
theorem finite_window_batching_exact (through : Nat) (calls : List Call) (m : Machine)
    (sufficient : SufficientWindows through calls m) :
    joinResult (resume calls m) (complete through (resume calls m).machine) = complete through m := by
  induction calls generalizing m with
  | nil => simp [resume, done, joinResult]
  | cons call calls ih =>
    obtain ⟨window, fuel, slots, rest⟩ := sufficient
    have eq := ih (run call.through call.probes call.outputs m).machine rest
    change joinResult (joinResult (run call.through call.probes call.outputs m)
      (resume calls (run call.through call.probes call.outputs m).machine))
      (complete through (resume calls (run call.through call.probes call.outputs m).machine).machine) = _
    rw [joinResult_assoc, eq, run_sufficient_exact _ _ _ _ fuel slots]
    exact complete_batching _ _ _ window

theorem finite_run_window_batching_exact (through fuel slots : Nat) (calls : List Call) (m : Machine)
    (sufficient : SufficientWindows through calls m)
    (hf : (resume calls m).machine.rest.length < fuel)
    (hs : (remaining (resume calls m).machine).length < slots) :
    joinResult (resume calls m) (run through fuel slots (resume calls m).machine) =
      run through (m.rest.length+1) ((remaining m).length+1) m := by
  rw [run_sufficient_exact _ _ _ _ hf hs, run_sufficient_exact _ _ _ _ (by omega) (by omega)]
  exact finite_window_batching_exact through calls m sufficient

end Scheduler2.Exact
