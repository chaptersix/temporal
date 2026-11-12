import Public
import KernelComposition
open Scheduler2.Exact
namespace Scheduler2.Wire

-- Unlike the completeness fold's terminal summary, this pair preserves the
-- actual second public call's stop, as required by exact probe resumption.
def publicPair (src : Source) (a b : Call) (s : State) : Except String Result :=
  match scan src s a.through a.probes a.outputs false with
  | .error e => .error e
  | .ok first =>
    match scan src first.machine.state b.through b.probes b.outputs false with
    | .error e => .error e
    | .ok second => .ok (joinResult first second)

theorem public_pair_kernel (src : Source) (a b : Call) (s : State)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (ha : PublicCallValid a) (hb : PublicCallValid b) :
    publicPair src a b s = .ok (joinResult
      (run a.through a.probes a.outputs (restore (bounded src) s))
      (run b.through b.probes b.outputs (run a.through a.probes a.outputs (restore (bounded src) s)).machine)) := by
  have first := ready_scan_success src s a.through a.probes a.outputs source ready inclusions ha.1 ha.2
  have next := ready_run src s a.through a.probes a.outputs source ready
  have second := ready_scan_success src _ b.through b.probes b.outputs source next inclusions hb.1 hb.2
  have frontier := restore_frontier (bounded src) s (source.2.2.2.2.2.2.1.filter _) ready.1
  have reconstruct := run_reconstructible (bounded src) a.through a.probes a.outputs
    (restore (bounded src) s) frontier rfl
  simp only [publicPair, first, second, reconstruct]

theorem public_probe_resume_exact (src : Source) (s : State) (through f₁ f₂ slots : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (firstValid : PublicCallValid {through, probes := f₁, outputs := slots})
    (secondValid : PublicCallValid {through, probes := f₂, outputs := slots - (run through f₁ slots (restore (bounded src) s)).output.length})
    (wholeValid : PublicCallValid {through, probes := f₁ + f₂, outputs := slots})
    (yielded : (run through f₁ slots (restore (bounded src) s)).stop = .yielded) :
    publicPair src {through, probes := f₁, outputs := slots}
      {through, probes := f₂, outputs := slots - (run through f₁ slots (restore (bounded src) s)).output.length} s =
      scan src s through (f₁ + f₂) slots false := by
  have cache : s.terminal ≠ .yielded := by
    have hv := ready.1
    intro h
    simp [validState, h] at hv
  rw [public_pair_kernel _ _ _ _ source ready inclusions firstValid secondValid,
    ready_scan_success _ _ _ _ _ source ready inclusions wholeValid.1 wholeValid.2]
  exact congrArg Except.ok (run_probe_resume through f₁ f₂ slots _ cache yielded)

theorem public_window_batching_exact (src : Source) (s : State) (middle through budget : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (firstValid : PublicCallValid {through := middle, probes := budget, outputs := budget})
    (secondValid : PublicCallValid {through, probes := budget, outputs := budget})
    (inc : middle ≤ through)
    (sufficient : budget = (remaining (restore (bounded src) s)).length + 1) :
    publicPair src {through := middle, probes := budget, outputs := budget}
      {through, probes := budget, outputs := budget} s = scan src s through budget budget false := by
  rw [public_pair_kernel _ _ _ _ source ready inclusions firstValid secondValid,
    ready_scan_success _ _ _ _ _ source ready inclusions secondValid.1 secondValid.2]
  rw [sufficient]
  exact congrArg Except.ok (run_window_batching_uniform middle through _ inc)

theorem public_pending_stable (src : Source) (s : State) (c : Candidate) (call : Call)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (valid : PublicCallValid call) (pending : s.pending = some c) (future : call.through < c.actual) :
    scan src s call.through call.probes call.outputs false =
      .ok (done (restore (bounded src) s) .waiting) := by
  have active : s.terminal = .active := by
    have hv := ready.1
    simp [validState, pending] at hv
    exact hv.2.1.1.1.1.1.1.1.1.1
  rw [ready_scan_success _ _ _ _ _ source ready inclusions valid.1 valid.2,
    pending_stable _ _ _ _ c active pending future valid.2.2.2.1]

theorem public_next_first (src : Source) (s : State) (through probes : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (valid : PublicCallValid {through, probes, outputs := 1}) :
    ∃ result, scan src s through probes 1 false = .ok result ∧
      (result.output ≠ [] → result.output = (expected through (remaining (restore (bounded src) s))).take 1) := by
  let m := restore (bounded src) s
  let result := run through probes 1 m
  refine ⟨result, ready_scan_success _ _ _ _ _ source ready inclusions valid.1 valid.2, ?_⟩
  intro nonempty
  cases output : result.output with
  | nil => exact False.elim (nonempty output)
  | cons c cs =>
    have frontier := restore_frontier (bounded src) s (source.2.2.2.2.2.2.1.filter _) ready.1
    have shape := loop_one_output through _ probes m c cs frontier output
    have conservation := loop_window_conservation through (2 * m.rest.length + m.state.pending.toList.length + 1) probes 1 m ready.2.2.2.2.2
    change result.output ++ expected through (remaining result.machine) = expected through (remaining m) at conservation
    rw [← conservation, output, shape.1]
    simp

theorem ready_resume_reconstructible (src : Source) (calls : List Call) (s : State)
    (source : SourceValid src) (ready : Ready src s) :
    Ready src (resume calls (restore (bounded src) s)).machine.state ∧
      restore (bounded src) (resume calls (restore (bounded src) s)).machine.state =
        (resume calls (restore (bounded src) s)).machine := by
  induction calls generalizing s with
  | nil => exact ⟨ready, rfl⟩
  | cons c calls ih =>
    have next := ready_run src s c.through c.probes c.outputs source ready
    have frontier := restore_frontier (bounded src) s (source.2.2.2.2.2.2.1.filter _) ready.1
    have reconstruct := run_reconstructible (bounded src) c.through c.probes c.outputs
      (restore (bounded src) s) frontier rfl
    have tail := ih _ next
    rw [reconstruct] at tail
    exact tail

def publicFinish (src : Source) (calls : List Call) (last : Call) (s : State) : Except String Result :=
  match publicResume src calls s with
  | .error e => .error e
  | .ok earlier =>
    match scan src earlier.machine.state last.through last.probes last.outputs false with
    | .error e => .error e
    | .ok final => .ok (joinResult earlier final)

theorem public_finite_window_batching_exact (src : Source) (s : State) (calls : List Call)
    (through fuel slots : Nat)
    (source : SourceValid src) (ready : Ready src s) (inclusions : src.hasInclusions = true)
    (valid : ∀ c ∈ calls, PublicCallValid c)
    (lastValid : PublicCallValid {through, probes := fuel, outputs := slots})
    (wholeValid : PublicCallValid {through, probes := (restore (bounded src) s).rest.length + 1, outputs := (remaining (restore (bounded src) s)).length + 1})
    (sufficient : SufficientWindows through calls (restore (bounded src) s))
    (lastFuel : (resume calls (restore (bounded src) s)).machine.rest.length < fuel)
    (lastSlots : (remaining (resume calls (restore (bounded src) s)).machine).length < slots) :
    publicFinish src calls {through, probes := fuel, outputs := slots} s =
      scan src s through ((restore (bounded src) s).rest.length + 1)
        ((remaining (restore (bounded src) s)).length + 1) false := by
  have resumeSuccess := public_resume_kernel src calls s source ready inclusions valid
  have next := ready_resume_reconstructible src calls s source ready
  have lastSuccess := ready_scan_success src _ through fuel slots source next.1 inclusions lastValid.1 lastValid.2
  rw [next.2] at lastSuccess
  rw [ready_scan_success _ _ _ _ _ source ready inclusions wholeValid.1 wholeValid.2]
  simp only [publicFinish, resumeSuccess, lastSuccess]
  exact congrArg Except.ok (finite_run_window_batching_exact through fuel slots calls
    (restore (bounded src) s) sufficient lastFuel lastSlots)

end Scheduler2.Wire
