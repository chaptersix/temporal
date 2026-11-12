import PublicComposition
open Scheduler2.Exact

#print axioms loop_conservation
#print axioms run_probe_bound
#print axioms run_no_internal_limit
#print axioms sufficient_completeness
#print axioms resume_cache_preserved
#print axioms resume_ordered
#print axioms resumable_completeness
#print axioms resume_unique
#print axioms finite_window_batching
#print axioms adjacent_window_batching
#print axioms begin_frontier
#print axioms loop_frontier
#print axioms run_reconstructible
#print axioms begin_bounds
#print axioms loop_bounds
#print axioms loop_authority
#print axioms pending_stable
#print axioms initial_resumability
#print axioms Scheduler2.Wire.begin_identity
#print axioms Scheduler2.Wire.replacement_boundary
#print axioms Scheduler2.Wire.scan_success_kernel
#print axioms Scheduler2.Wire.replacement_stale
#print axioms Scheduler2.Wire.dispose_success
#print axioms Scheduler2.Wire.peek_no_output
#print axioms Scheduler2.Wire.peek_retains

-- These tests execute the proved kernel, not the old abstract executable.
private def c1 : Candidate := { nominal := 1, actual := 500000001, revision := 1, excluded := false }
private def c2 : Candidate := { nominal := 1000000001, actual := 1000000001, revision := 1, excluded := true }
private def c3 : Candidate := { nominal := 2000000001, actual := 2500000001, revision := 1, excluded := false }
private def initial : Machine :=
  { state := { identity := "fixture", revision := 1, origin := 0, searchAfter := 0,
               disposedThrough := 0, fullRange := true }, rest := [c1, c2, c3] }

#guard (run 1 1 1 initial).machine.state.pending == some c1
#guard (run 1 1 1 initial).stop == .waiting
#guard (resume (List.replicate 8 {through := 3000000001, probes := 1, outputs := 1}) initial).output == [c1, c3]
#guard (resume [{through := 1, probes := 1, outputs := 1},
                {through := 500000001, probes := 1, outputs := 1},
                {through := 3000000001, probes := 8, outputs := 8}] initial).output == [c1, c3]

#print axioms Scheduler2.Wire.scan_reconstructible
#print axioms Scheduler2.Wire.scan_conservation
#print axioms Scheduler2.Wire.initial_valid
#print axioms Scheduler2.Wire.disposition_valid
#print axioms Scheduler2.Wire.probe_valid
#print axioms Scheduler2.Wire.terminal_valid
#print axioms Scheduler2.Wire.loop_valid
#print axioms Scheduler2.Wire.run_valid
#print axioms Scheduler2.Wire.dispose_valid
#print axioms Scheduler2.Wire.scan_active_success
#print axioms Scheduler2.Wire.restore_frontier
#print axioms Scheduler2.Wire.scan_active_valid
#print axioms Scheduler2.Wire.replacement_valid
#print axioms Scheduler2.Wire.loop_zero_slots_state
#print axioms Scheduler2.Wire.loop_one_output
#print axioms Scheduler2.Wire.run_output_member
#print axioms Scheduler2.Wire.remaining_after_disposed
#print axioms Scheduler2.Wire.fold_cursor_bounds
#print axioms Scheduler2.Wire.pending_candidate_valid
#print axioms Scheduler2.Wire.loop_seen_true
#print axioms Scheduler2.Wire.loop_seen
#print axioms Scheduler2.Wire.peek_run_valid
#print axioms Scheduler2.Wire.peek_dispose_run
#print axioms Scheduler2.Wire.scan_peek_of_run
#print axioms Scheduler2.Wire.loop_not_empty
#print axioms Scheduler2.Wire.ready_run
#print axioms Scheduler2.Wire.ready_scan_success
#print axioms Scheduler2.Wire.initial_ready
#print axioms Scheduler2.Wire.public_resume_kernel
#print axioms Scheduler2.Wire.public_resume_complete
#print axioms Scheduler2.Wire.public_initial_complete
#print axioms Scheduler2.Wire.peek_reconstructible
#print axioms Scheduler2.Wire.ready_peek_run
#print axioms Scheduler2.Wire.initial_empty_ready
#print axioms Scheduler2.Wire.empty_ready_next
#print axioms Scheduler2.Wire.empty_scan_success
#print axioms Scheduler2.Wire.public_empty_resume
#print axioms Scheduler2.Wire.replacement_initial
#print axioms Scheduler2.Wire.replacement_ready
#print axioms Scheduler2.Wire.public_peek_dispose
#print axioms Scheduler2.Wire.peek_pending_stable
#print axioms Scheduler2.Wire.public_capacity_wait
#print axioms Scheduler2.Wire.loop_all_excluded_impossible
#print axioms Scheduler2.Wire.run_impossible_no_eligible
#print axioms Scheduler2.Wire.full_initial_remaining
#print axioms Scheduler2.Wire.initial_impossible_classification
#print axioms Scheduler2.Wire.initial_all_excluded_impossible
#print axioms Scheduler2.Wire.scan_invalid_state
#print axioms Scheduler2.Wire.dispose_invalid_state
#print axioms Scheduler2.Wire.replacement_invalid_state
#print axioms loop_ticks_irrelevant
#print axioms run_probe_resume
#print axioms run_sufficient_exact
#print axioms complete_batching
#print axioms run_window_batching_exact
#print axioms run_window_batching_uniform
#print axioms run_disposedTrace_spec
#print axioms disposedTrace_compose
#print axioms run_probe_resume_dispositions
#print axioms finite_run_window_batching_exact
#print axioms finite_window_batching_exact
#print axioms Scheduler2.Wire.public_pair_kernel
#print axioms Scheduler2.Wire.public_probe_resume_exact
#print axioms Scheduler2.Wire.public_window_batching_exact
#print axioms Scheduler2.Wire.public_pending_stable
#print axioms Scheduler2.Wire.public_next_first
#print axioms Scheduler2.Wire.ready_resume_reconstructible
#print axioms Scheduler2.Wire.public_finite_window_batching_exact
