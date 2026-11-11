import Enumeration
open Scheduler2.Enumeration

private def fixture : Source :=
  { revision := 1, lowerNominal := 10, upperMillis := 30000, hasInclusions := true,
    raw := [{ nominal := 10, actualMillis := 10900, eligible := true },
            { nominal := 20, actualMillis := 20000, eligible := true }] }

-- A one-probe call retains the complete jittered candidate beyond its window.
#guard match scan fixture 10500 1 3 (initial fixture 9) with
  | .ok r => r.stop == .waiting && r.probes == 1 && r.output.isEmpty &&
      r.state.searchAfter == 10 && r.state.disposedThrough == 9 &&
      r.state.pending == some { nominal := 10, actualMillis := 10900, eligible := true }
  | .error _ => false

-- A zero-probe resume can still emit the already buffered candidate.
#guard match scan fixture 10500 1 3 (initial fixture 9) with
  | .ok first => match scan fixture 10900 0 3 first.state with
    | .ok second => second.stop == .yielded && second.probes == 0 &&
        second.output == [{ nominal := 10, actualMillis := 10900, eligible := true }] &&
        second.state.disposedThrough == 10
    | .error _ => false
  | .error _ => false

-- Output-limited and probe-limited are distinct, including at zero.
#guard match scan fixture 30000 0 1 (initial fixture 9) with
  | .ok r => r.stop == .yielded
  | .error _ => false
#guard match scan fixture 30000 1 0 (initial fixture 9) with
  | .ok r => r.stop == .limited
  | .error _ => false

-- An impossible configured source differs from a definition with no terms.
#guard match scan { fixture with raw := [] } 30000 1 1 (initial fixture 9) with
  | .ok r => r.stop == .impossible
  | .error _ => false
#guard match scan { fixture with raw := [], hasInclusions := false }
    30000 1 1 (initial fixture 9) with
  | .ok r => r.stop == .empty
  | .error _ => false

-- A suffix with no remaining raw matches is exhausted, not globally impossible.
#guard match scan { fixture with raw := [] } 30000 1 1 (initial fixture 10) with
  | .ok r => r.stop == .exhausted
  | .error _ => false

-- Updating before a buffered future nominal can reconsider that nominal.
#guard match scan fixture 10000 1 3 (initial fixture 9) with
  | .ok r => match update { fixture with revision := 2 } 9 r.state with
    | .ok updated => updated.state.searchAfter == 9 && updated.state.disposedThrough == 9 &&
        updated.state.pending == none && updated.dispositions.length == 1 &&
        updated.state.revision == 2
    | .error _ => false
  | .error _ => false

-- A revision change cannot silently replace the source used by a pending scan.
#guard match scan { fixture with revision := 2 } 30000 1 1 (initial fixture 9) with
  | .error .changedRevision => true
  | _ => false

-- Range errors are not exhaustion.
#guard match scan { fixture with upperMillis := 10500 }
    30000 1 1 (initial fixture 9) with
  | .error .actualOutsideRange => true
  | _ => false

-- A future exclusion cannot advance the disposition cursor before its window.
private def excludedFixture : Source :=
  { fixture with raw := [{ nominal := 10, actualMillis := 10000, eligible := false }] }
#guard match scan excludedFixture 9000 1 3 (initial excludedFixture 9) with
  | .ok r => r.stop == .waiting && r.state.disposedThrough == 9 &&
      r.state.searchAfter == 10 && r.dispositions.isEmpty && r.output.isEmpty
  | .error _ => false
#guard match scan excludedFixture 9000 1 3 (initial excludedFixture 9) with
  | .ok first => match scan excludedFixture 10000 1 3 first.state with
    | .ok second => second.stop == .impossible && second.state.disposedThrough == 10 &&
        second.output.isEmpty && second.dispositions.length == 1
    | .error _ => false
  | .error _ => false

-- The skipped interval is disposed through the explicit update boundary.
#guard match update { fixture with revision := 2 } 15 (initial fixture 9) with
  | .ok updated => updated.state.searchAfter == 15 && updated.state.disposedThrough == 15
  | .error _ => false

-- An older update time cannot regress the disposition cursor.
#guard match update { fixture with revision := 2 } 8 (initial fixture 9) with
  | .ok updated => updated.state.searchAfter == 9 && updated.state.disposedThrough == 9
  | .error _ => false

#guard match update fixture 15 (initial fixture 9) with
  | .error .staleRevision => true
  | _ => false
#guard match update { fixture with revision := 0 } 15 (initial fixture 9) with
  | .error .staleRevision => true
  | _ => false
