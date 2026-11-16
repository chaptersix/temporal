import Lean
import Frontier

open Lean Scheduler2.Exact

namespace Scheduler2.Wire

def upper : Nat := 3187296000000000000

structure Source where
  identity : String
  revision : Nat
  lower : Nat
  nominalEnd : Nat
  upper : Nat
  hasInclusions : Bool
  raw : List Candidate

def field (j : Json) (k : String) : Except String Json := j.getObjVal? k
def textField (j : Json) (k : String) : Except String String := do (← field j k).getStr?
-- Parse representation before applying the operation's semantic validation order.
def intField (j : Json) (k : String) : Except String Int := do
  let s ← textField j k
  let digits := if s.startsWith "-" then s.drop 1 else s
  if digits.isEmpty || !digits.toList.all Char.isDigit then throw "wire-invalid"
  match s.toInt? with
  | some n => pure n
  | none => throw "wire-invalid"

-- Invalid signed values remain invalid without deciding their error class here.
-- The public identity/budget/state guards determine that class in contract order.
def natField (j : Json) (k : String) (negative : Nat := upper + 1) : Except String Nat := do
  let n ← intField j k
  return if n < 0 then negative else n.toNat
def boolField (j : Json) (k : String) : Except String Bool := do (← field j k).getBool?

def keys (j : Json) (allowed : List String) : Except String Unit := do
  let obj ← j.getObj?
  for (k, _) in obj.toList do
    if !allowed.contains k then throw "wire-invalid"

def candidate (j : Json) : Except String Candidate := do
  keys j ["nominal", "actual", "revision", "excluded"]
  return { nominal := ← natField j "nominal", actual := ← natField j "actual", revision := ← natField j "revision" 0, excluded := ← boolField j "excluded" }

def source (j : Json) : Except String Source := do
  keys j ["identity", "revision", "lower", "nominalEnd", "upper", "hasInclusions", "raw"]
  let raw ← (← field j "raw").getArr?
  return { identity := ← textField j "identity", revision := ← natField j "revision" 0, lower := ← natField j "lower", nominalEnd := ← natField j "nominalEnd", upper := ← natField j "upper", hasInclusions := ← boolField j "hasInclusions", raw := (← raw.toList.mapM candidate) }

def parseStop (s : String) : Except String Stop :=
  match s with
  | "active" => pure .active
  | "empty" => pure .empty
  | "impossible" => pure .impossible
  | "exhausted" => pure .exhausted
  | _ => pure .internalLimit

def state (j : Json) : Except String State := do
  keys j ["identity", "revision", "origin", "searchAfter", "disposedThrough", "pending", "fullRange", "eligibleSeen", "terminal"]
  let p ← field j "pending"
  let pending ← if p == Json.null then pure none else some <$> candidate p
  return { identity := ← textField j "identity", revision := ← natField j "revision" 0, origin := ← natField j "origin", searchAfter := ← natField j "searchAfter", disposedThrough := ← natField j "disposedThrough", pending, fullRange := ← boolField j "fullRange", eligibleSeen := ← boolField j "eligibleSeen", terminal := ← parseStop (← textField j "terminal") }

def supported (n : Nat) : Bool := 0 < n && n ≤ upper
def validState (s : State) : Bool :=
  (s.terminal == .active || s.terminal == .empty || s.terminal == .impossible || s.terminal == .exhausted) &&
  s.revision > 0 && !s.identity.isEmpty && s.origin ≤ s.disposedThrough &&
  s.disposedThrough ≤ s.searchAfter && s.searchAfter ≤ upper &&
  (s.terminal != .impossible || (s.fullRange && !s.eligibleSeen)) &&
  match s.pending with
  | none => s.searchAfter == s.disposedThrough
  | some c => s.terminal == .active && c.revision == s.revision &&
      supported c.nominal && supported c.actual && (c.nominal - 1) % 1000000000 == 0 &&
      c.nominal ≤ c.actual && c.nominal == s.searchAfter && s.disposedThrough < c.nominal &&
      (!c.excluded || c.actual == c.nominal) && (c.excluded || s.eligibleSeen)

def initial (src : Source) (after : Nat) : State :=
  (beginMachine src.identity src.revision src.lower after src.raw).state

def begin (src : Source) (after : Nat) : Except String State :=
  if after > upper then .error "input-invalid" else .ok (initial src after)

def replacementAt (src : Source) (s : State) (boundary : Nat) : Except String (State × Option Candidate) :=
  match begin src boundary with
  | .error e => .error e
  | .ok next => .ok (next, s.pending)

def replacement (src : Source) (s : State) (atTime : Option Nat) : Except String (State × Option Candidate) :=
  if !validState s then .error "state-invalid"
  else if src.revision ≤ s.revision then .error "stale-revision"
  else match atTime with
    | some time => if !supported time then .error "input-invalid"
      else replacementAt src s (max time s.disposedThrough)
    | none => replacementAt src s s.disposedThrough

def dispose (src : Source) (s : State) : Except String State :=
  if src.identity != s.identity || src.revision != s.revision then .error "identity-mismatch"
  else if !validState s then .error "state-invalid"
  else match s.pending with
    | none => .error "state-invalid"
    | some c => if c.excluded then .error "state-invalid"
      else .ok { s with pending := none, disposedThrough := c.nominal }

def asPeek (initial : Machine) (r : Result) : Result :=
  match r.output with
  | [] => r
  | c :: _ =>
    let disposed := (remaining initial).takeWhile (fun x => x.nominal < c.nominal)
    let cursor := disposed.foldl (fun _ x => x.nominal) initial.state.disposedThrough
    { r with output := [], stop := .active, machine := { r.machine with state := { r.machine.state with pending := some c, disposedThrough := cursor } } }

def scan (src : Source) (s : State) (through probes outputs : Nat) (peek : Bool) : Except String Result :=
  if src.identity != s.identity || src.revision != s.revision then .error "identity-mismatch"
  else if !supported through || probes < 1 || probes > 1024 || outputs < 1 || outputs > 128 then .error "input-invalid"
  else if !validState s || s.fullRange != (s.origin < src.lower) || (s.terminal == .empty && src.hasInclusions) then .error "state-invalid"
  else if s.terminal != .active then .ok (done { state := s, rest := [] } s.terminal)
  else if !src.hasInclusions then .ok (done { state := { s with terminal := .empty }, rest := [] } .empty)
  else
    let bounded := src.raw.filter (fun c => src.lower ≤ c.nominal && c.nominal ≤ src.nominalEnd)
    let m := restore bounded s
    let rest := m.rest
    let result := run through probes (if peek then 1 else outputs) m
    -- Inspect only actually probed candidates, not an unreachable bad suffix.
    let probed := rest.take (rest.length - result.machine.rest.length)
    if probed.any (fun c => !c.excluded && !supported c.actual) then .error "actual-outside-range"
    else if result.stop == .internalLimit then .error "state-invalid"
    else if peek then .ok (asPeek m result)
    else .ok result

def stopText : Stop → String
  | .active => "active" | .empty => "empty" | .impossible => "impossible"
  | .exhausted => "exhausted" | .waiting => "waiting" | .yielded => "yield"
  | .limited => "limited" | .internalLimit => "internal-limit"
def njson (n : Nat) : Json := toJson (toString n)
def candidateJson (c : Candidate) : Json := Json.mkObj
  [("nominal", njson c.nominal), ("actual", njson c.actual), ("revision", njson c.revision), ("excluded", toJson c.excluded)]
def pendingJson (p : Option Candidate) : Json := p.map candidateJson |>.getD Json.null
def stateJson (s : State) : Json := Json.mkObj
  [("identity", toJson s.identity), ("revision", njson s.revision), ("origin", njson s.origin),
   ("searchAfter", njson s.searchAfter), ("disposedThrough", njson s.disposedThrough),
   ("pending", pendingJson s.pending), ("fullRange", toJson s.fullRange),
   ("eligibleSeen", toJson s.eligibleSeen), ("terminal", toJson (stopText s.terminal))]
def success (s : State) (status : Stop) (output : List Candidate := []) (probes : Nat := 0)
    (superseded : Option Candidate := none) : Json := Json.mkObj
  [("version", toJson (1 : Nat)), ("ok", toJson true), ("state", stateJson s),
   ("status", toJson (stopText status)), ("outputs", toJson (output.map candidateJson)),
   ("probes", njson probes), ("superseded", pendingJson superseded)]

def execute (j : Json) : Except String Json := do
  if (← field j "version") != toJson (1 : Nat) then throw "wire-invalid"
  let op ← textField j "op"
  let extra ← match op with
    | "begin" => pure ["after"]
    | "beginRange" => pure ["start", "end"]
    | "run" => pure ["through", "probes", "outputs"]
    | "peek" => pure ["through", "probes"]
    | "peekRange" => pure ["end", "probes"]
    | "update" => pure ["at"]
    | "disposeRange" | "rebaseRange" => pure []
    | _ => throw "wire-invalid"
  keys j (["version", "op", "source", "state"] ++ extra)
  let src ← source (← field j "source")
  if src.identity.isEmpty || src.revision == 0 || src.upper != upper || !supported src.lower || !supported src.nominalEnd then throw "source-invalid"
  if op == "begin" || op == "beginRange" then
    if (← field j "state") != Json.null then throw "wire-invalid"
    let after ← if op == "begin" then natField j "after" else do
      let start ← natField j "start"
      let finish ← natField j "end"
      if !supported start || !supported finish || finish < start then throw "input-invalid"
      pure (start - 1)
    let s ← begin src after
    return success s s.terminal
  let s ← state (← field j "state")
  if op == "update" || op == "rebaseRange" then
    let atTime ← if op == "update" then some <$> natField j "at" else pure none
    let (next, superseded) ← replacement src s atTime
    return success next next.terminal [] 0 superseded
  if op == "disposeRange" then
    let next ← dispose src s
    return success next next.terminal
  let through ← natField j (if op == "peekRange" then "end" else "through")
  if op == "peekRange" then
    if (← intField j "end") < (← intField (← field j "state") "origin") then throw "input-invalid"
  let probes ← natField j "probes" 0
  let outputs ← if op == "run" then natField j "outputs" 0 else pure 1
  let r ← scan src s through probes outputs (op != "run")
  let status := if op == "peekRange" && r.stop == .waiting then .exhausted else r.stop
  return success r.machine.state status r.output r.probes

def response (line : String) : Json :=
  match Json.parse line >>= execute with
  | .ok j => j
  | .error error => Json.mkObj [("version", toJson (1 : Nat)), ("ok", toJson false),
      ("error", toJson (if ["wire-invalid", "source-invalid", "input-invalid", "state-invalid", "identity-mismatch", "stale-revision", "actual-outside-range"].contains error then error else "wire-invalid"))]

end Scheduler2.Wire

def main : IO Unit := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  repeat
    let line ← stdin.getLine
    if line.isEmpty then break
    stdout.putStrLn (Scheduler2.Wire.response line).compress
    stdout.flush
