# Integrator independent seam review

Reviewed delegate-owned frontend dispatch, query validation, testhooks and testcore
construction against surrounding default handlers and actual FX startup. Selection is
cluster-scoped and installed before serving; production testhooks are disabled. Default
handlers retain their branches; selected errors never fall back. Create retains common
payload/SA/namespace validation, and public auth denial proves interceptors precede
dispatch. Unsupported Update/Patch/Matching remain explicit M3 dependencies.

History and frontend have separate registries; both now register before startup.
Factory errors abort startup and cleanup survives until stop. One-history-host scope
is explicit. Typed visibility retains native lifecycle/archetype filtering, rejects
resolved division overrides, and preserves namespace-custom ScheduleId precedence.
Real same-ID old/new rows and custom aliases substantiate those checks.

The final returned-SA test exposed skipped List response cleanup; the selected path
now applies the existing common cleanup and a focused test preserves custom values
and token while removing internal fields. Final public/race/lint pass. Existing
V1/CHASM shared suites also pass (checks.json records source-timing limits). No
remaining blocking seam finding for WI-014. This is not approval of full M3 execution.
