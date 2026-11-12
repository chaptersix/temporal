# Bounded range preparation primitive

The isolated compiler processes a validated calendar from immutable sixteen-range pages. Each call spends at most64 elementary steps; a step sets one range value or advances a field. Page reads are no greater than steps. Calendar matching output has six fixed masks plus a101-bit explicit-year mask or the distinct allYears flag. The continuation contains no source pointers or growing arrays.

Tests compare full expansion across fixed and mixed budgets, including1025 duplicate ranges, overlapping steps, field endpoints, maximal int32 steps, empty/explicit years, source/cursor/mask/page corruption, and failed reads after partial progress. Errors return the original continuation. Reconstructed bounded calendar masks produce the same reference UTC candidates. The final targeted suite passes0.806s.

Independent review found reversed End normalization accepted invalid nonzero endpoints and continuation masks lacked structural validation. Both are fixed and regression-covered. Semantic equality of a persisted partial mask to its already processed source prefix remains a trusted integrity obligation; fixed structural checks do not replay the source. Initial lint found complexity28; extracting bounded page validation resolved the code structure, with final combined lint recorded separately.

This is slice1 only. It has no scheduler routing, runtime schema or candidate-enumeration integration. Copying value state is not persistence/recovery evidence. Input-page identity/lifetime, durable compilation, timezone preparation, calendar carries, raw union/exclusion and List projection integration remain required WI018 work.
