# Independent compact address review

No source defect found. Fixed16-byte namespace IDs remain injective when only trailing zero bytes are removed: decoding pads back to exactly16 bytes, preserving interior zeros and arbitrary fullwidth IDs. Allzero has the unique empty namespace segment. Raw URL base64 uses no slash, so three address segments are unambiguous. Hex ordinal preserves uint64; decode-and-reencode rejects leading zeros, uppercase/sign variants, encoded trailing zero namespaces, alternate base64 tail bits/newlines and other aliases. Address maximum is41 ASCII bytes.

Store traversal uses exact addressed reads and logical entry separators, never lexical physical page-address ordering. Commit's sorted address list only stabilizes atomic Apply order. Immutable request addresses q/<base64url exact8-byte big-endian sequence> preserve all64 bits and have13 bytes. This is a single immutable-request namespace, not a generic encoding for multiple Q stores; p/ and q/ remain disjoint.

Independent execution of canonical/fullwidth/random namespace tests and immutable request extremes PASS packedstore0.191s and scheduler2 0.645s. No source edits. No runtime selection or storage-fit claim follows from address correctness; frozen physical framing/namespace variants remain separate author measurements.
