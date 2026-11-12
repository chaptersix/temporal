# Independent schedule context metadata review

Reviewer: api_seam. Read public_context.go/tests, removal of lifecycle.go's nil stub,
new actual schedule2_metadata_test.go and old scheduler.ContextMetadata. No finding.

The provider exactly matches the old scheduler's conditional workflow-type and task-queue
keys, omitting absent/empty strings and returning nil for no metadata. It reads only
PublicSchedule's action skeleton, whose workflow type/task queue remain inline after
large-payload extraction. It does not resolve payload owner IDs, so unrelated unavailable
large input data cannot prevent request attribution. Closed roots retain attribution for
Delete responses, matching the reference behavior. Missing public profile is safe via
TryGet and protobuf nil getters. Returning a new map avoids aliasing stored metadata.

Removing the lifecycle nil implementation resolves the competing method declaration;
there is one provider. Unit tests cover missing public action, empty action, closed root
and invalid payload-owner ref. Real Patch/Delete tests enable the existing frontend
trailer configuration, assert both exact keys through actual API transport, and independently
verify persisted scheduler2 identity. Parent reports original both-case failure and
corrected metadata03 PASS1.246s. This reviewer inspected source and did not rerun those
tests. This scope does not assert metadata coverage for unrelated RPC methods.
