package testcore

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/testhooks"
)

// ScheduleFrontendFactory registers a test library and binds it to the actual History runtime.
// It runs during construction, before any service starts.
type ScheduleFrontendFactory func(chasm.Engine, chasm.VisibilityManager, *chasm.Registry) (testhooks.ScheduleFrontend, error)

// WithScheduleFrontend selects an isolated schedule backend for a dedicated cluster.
// frontendLibraries register offline identities in the separate frontend registry,
// allowing admin inspection and visibility APIs to resolve the same archetypes.
func WithScheduleFrontend(factory ScheduleFrontendFactory, frontendLibraries ...chasm.Library) TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
		o.dedicatedReason = "schedule frontend configured"
		o.clusterOptions = append(o.clusterOptions, func(params *testClusterParams) {
			params.ScheduleFrontendFactory = factory
			params.ScheduleFrontendLibraries = append([]chasm.Library(nil), frontendLibraries...)
		})
	}
}
