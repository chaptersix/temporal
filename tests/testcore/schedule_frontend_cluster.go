package testcore

import "go.temporal.io/server/chasm"

// WithClusterScheduleFrontend binds an explicit test schedule backend in each
// local cluster constructed by a replication suite. Offline frontend identities
// match the History registry so admin and visibility requests resolve correctly.
func WithClusterScheduleFrontend(factory ScheduleFrontendFactory, frontendLibraries ...chasm.Library) TestClusterOption {
	return func(params *testClusterParams) {
		params.ScheduleFrontendFactory = factory
		params.ScheduleFrontendLibraries = append([]chasm.Library(nil), frontendLibraries...)
	}
}
