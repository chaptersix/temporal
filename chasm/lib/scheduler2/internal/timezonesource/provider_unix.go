//go:build unix && !ios && !android

package timezonesource

import (
	"os"
	"runtime"

	"go.temporal.io/server/common/clock"
)

// NewUnixEnvironment captures its own process source history. Runtime integration
// must share this provider between builders and establish its startup boundary.
//
//nolint:staticcheck // Mirror Go time.loadLocation, which uses the build runtime GOROOT fallback.
func NewUnixEnvironment(timeSource clock.TimeSource) *Environment {
	return NewEnvironment(SourceConfig{LookupEnv: os.LookupEnv, Read: ReadSource, Embedded: Embedded, Platform: []string{"/usr/share/zoneinfo/", "/usr/share/lib/zoneinfo/", "/usr/lib/locale/TZ/", "/etc/zoneinfo"}, GOROOT: gorootSource(runtime.GOROOT()), Clock: timeSource})
}
