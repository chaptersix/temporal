//go:build windows

package timezonesource

import (
	"os"
	"runtime"
	"strconv"
	"syscall"

	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common/clock"
)

// NewWindowsEnvironment captures the supported native API once for Local and
// uses the same named-source order as Go's Windows provider.
//
//nolint:staticcheck // Mirror Go time.loadLocation's runtime GOROOT fallback.
func NewWindowsEnvironment(timeSource clock.TimeSource) *Environment {
	return NewEnvironment(SourceConfig{LookupEnv: os.LookupEnv, Read: ReadSource, Embedded: Embedded, GOROOT: gorootSource(runtime.GOROOT()), Clock: timeSource, WindowsLocal: captureWindowsTimezone})
}
func captureWindowsTimezone() (occurrence.WindowsTimezoneSource, error) {
	var input syscall.Timezoneinformation
	if _, err := syscall.GetTimeZoneInformation(&input); err != nil {
		return occurrence.WindowsTimezoneSource{}, err
	}
	return occurrence.WindowsTimezoneSource{WordBits: strconv.IntSize, Bias: input.Bias, StandardBias: input.StandardBias, DaylightBias: input.DaylightBias, StandardName: input.StandardName, DaylightName: input.DaylightName, StandardDate: windowsSystemTime(input.StandardDate), DaylightDate: windowsSystemTime(input.DaylightDate)}, nil
}
func windowsSystemTime(input syscall.Systemtime) occurrence.WindowsSystemTime {
	return occurrence.WindowsSystemTime{Year: input.Year, Month: input.Month, DayOfWeek: input.DayOfWeek, Day: input.Day, Hour: input.Hour, Minute: input.Minute, Second: input.Second, Milliseconds: input.Milliseconds}
}
