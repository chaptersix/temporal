package timezonesource

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func resolverFixture(t *testing.T) (*Environment, map[string][]byte, *clock.EventTimeSource, *[]string) {
	t.Helper()
	data, err := Embedded("America/New_York")
	require.NoError(t, err)
	files := map[string][]byte{"override/Zone": data, "platform/Zone": data, "embedded/Zone": data, "goroot.zip/Zone": data}
	calls := []string{}
	read := func(source, name string) ([]byte, error) {
		key := source + "/" + name
		calls = append(calls, key)
		if data, ok := files[key]; ok {
			return bytes.Clone(data), nil
		}
		return nil, syscall.ENOENT
	}
	now := clock.NewEventTimeSource().Update(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	environment := NewEnvironment(SourceConfig{LookupEnv: func(key string) (string, bool) {
		if key == "ZONEINFO" {
			return "override", true
		}
		return "", false
	}, Read: read, Embedded: func(name string) ([]byte, error) { return read("embedded", name) }, Platform: []string{"platform"}, GOROOT: "goroot.zip", Clock: now})
	return environment, files, now, &calls
}
func TestSourceResolutionPrecedence(t *testing.T) {
	t.Parallel()
	for _, winner := range []string{"override", "platform", "embedded", "goroot.zip"} {
		t.Run(winner, func(t *testing.T) {
			t.Parallel()
			environment, files, _, calls := resolverFixture(t)
			order := []string{"override", "platform", "embedded", "goroot.zip"}
			var want []string
			for _, source := range order {
				want = append(want, source+"/Zone")
				if source == winner {
					break
				}
				files[source+"/Zone"] = []byte("corrupt")
			}
			snapshot, err := NewResolver(environment).Resolve("Zone", nil)
			require.NoError(t, err)
			require.Equal(t, winner, snapshot.Source())
			require.Equal(t, want, *calls)
			detached := snapshot.Data()
			clear(detached)
			require.NotEqual(t, detached, snapshot.Data())
		})
	}
}
func TestSourceErrorPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		overrideRead error
		platformBad  bool
		want         string
	}{
		{"ignored override parser error", nil, false, "unknown time zone Zone"},
		{"platform parser retained", nil, true, "malformed time zone information"},
		{"override read wins", syscall.EACCES, true, "permission denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			environment, files, _, _ := resolverFixture(t)
			clear(files)
			files["override/Zone"] = []byte("bad")
			if tc.platformBad {
				files["platform/Zone"] = []byte("bad")
			}
			if tc.overrideRead != nil {
				original := environment.config.Read
				environment.config.Read = func(source, name string) ([]byte, error) {
					if source == "override" {
						return nil, tc.overrideRead
					}
					return original(source, name)
				}
			}
			_, err := NewResolver(environment).Resolve("Zone", nil)
			require.EqualError(t, err, tc.want)
		})
	}
}
func TestSourceCacheRefreshAndCustomBypass(t *testing.T) {
	t.Parallel()
	environment, files, now, calls := resolverFixture(t)
	resolver := NewResolver(environment)
	first, err := resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	replacement, err := Embedded("Europe/London")
	require.NoError(t, err)
	files["override/Zone"] = replacement
	now.Advance(24 * time.Hour)
	atBoundary, err := resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	require.Same(t, first, atBoundary)
	require.Len(t, *calls, 1)
	now.Advance(time.Nanosecond)
	refreshed, err := resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	require.NotSame(t, first, refreshed)
	require.NotEqual(t, first.Prepared().Fingerprint(), refreshed.Prepared().Fingerprint())
	require.NotEqual(t, first.ResolvedAt(), refreshed.ResolvedAt())
	_, err = resolver.Resolve("Zone", []byte{})
	require.EqualError(t, err, "malformed time zone information")
	custom, err := resolver.Resolve("Zone", first.Data())
	require.NoError(t, err)
	require.Equal(t, "custom", custom.Source())
	again, err := resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	require.Same(t, refreshed, again)
	_, err = resolver.Resolve("Missing", nil)
	require.Error(t, err)
	files["override/Missing"] = replacement
	_, err = resolver.Resolve("Missing", nil)
	require.Error(t, err)
	now.Advance(24*time.Hour + time.Nanosecond)
	_, err = resolver.Resolve("Missing", nil)
	require.NoError(t, err)
}
func TestSourceCacheCapacityAndCapture(t *testing.T) {
	t.Parallel()
	environment, _, _, calls := resolverFixture(t)
	resolver := NewResolver(environment)
	_, err := resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	environment.config.LookupEnv = func(string) (string, bool) { return "other", true }
	for i := range 1000 {
		_, err := resolver.Resolve(fmt.Sprintf("missing%d", i), nil)
		require.Error(t, err)
	}
	before := len(*calls)
	_, err = resolver.Resolve("Zone", nil)
	require.NoError(t, err)
	require.Len(t, *calls, before+1)
	require.Equal(t, "override/Zone", (*calls)[before])
}
func TestSourceSpecialNamesAndUnixLocal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, tz string
		present  bool
		source   string
		utc      bool
	}{
		{"unset", "", false, "/etc/localtime", false},
		{"empty", "", true, "", true},
		{"UTC", "UTC", true, "", true},
		{"colon empty", ":", true, "", true},
		{"absolute", ":/custom", true, "//custom", false},
		{"named", ":Zone", true, "platform/Zone", false},
		{"invalid", "missing", true, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			environment, files, now, _ := resolverFixture(t)
			data := bytes.Clone(files["platform/Zone"])
			if tc.source != "" {
				files[tc.source] = data
			}
			environment.config.LookupEnv = func(key string) (string, bool) {
				if key == "TZ" {
					return tc.tz, tc.present
				}
				return "override", true
			}
			resolver := NewResolver(environment)
			first, err := resolver.Resolve("Local", nil)
			require.NoError(t, err)
			require.Equal(t, tc.utc, first.UTC())
			require.Equal(t, "Local", first.Name())
			clear(files)
			now.Advance(25 * time.Hour)
			again, err := resolver.Resolve("Local", nil)
			require.NoError(t, err)
			require.Same(t, first, again)
			other, err := NewResolver(environment).Resolve("Local", nil)
			require.NoError(t, err)
			require.Same(t, first, other)
		})
	}
	environment, _, _, calls := resolverFixture(t)
	resolver := NewResolver(environment)
	for _, name := range []string{"", "UTC"} {
		snapshot, err := resolver.Resolve(name, nil)
		require.NoError(t, err)
		require.True(t, snapshot.UTC())
	}
	for _, name := range []string{"../Zone", "A..B", "/Zone", "\\Zone"} {
		_, err := resolver.Resolve(name, nil)
		require.EqualError(t, err, "time: invalid location name")
	}
	require.Empty(t, *calls)
}
func TestSourceUnknownErrorIsCached(t *testing.T) {
	t.Parallel()
	environment, _, _, _ := resolverFixture(t)
	failure := errors.New("source unavailable")
	environment.config.Read = func(string, string) ([]byte, error) { return nil, failure }
	environment.config.Embedded = nil
	resolver := NewResolver(environment)
	_, first := resolver.Resolve("Zone", nil)
	_, second := resolver.Resolve("Zone", nil)
	require.Same(t, first, second)
	require.ErrorIs(t, first, failure)
}
