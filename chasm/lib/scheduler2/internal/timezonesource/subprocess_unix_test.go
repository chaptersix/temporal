//go:build unix && !ios && !android

package timezonesource

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/stretchr/testify/require"
)

type coldResult struct {
	LegacyError, PreparedError     string
	LegacyOffsets, PreparedOffsets []int
}

func TestSourceColdProcessHelper(t *testing.T) {
	if os.Getenv("SCHEDULER_ZONE_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	name := os.Getenv("SCHEDULER_ZONE_NAME")
	legacy, legacyErr := time.LoadLocation(name)
	snapshot, preparedErr := NewResolver(NewUnixEnvironment(nil)).Resolve(name, nil)
	result := coldResult{}
	if legacyErr != nil {
		result.LegacyError = legacyErr.Error()
	}
	if preparedErr != nil {
		result.PreparedError = preparedErr.Error()
	}
	if legacyErr == nil && preparedErr == nil {
		prepared := time.UTC
		if !snapshot.UTC() {
			var err error
			prepared, err = time.LoadLocationFromTZData(name, snapshot.Data())
			require.NoError(t, err)
		}
		for _, date := range []string{"1900-01-01T00:00:00Z", "2024-03-10T07:00:00Z", "2024-11-03T05:15:00Z", "2024-11-03T06:15:00Z", "2050-07-01T00:00:00Z"} {
			instant, err := time.Parse(time.RFC3339, date)
			require.NoError(t, err)
			_, left := instant.In(legacy).Zone()
			_, right := instant.In(prepared).Zone()
			result.LegacyOffsets = append(result.LegacyOffsets, left)
			result.PreparedOffsets = append(result.PreparedOffsets, right)
		}
	}
	require.NoError(t, json.NewEncoder(os.Stdout).Encode(result))
}
func TestSourceColdProcessParity(t *testing.T) {
	t.Parallel()
	data, err := Embedded("America/New_York")
	require.NoError(t, err)
	for _, kind := range []string{"directory", "zip", "invalid bytes", "compressed", "local absolute", "local empty"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			name := "SchedulerColdFixture"
			override := directory
			tz := ""
			switch kind {
			case "directory":
				require.NoError(t, os.WriteFile(filepath.Join(directory, name), data, 0600))
			case "invalid bytes":
				require.NoError(t, os.WriteFile(filepath.Join(directory, name), []byte("invalid"), 0600))
			case "zip", "compressed":
				var output bytes.Buffer
				writer := zip.NewWriter(&output)
				method := uint16(zip.Store)
				if kind == "compressed" {
					method = zip.Deflate
				}
				entry, err := writer.CreateHeader(&zip.FileHeader{Name: name, Method: method})
				require.NoError(t, err)
				_, err = entry.Write(data)
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				override = filepath.Join(directory, "zones.zip")
				require.NoError(t, os.WriteFile(override, output.Bytes(), 0600))
			case "local absolute":
				name = "Local"
				tz = filepath.Join(directory, "zone")
				require.NoError(t, os.WriteFile(tz, data, 0600))
				tz = ":" + tz
			case "local empty":
				name = "Local"
			default:
				t.Fatal("unexpected fixture")
			}
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSourceColdProcessHelper$", "-test.v=false")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "ZONEINFO=") && !strings.HasPrefix(entry, "TZ=") && !strings.HasPrefix(entry, "SCHEDULER_ZONE_") {
					command.Env = append(command.Env, entry)
				}
			}
			command.Env = append(command.Env, "ZONEINFO="+override, "TZ="+tz, "SCHEDULER_ZONE_CHILD=1", "SCHEDULER_ZONE_NAME="+name)
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			var result coldResult
			require.NoError(t, json.NewDecoder(bytes.NewReader(output)).Decode(&result), string(output))
			require.Equal(t, result.LegacyError, result.PreparedError)
			require.Equal(t, result.LegacyOffsets, result.PreparedOffsets)
			if kind == "invalid bytes" || kind == "compressed" {
				require.NotEmpty(t, result.LegacyError)
			} else {
				require.Len(t, result.LegacyOffsets, 5)
			}
		})
	}
}

func TestEmptyGOROOTSkipsFallback(t *testing.T) {
	t.Parallel()
	require.Empty(t, gorootSource(""))
	require.Equal(t, "/toolchain/lib/time/zoneinfo.zip", gorootSource("/toolchain"))
	calls := 0
	environment := NewEnvironment(SourceConfig{
		LookupEnv: func(string) (string, bool) { return "", false },
		Read:      func(string, string) ([]byte, error) { calls++; return nil, os.ErrNotExist },
		GOROOT:    gorootSource(""),
	})
	_, err := NewResolver(environment).Resolve("Missing", nil)
	require.EqualError(t, err, "unknown time zone Missing")
	require.Zero(t, calls)
}
