package occurrence

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedTimezoneCodecRoundTrip(t *testing.T) {
	t.Parallel()
	z, err := PrepareTimezone(preparedNewYorkTZ, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	header := z.Header()
	require.Len(t, header, PreparedTimezoneHeaderBytes)
	pages := make(map[uint32][]byte)
	for ordinal := uint32(0); ordinal < z.PageCount(); ordinal++ {
		page, err := z.Page(ordinal)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), PreparedTimezoneTZifPageBytes)
		pages[ordinal] = page
	}
	reads := 0
	restored, err := RestorePreparedTimezone(header, func(ordinal uint32) ([]byte, error) { reads++; return bytes.Clone(pages[ordinal]), nil })
	require.NoError(t, err)
	require.Zero(t, reads)
	require.Equal(t, z.Fingerprint(), restored.Fingerprint())
	clear(header)
	for _, at := range []time.Time{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 11, 3, 5, 15, 0, 0, time.UTC), time.Date(2050, 7, 1, 0, 0, 0, 0, time.UTC)} {
		var expected, actual timezoneWork
		want, err := z.wall(at.Unix(), &expected)
		require.NoError(t, err)
		got, err := restored.wall(at.Unix(), &actual)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, expected, actual)
	}
	require.Positive(t, reads)
	_, err = z.Page(z.PageCount())
	require.Error(t, err)
	utc, err := RestorePreparedTimezone(PreparedUTC().Header(), nil)
	require.NoError(t, err)
	var work timezoneWork
	wall, err := utc.wall(0, &work)
	require.NoError(t, err)
	require.Equal(t, time.Unix(0, 0).UTC(), wall)
	require.Zero(t, work.pages)
}

func TestPreparedTimezoneCodecRejectsMalformed(t *testing.T) {
	t.Parallel()
	z, err := PrepareTimezone(preparedNewYorkTZ, time.Unix(0, 0))
	require.NoError(t, err)
	header := z.Header()
	for n := 0; n < len(header); n++ {
		_, err := RestorePreparedTimezone(header[:n], nil)
		require.Error(t, err)
	}
	for _, change := range []func([]byte){
		func(b []byte) { b[0] = 2 }, func(b []byte) { clear(b[1:33]) }, func(b []byte) { b[41] = 2 }, func(b []byte) { b[62] = 3 }, func(b []byte) { b[71] = 255 }, func(b []byte) { b[74] = 0 },
	} {
		bad := bytes.Clone(header)
		change(bad)
		_, err := RestorePreparedTimezone(bad, func(uint32) ([]byte, error) { return nil, nil })
		require.Error(t, err)
	}
	_, err = RestorePreparedTimezone(header, nil)
	require.Error(t, err)
	page, err := z.Page(0)
	require.NoError(t, err)
	for _, kind := range []string{"missing", "source", "ordinal", "length", "version", "error"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			bad := bytes.Clone(page)
			reader := func(uint32) ([]byte, error) { return bad, nil }
			switch kind {
			case "missing":
				bad = nil
			case "source":
				bad[1]++
			case "ordinal":
				bad[36]++
			case "length":
				bad[37]--
			case "version":
				bad[0]++
			case "error":
				reader = func(uint32) ([]byte, error) { return nil, errors.New("reader failed") }
			default:
				t.Fatal("unexpected case")
			}
			restored, err := RestorePreparedTimezone(header, reader)
			require.NoError(t, err)
			var work timezoneWork
			_, _, _, err = restored.lookup(-1<<40, &work)
			require.Error(t, err)
		})
	}
}
