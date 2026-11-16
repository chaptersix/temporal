package occurrence

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedTimezoneV1Compatibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{{"UTC", nil, "f71086c9d9ec810b52d526a264ae7c5bb42d3eab76ba0c06d08a6bc010117703"}, {"NewYork", preparedNewYorkTZ, "4cd0707dc7f320e89b58461d7f1544e50296500d5abdf887e6555a964fc5059b"}, {"London", preparedLondonTZ, "b61945e1b92e805a74cc5d6b3c4568394fc84d602613b1d9eb21975665a76046"}} {
		zone := PreparedUTC()
		if tc.data != nil {
			var err error
			zone, err = PrepareTimezone(tc.data, time.Unix(1704067200, 0))
			require.NoError(t, err)
		}
		hash := sha256.New()
		_, err := hash.Write(zone.Header())
		require.NoError(t, err)
		for i := uint32(0); i < zone.PageCount(); i++ {
			page, err := zone.Page(i)
			require.NoError(t, err)
			_, err = hash.Write(page)
			require.NoError(t, err)
		}
		got := fmt.Sprintf("%x", hash.Sum(nil))
		require.Equal(t, tc.want, got, tc.name)
	}
}
