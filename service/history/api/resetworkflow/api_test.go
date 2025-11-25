package resetworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestGetResetReapplyExcludeTypes(t *testing.T) {
	t.Parallel()

	t.Run("Include all with no exclusions => no exclusions", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{},
				enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
			),
		)
	})

	t.Run("Include all with one exclusion => one exclusion (honor exclude in presence of default value of deprecated option)", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {}},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
				enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
			),
		)
	})

	t.Run("Include signal with no exclusions => exclude updates (honor non-default value of deprecated option in presence of default value of non-deprecated option)", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {}},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{},
				enumspb.RESET_REAPPLY_TYPE_SIGNAL,
			),
		)
	})

	t.Run("Include signal with exclude signal => include signal means they want to exclude updates, and then the explicit exclusion of signal trumps the deprecated inclusion", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
			},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
				enumspb.RESET_REAPPLY_TYPE_SIGNAL,
			),
		)
	})

	t.Run("Include none with no exclusions => all excluded (honor non-default value of deprecated option in presence of default value of non-deprecated option)", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
			},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{},
				enumspb.RESET_REAPPLY_TYPE_NONE,
			),
		)
	})

	t.Run("Include none with exclude signal is all excluded", func(t *testing.T) {
		t.Parallel()
		require.Equal(t,
			map[enumspb.ResetReapplyExcludeType]struct{}{
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
				enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
			},
			GetResetReapplyExcludeTypes(
				[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
				enumspb.RESET_REAPPLY_TYPE_NONE,
			),
		)
	})
}
