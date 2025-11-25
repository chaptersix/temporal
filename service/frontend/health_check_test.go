package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

type (
	healthCheckerTest struct {
		controller *gomock.Controller

		membershipMonitor *membership.MockMonitor
		resolver          *membership.MockServiceResolver

		checker *healthCheckerImpl
	}
)

func setupHealthCheckerTest(t *testing.T) *healthCheckerTest {
	s := &healthCheckerTest{}
	s.controller = gomock.NewController(t)
	s.membershipMonitor = membership.NewMockMonitor(s.controller)
	s.resolver = membership.NewMockServiceResolver(s.controller)
	s.membershipMonitor.EXPECT().GetResolver(gomock.Any()).Return(s.resolver, nil).AnyTimes()

	checker := NewHealthChecker(
		primitives.HistoryService,
		s.membershipMonitor,
		func() float64 {
			return 0.25
		},
		func() float64 {
			return 0.15
		},
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			switch hostAddress {
			case "1", "3":
				return enumsspb.HEALTH_STATE_SERVING, nil
			case "2":
				return enumsspb.HEALTH_STATE_UNSPECIFIED, errors.New("test")
			case "4":
				return enumsspb.HEALTH_STATE_DECLINED_SERVING, nil
			default:
				return enumsspb.HEALTH_STATE_NOT_SERVING, nil
			}
		},
		log.NewNoopLogger(),
	)
	healthChecker, ok := checker.(*healthCheckerImpl)
	if !ok {
		t.Fatal("The constructor did not return correct type")
	}
	s.checker = healthChecker
	return s
}

func Test_Check_Serving(t *testing.T) {
	s := setupHealthCheckerTest(t)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("1"),
	})

	state, err := s.checker.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_SERVING, state)
}

func Test_Check_Not_Serving(t *testing.T) {
	s := setupHealthCheckerTest(t)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("5"),
	})

	state, err := s.checker.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_NOT_SERVING, state)
}

func Test_Check_Declined_Serving(t *testing.T) {
	s := setupHealthCheckerTest(t)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("7"),
	})

	state, err := s.checker.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_DECLINED_SERVING, state)
}

func Test_Check_No_Available_Hosts(t *testing.T) {
	s := setupHealthCheckerTest(t)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{})

	state, err := s.checker.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_NOT_SERVING, state)
}

func Test_Check_GetResolver_Error(t *testing.T) {
	s := setupHealthCheckerTest(t)
	// Create a new checker for this test to avoid conflicting expectations
	membershipMonitor := membership.NewMockMonitor(s.controller)
	membershipMonitor.EXPECT().GetResolver(primitives.HistoryService).Return(nil, errors.New("resolver error"))

	checker := NewHealthChecker(
		primitives.HistoryService,
		membershipMonitor,
		func() float64 { return 0.25 },
		func() float64 { return 0.15 },
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			return enumsspb.HEALTH_STATE_SERVING, nil
		},
		log.NewNoopLogger(),
	)

	state, err := checker.Check(context.Background())
	require.Error(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_UNSPECIFIED, state)
	require.Contains(t, err.Error(), "resolver error")
}

func Test_Check_Boundary_Failure_Percentage_Equals_Threshold(t *testing.T) {
	s := setupHealthCheckerTest(t)
	// Test when failure percentage exactly equals the threshold (0.25)
	// With 4 hosts, 1 failed = 0.25 (25%), should return SERVING since it's not > threshold
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"), // SERVING
		membership.NewHostInfoFromAddress("2"), // UNSPECIFIED (failed)
		membership.NewHostInfoFromAddress("3"), // SERVING
		membership.NewHostInfoFromAddress("1"), // SERVING
	})

	state, err := s.checker.Check(context.Background())
	require.NoError(t, err)
	require.Equal(t, enumsspb.HEALTH_STATE_SERVING, state)
}

func Test_Check_Single_Host_Scenarios(t *testing.T) {
	testCases := []struct {
		name          string
		hostAddress   string
		expectedState enumsspb.HealthState
	}{
		{
			name:          "single host serving",
			hostAddress:   "1", // SERVING
			expectedState: enumsspb.HEALTH_STATE_SERVING,
		},
		{
			name:          "single host failed",
			hostAddress:   "2", // UNSPECIFIED (failed)
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			name:          "single host declined serving",
			hostAddress:   "4",                               // DECLINED_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING, // Combined logic: 0% failed + 100% declined = 100% > 25% threshold
		},
		{
			name:          "single host not serving",
			hostAddress:   "5", // NOT_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupHealthCheckerTest(t)
			s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
				membership.NewHostInfoFromAddress(tc.hostAddress),
			})

			state, err := s.checker.Check(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.expectedState, state)
		})
	}
}

func Test_Check_Context_Cancellation(t *testing.T) {
	s := setupHealthCheckerTest(t)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
	})

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Create a checker with a health check function that respects context cancellation
	checker := NewHealthChecker(
		primitives.HistoryService,
		s.membershipMonitor,
		func() float64 { return 0.25 },
		func() float64 { return 0.15 },
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			select {
			case <-ctx.Done():
				return enumsspb.HEALTH_STATE_UNSPECIFIED, ctx.Err()
			default:
				return enumsspb.HEALTH_STATE_SERVING, nil
			}
		},
		log.NewNoopLogger(),
	)

	state, err := checker.Check(ctx)
	require.NoError(t, err)                                   // Context cancellation in individual health checks should not fail the overall check
	require.Equal(t, enumsspb.HEALTH_STATE_NOT_SERVING, state) // All hosts will return UNSPECIFIED due to cancellation
}

func Test_Check_Mixed_Host_States_Edge_Cases(t *testing.T) {
	testCases := []struct {
		name          string
		hosts         []string
		expectedState enumsspb.HealthState
		description   string
	}{
		{
			name:          "edge case: 50% declined serving equals minimum threshold",
			hosts:         []string{"4", "4", "1", "1"},      // 2 declined, 2 serving out of 4
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING, // Combined: 0% failed + 50% declined = 50% > 25% threshold
			description:   "50% declined serving triggers combined failure threshold, returns NOT_SERVING",
		},
		{
			name:          "edge case: 60% declined serving exceeds minimum threshold",
			hosts:         []string{"4", "4", "4", "1", "1"},      // 3 declined, 2 serving out of 5
			expectedState: enumsspb.HEALTH_STATE_DECLINED_SERVING, // 60% > 40% minimum threshold
			description:   "60% declined serving should trigger DECLINED_SERVING response",
		},
		{
			name:          "edge case: mixed failures just under threshold",
			hosts:         []string{"2", "1", "1", "1", "1"}, // 1 failed (20%), 4 serving (80%) out of 5
			expectedState: enumsspb.HEALTH_STATE_SERVING,     // 20% < 25% threshold
			description:   "20% failures should still return SERVING",
		},
		{
			name:          "edge case: combined failures and declined just over threshold",
			hosts:         []string{"2", "4", "1", "1"}, // 1 failed (25%) + 1 declined (25%) = 50% > 25% threshold
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
			description:   "Combined 50% failures and declined serving should trigger NOT_SERVING",
		},
		{
			name:          "edge case: all declined serving with many hosts",
			hosts:         []string{"4", "4", "4", "4", "4"}, // All declined serving
			expectedState: enumsspb.HEALTH_STATE_DECLINED_SERVING,
			description:   "100% declined serving should return DECLINED_SERVING",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupHealthCheckerTest(t)
			hostInfos := make([]membership.HostInfo, len(tc.hosts))
			for i, host := range tc.hosts {
				hostInfos[i] = membership.NewHostInfoFromAddress(host)
			}
			s.resolver.EXPECT().AvailableMembers().Return(hostInfos)

			state, err := s.checker.Check(context.Background())
			require.NoError(t, err, tc.description)
			require.Equal(t, tc.expectedState, state, tc.description)
		})
	}
}

func Test_GetProportionOfNotReadyHosts(t *testing.T) {
	testCases := []struct {
		name                             string
		proportionOfDeclinedServingHosts float64
		totalHosts                       int
		expectedProportion               float64
	}{
		{
			name:                             "zero proportion",
			proportionOfDeclinedServingHosts: 0.0,
			totalHosts:                       10,
			expectedProportion:               0.2,
		},
		{
			name:                             "small proportion with few hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       10,
			expectedProportion:               0.2, // 2/10 = 0.2 since numHostsToFail < 2
		},
		{
			name:                             "small proportion with many hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       100,
			expectedProportion:               0.1, // 10 hosts > 2, so use original proportion
		},
		{
			name:                             "large proportion",
			proportionOfDeclinedServingHosts: 0.8,
			totalHosts:                       10,
			expectedProportion:               0.8, // 8 hosts > 2, so use original proportion
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proportion := ensureMinimumProportionOfHosts(tc.proportionOfDeclinedServingHosts, tc.totalHosts)
			require.Equal(t, tc.expectedProportion, proportion)
		})
	}
}
