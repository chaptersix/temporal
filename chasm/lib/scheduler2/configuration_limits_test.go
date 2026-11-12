package scheduler2

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

// Include protobuf tags and length prefixes in the request-size boundary. Identity
// is unconstrained by this local adapter and does not invalidate the workflow ID.
func configurationSizedRequest(t *testing.T, size int) *workflowservice.StartWorkflowExecutionRequest {
	t.Helper()
	request := &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "valid-workflow", Identity: strings.Repeat("x", size)}
	overhead := proto.Size(request) - size
	request.Identity = strings.Repeat("x", size-overhead)
	require.Equal(t, size, proto.Size(request))
	return request
}

func TestConfigurationPrototypeCeilingsRemoved(t *testing.T) {
	t.Parallel()
	for _, resource := range []string{"template bytes", "seed bytes", "catchup nanoseconds"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			for _, delta := range []int{-1, 0, 1} {
				t.Run(fmt.Sprintf("limit%+d", delta), func(t *testing.T) {
					t.Parallel()
					h := manualHarness(t)
					config := localUpdateConfig(t, 2)
					switch resource {
					case "template bytes":
						config.Generation.Request = configurationSizedRequest(t, 64*1024+delta)
					case "seed bytes":
						config.Generation.Seed = strings.Repeat("s", 64*1024+delta)
					case "catchup nanoseconds":
						config.Generation.CatchupWindow = 365*24*time.Hour + time.Duration(delta)
					default:
						t.Fatal("unknown resource")
					}
					s, ctx := h.root(true)
					require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
					commitGeneration(h, true)
					s, ctx = h.root(false)
					require.Equal(t, int64(2), s.Revision)
					require.True(t, s.Coordination.Armed)
					g := s.Generator.Get(ctx)
					require.Equal(t, int64(config.Generation.CatchupWindow), g.CatchupWindow)
					definition := g.Definition.Get(ctx)
					require.Equal(t, config.Generation.Seed, definition.Seed)
					var actual workflowservice.StartWorkflowExecutionRequest
					require.NoError(t, proto.Unmarshal(definition.Request, &actual))
					protorequire.ProtoEqual(t, config.Generation.Request, &actual)
				})
			}
		})
	}
}

func TestConfigurationIdleLimitNeighborsAndRecovery(t *testing.T) {
	t.Parallel()
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("limit%+d", delta), func(t *testing.T) {
			t.Parallel()
			h := idleHarness(t, time.Minute)
			before, physical := cloneNodes(h.durable), len(h.physical)
			duration := 365*24*time.Hour + time.Duration(delta)
			s, ctx := h.root(true)
			anchorNano := s.ActivityAt
			err := s.ConfigureIdle(ctx.(chasm.MutableContext), duration)
			if delta > 0 {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				requireRejectedCoordinatorUnchanged(h, before, physical)
				duration = 365 * 24 * time.Hour
				s, ctx = h.root(true)
				require.NoError(t, s.ConfigureIdle(ctx.(chasm.MutableContext), duration))
			} else {
				require.NoError(t, err)
			}
			commitGeneration(h, true)
			s, _ = h.root(false)
			require.Equal(t, int64(duration), s.IdleDuration)
			require.Equal(t, anchorNano, s.ActivityAt)
			require.Equal(t, anchorNano+int64(duration), s.IdleAt)
			require.True(t, s.IdleArmed)
			require.Equal(t, 1, pureObligations(h))
			h.now = s.IdleAt
			count, err := deliverGeneration(h, time.Unix(0, h.now))
			require.NoError(t, err)
			require.Equal(t, 1, count)
			commitGeneration(h, true)
			s, _ = h.root(false)
			require.True(t, s.Closed)
		})
	}
}

func TestConfigurationLargeInputRetained(t *testing.T) {
	t.Parallel()
	h := manualHarness(t)
	config := localUpdateConfig(t, 2)
	config.Generation.Request.Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: bytes.Repeat([]byte("i"), 2*1024*1024-16*1024)}}}
	expected := proto.Clone(config.Generation.Request)
	require.Greater(t, proto.Size(expected), 64*1024)
	s, ctx := h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	config.Generation.Request.Input.Payloads[0].Data[0] ^= 0xff
	s, ctx = h.root(false)
	var actual workflowservice.StartWorkflowExecutionRequest
	require.NoError(t, proto.Unmarshal(s.Generator.Get(ctx).Definition.Get(ctx).Request, &actual))
	protorequire.ProtoEqual(t, expected, &actual)
}

func TestCatchupDeadlineSaturatesWithoutWrapping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		actual int64
		window time.Duration
		want   int64
	}{
		{"exact", 10, 20, 30},
		{"maximum", 10, time.Duration(math.MaxInt64 - 10), math.MaxInt64},
		{"beyond", 10, time.Duration(math.MaxInt64), math.MaxInt64},
		{"negative_epoch", -10, time.Duration(math.MaxInt64), math.MaxInt64 - 10},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); require.Equal(t, tc.want, catchupDeadline(tc.actual, tc.window)) })
	}
	h := manualHarness(t)
	config := localUpdateConfig(t, 2)
	config.Generation.CatchupWindow = time.Duration(math.MaxInt64)
	s, ctx := h.root(true)
	require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
	commitGeneration(h, true)
	s, ctx = h.root(false)
	require.Equal(t, int64(math.MaxInt64), s.Generator.Get(ctx).CatchupWindow)
}
