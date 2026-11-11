package scheduler2

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestConfigurationResourceLimitNeighborsAndRecovery(t *testing.T) {
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
					before, physical := cloneNodes(h.durable), len(h.physical)
					s, ctx := h.root(true)
					err := s.UpdateConfiguration(ctx.(chasm.MutableContext), config)
					if delta > 0 {
						require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
						requireRejectedCoordinatorUnchanged(h, before, physical)
						// Retrying the same revision with valid input must succeed: rejection did
						// not consume a revision, damage the source, or strand task publication.
						config = localUpdateConfig(t, 2)
						s, ctx = h.root(true)
						require.NoError(t, s.UpdateConfiguration(ctx.(chasm.MutableContext), config))
					} else {
						require.NoError(t, err)
					}
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
