//go:build matching_memory_probe

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

func TestMatchingMemoryProbe(t *testing.T) {
	testcontext.For(t, testcontext.WithTimeout(17*time.Minute))
	output := os.Getenv("MATCHING_MEMORY_OUTPUT")
	require.NotEmpty(t, output)
	require.NoError(t, os.MkdirAll(output, 0o755))
	partitions := 4
	if value := os.Getenv("MATCHING_MEMORY_PARTITIONS"); value != "" {
		var err error
		partitions, err = strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, partitions)
	}
	options := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, partitions),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, partitions),
	}
	maxIdleTime := 5 * time.Minute
	longPollExpiration := time.Minute
	userDataLongPollTimeout := 5*time.Minute - 10*time.Second
	if os.Getenv("MATCHING_MEMORY_SHORT") != "" {
		maxIdleTime = 30 * time.Second
		longPollExpiration = 5 * time.Second
		userDataLongPollTimeout = 5 * time.Second
	}
	options = append(options,
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxTaskQueueIdleTime, maxIdleTime),
		testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, longPollExpiration),
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetUserDataLongPollTimeout, userDataLongPollTimeout),
	)
	env := testcore.NewEnv(t, options...)
	client := env.SdkClient()
	started := time.Now()
	sample := func(phase string) int {
		// Compare retained heap after collection at each sampling point.
		//nolint:revive // This opt-in probe explicitly measures live memory.
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		var profile bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&profile, 1))
		counts := make(map[string]int)
		matching := 0
		_, stacks, _ := strings.Cut(profile.String(), "\n")
		for _, block := range strings.Split(stacks, "\n\n") {
			lines := strings.Split(block, "\n")
			if len(lines) < 2 || !strings.Contains(lines[0], " @ ") {
				continue
			}
			count, err := strconv.Atoi(strings.Fields(lines[0])[0])
			require.NoError(t, err)
			for _, line := range lines[1:] {
				if _, after, ok := strings.Cut(line, "go.temporal.io/server/service/matching."); ok {
					name := strings.SplitN(after, "+", 2)[0]
					counts[name] += count
					matching += count
					break
				}
			}
		}
		smaps, err := os.ReadFile("/proc/self/smaps_rollup")
		require.NoError(t, err)
		row := map[string]any{
			"phase": phase, "elapsed_seconds": time.Since(started).Seconds(),
			"goroutines": runtime.NumGoroutine(), "matching_goroutines": matching,
			"matching_stacks": counts, "heap_alloc": mem.HeapAlloc,
			"heap_inuse": mem.HeapInuse, "stack_inuse": mem.StackInuse,
			"smaps_rollup": string(smaps),
		}
		data, err := json.MarshalIndent(row, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, phase+".json"), data, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(output, phase+"-goroutines.txt"), profile.Bytes(), 0o644))
		t.Logf("MEMORY phase=%s elapsed=%.1fs matching=%d total=%d heap=%.1fMiB", phase,
			time.Since(started).Seconds(), matching, runtime.NumGoroutine(), float64(mem.HeapAlloc)/(1<<20))
		return matching
	}
	baseline := sample("baseline")
	t.Run("workload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		activityFn := func(context.Context) error { return nil }
		workflowFn := func(ctx workflow.Context) error {
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
			return workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
		}
		for i := range 12 {
			queue := fmt.Sprintf("memory-probe-%02d", i)
			w := worker.New(client, queue, worker.Options{
				MaxConcurrentWorkflowTaskPollers: 8,
				MaxConcurrentActivityTaskPollers: 8,
			})
			w.RegisterWorkflow(workflowFn)
			w.RegisterActivity(activityFn)
			require.NoError(t, w.Start())
			t.Cleanup(func() { w.Stop(); w = nil })
			run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID: queue, TaskQueue: queue,
			}, workflowFn)
			require.NoError(t, err)
			require.NoError(t, run.Get(ctx, nil))
		}
		sample("workers-running")
	})
	sample("workers-stopped")
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	observeFor := 16 * time.Minute
	if value := os.Getenv("MATCHING_MEMORY_OBSERVE_SECONDS"); value != "" {
		seconds, err := strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, seconds)
		observeFor = time.Duration(seconds) * time.Second
	}
	deadline := time.NewTimer(observeFor)
	defer deadline.Stop()
	for index := 1; ; index++ {
		select {
		case <-ticker.C:
			if sample(fmt.Sprintf("idle-%03d", index)) <= baseline {
				sample("evicted")
				return
			}
		case <-deadline.C:
			if os.Getenv("MATCHING_MEMORY_OBSERVE_SECONDS") != "" {
				sample("observation-ended")
				return
			}
			t.Fatal("matching goroutines did not return to baseline within 16 minutes")
		case <-t.Context().Done():
			t.Fatal("test canceled before matching goroutines returned to baseline")
		}
	}
}
