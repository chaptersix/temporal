package tdbg

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/tools/schedutil"
)

// ScheduleDedup deduplicates StructuredCalendar entries in a schedule spec.
// Without --execute it writes before/after JSON to a temp directory and exits.
// With --recreate it reads from workflow history instead of describing the schedule,
// then deletes and recreates it — use when the workflow is too degraded to process an update.
func ScheduleDedup(c *cli.Context, clientFactory ClientFactory) error {
	outDir, err := os.MkdirTemp("", "schedutil-*")
	if err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	fmt.Printf("Output directory: %s\n\n", outDir)

	ns := c.String(FlagNamespace)
	execute := c.Bool(FlagExecute)
	recreate := c.Bool(FlagRecreate)
	cl := clientFactory.WorkflowClient(c)

	// Single-schedule: one context for the whole operation.
	if sid := c.String(FlagScheduleID); sid != "" {
		ctx, cancel := newContext(c)
		defer cancel()
		if recreate {
			return schedutil.RunDedupRecreate(ctx, cl, ns, sid, outDir, execute)
		}
		return schedutil.RunDedup(ctx, cl, ns, sid, outDir, execute)
	}

	// Namespace-wide: use a background context for listing so the session is
	// not cut short, and give each schedule its own per-operation timeout.
	fn := func(sid string) error {
		ctx, cancel := newContext(c)
		defer cancel()
		if recreate {
			return schedutil.RunDedupRecreate(ctx, cl, ns, sid, outDir, execute)
		}
		return schedutil.RunDedup(ctx, cl, ns, sid, outDir, execute)
	}
	return schedutil.ForEachSchedule(context.Background(), cl, ns, fn)
}

// ScheduleForceCAN sends a force-continue-as-new signal to the scheduler workflow.
// Without --execute it prints what would be signalled and exits.
func ScheduleForceCAN(c *cli.Context, clientFactory ClientFactory) error {
	ns := c.String(FlagNamespace)
	execute := c.Bool(FlagExecute)
	cl := clientFactory.WorkflowClient(c)

	// Single-schedule: one context for the whole operation.
	if sid := c.String(FlagScheduleID); sid != "" {
		ctx, cancel := newContext(c)
		defer cancel()
		return schedutil.RunForceCAN(ctx, cl, ns, sid, execute)
	}

	// Namespace-wide: per-operation timeout per schedule.
	return schedutil.ForEachSchedule(context.Background(), cl, ns, func(sid string) error {
		ctx, cancel := newContext(c)
		defer cancel()
		return schedutil.RunForceCAN(ctx, cl, ns, sid, execute)
	})
}
