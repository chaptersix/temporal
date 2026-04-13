package tdbg

import (
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

	ctx, cancel := newContext(c)
	defer cancel()

	fn := func(sid string) error {
		if recreate {
			return schedutil.RunDedupRecreate(ctx, cl, ns, sid, outDir, execute)
		}
		return schedutil.RunDedup(ctx, cl, ns, sid, outDir, execute)
	}
	if sid := c.String(FlagScheduleID); sid != "" {
		return fn(sid)
	}
	return schedutil.ForEachSchedule(ctx, cl, ns, fn)
}

// ScheduleForceCAN sends a force-continue-as-new signal to the scheduler workflow.
// Without --execute it prints what would be signalled and exits.
func ScheduleForceCAN(c *cli.Context, clientFactory ClientFactory) error {
	ns := c.String(FlagNamespace)
	execute := c.Bool(FlagExecute)
	cl := clientFactory.WorkflowClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	if sid := c.String(FlagScheduleID); sid != "" {
		return schedutil.RunForceCAN(ctx, cl, ns, sid, execute)
	}
	return schedutil.ForEachSchedule(ctx, cl, ns, func(sid string) error {
		return schedutil.RunForceCAN(ctx, cl, ns, sid, execute)
	})
}
