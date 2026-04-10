// schedutil provides two operations for managing individual schedules or entire
// namespaces.
//
// # Targeting
//
//   - --schedule-id <id>   operate on a single schedule
//   - stdin pipe           read one ID per line from piped input
//   - (neither)            operate on all schedules in the namespace
//
// # Dry-run vs execute
//
// Without --execute the command describes each schedule, writes before/after
// JSON files to a temp directory, and exits without applying any changes.
// With --execute the changes are applied and the same files are written.
//
// # Commands
//
//	dedup      Deduplicate StructuredCalendar and Interval entries.
//	force-can  Send a force-continue-as-new signal to the scheduler workflow.
//
// # Examples
//
//	# Single schedule — dry run (writes JSON, no update sent)
//	schedutil -namespace prod dedup --schedule-id my-sched
//
//	# Single schedule — apply
//	schedutil -namespace prod dedup --schedule-id my-sched --execute
//
//	# Subset piped from another tool
//	temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
//	    schedutil -namespace prod dedup --execute
//
//	# Whole namespace — dry run first, then execute
//	schedutil -namespace prod dedup
//	schedutil -namespace prod dedup --execute
//
// Global flags mirror tdbg (address, namespace, TLS, context-timeout) and
// honour the same environment variables (TEMPORAL_CLI_ADDRESS, etc.).
package schedutil

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tools/tdbg"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultContextTimeoutSeconds = 30
	flagExecute                  = "execute"
	flagRecreate                 = "recreate"
)

// Run is the entry point for the schedutil tool.
func Run(args []string) error {
	app := cli.NewApp()
	app.Name = "schedutil"
	app.Usage = "Operations on individual Temporal schedules"
	app.Description = `Without --schedule-id, reads IDs from stdin if piped, otherwise targets all
schedules in the namespace. Without --execute, writes before/after JSON to a
temp directory and exits without applying changes.

  # Single schedule
  schedutil -namespace prod dedup --schedule-id my-sched
  schedutil -namespace prod dedup --schedule-id my-sched --execute

  # Subset via pipe
  temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
      schedutil -namespace prod dedup --execute

  # Whole namespace
  schedutil -namespace prod dedup
  schedutil -namespace prod dedup --execute

  # Force CAN
  schedutil -namespace prod force-can --schedule-id my-sched --execute`
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    tdbg.FlagAddress,
			Value:   "",
			Usage:   "host:port for Temporal frontend service",
			EnvVars: []string{"TEMPORAL_CLI_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    tdbg.FlagNamespace,
			Aliases: tdbg.FlagNamespaceAlias,
			Value:   "default",
			Usage:   "Temporal workflow namespace",
			EnvVars: []string{"TEMPORAL_CLI_NAMESPACE"},
		},
		&cli.IntFlag{
			Name:    tdbg.FlagContextTimeout,
			Aliases: tdbg.FlagContextTimeoutAlias,
			Value:   defaultContextTimeoutSeconds,
			Usage:   "Timeout for RPC calls in seconds",
			EnvVars: []string{"TEMPORAL_CONTEXT_TIMEOUT"},
		},
		&cli.StringFlag{
			Name:    tdbg.FlagTLSCertPath,
			Value:   "",
			Usage:   "Path to x509 certificate",
			EnvVars: []string{"TEMPORAL_CLI_TLS_CERT"},
		},
		&cli.StringFlag{
			Name:    tdbg.FlagTLSKeyPath,
			Value:   "",
			Usage:   "Path to private key",
			EnvVars: []string{"TEMPORAL_CLI_TLS_KEY"},
		},
		&cli.StringFlag{
			Name:    tdbg.FlagTLSCaPath,
			Value:   "",
			Usage:   "Path to server CA certificate",
			EnvVars: []string{"TEMPORAL_CLI_TLS_CA"},
		},
		&cli.BoolFlag{
			Name:    tdbg.FlagTLSDisableHostVerification,
			Usage:   "Disable TLS host name verification",
			EnvVars: []string{"TEMPORAL_CLI_TLS_DISABLE_HOST_VERIFICATION"},
		},
		&cli.StringFlag{
			Name:    tdbg.FlagTLSServerName,
			Value:   "",
			Usage:   "Override for target server name",
			EnvVars: []string{"TEMPORAL_CLI_TLS_SERVER_NAME"},
		},
	}
	scheduleIDFlag := &cli.StringFlag{
		Name:    tdbg.FlagScheduleID,
		Aliases: []string{"s"},
		Usage:   "Schedule ID. Omit to read IDs from stdin (if piped) or target all schedules in the namespace.",
	}
	executeFlag := &cli.BoolFlag{
		Name:  flagExecute,
		Usage: "Apply changes. Without this flag the command runs in dry-run mode: describes each schedule, writes before/after JSON, and exits without modifying anything.",
	}
	app.Commands = []*cli.Command{
		{
			Name:  "dedup",
			Usage: "Deduplicate StructuredCalendar entries in a schedule spec",
			Flags: []cli.Flag{
				scheduleIDFlag,
				executeFlag,
				&cli.BoolFlag{
					Name:  flagRecreate,
					Usage: "Read schedule state from workflow history, deduplicate, then delete and recreate the schedule. Use when the workflow is too degraded to process an update.",
				},
			},
			Action: func(c *cli.Context) error {
				outDir, err := os.MkdirTemp("", "schedutil-*")
				if err != nil {
					return fmt.Errorf("create output dir: %w", err)
				}
				fmt.Printf("Output directory: %s\n\n", outDir)
				ns := c.String(tdbg.FlagNamespace)
				execute := c.Bool(flagExecute)
				recreate := c.Bool(flagRecreate)
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					fn := func(sid string) error {
						if recreate {
							return RunDedupRecreate(ctx, cl, ns, sid, outDir, execute)
						}
						return RunDedup(ctx, cl, ns, sid, outDir, execute)
					}
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return fn(sid)
					}
					return ForEachSchedule(ctx, cl, ns, fn)
				})
			},
		},
		{
			Name:  "force-can",
			Usage: "Send a force-continue-as-new signal to the scheduler workflow",
			Flags: []cli.Flag{scheduleIDFlag, executeFlag},
			Action: func(c *cli.Context) error {
				ns := c.String(tdbg.FlagNamespace)
				execute := c.Bool(flagExecute)
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return RunForceCAN(ctx, cl, sid, execute)
					}
					return ForEachSchedule(ctx, cl, ns, func(sid string) error {
						return RunForceCAN(ctx, cl, sid, execute)
					})
				})
			},
		},
	}
	return app.Run(append([]string{"schedutil"}, args...))
}

// withClient creates an SDK client from the CLI flags and calls fn with an
// unbounded context. The session itself is not deadline-bounded so bulk
// namespace operations are not cut off mid-run.
func withClient(c *cli.Context, fn func(context.Context, sdkclient.Client) error) error {
	address := c.String(tdbg.FlagAddress)
	if address == "" {
		address = tdbg.DefaultFrontendAddress
	}

	tlsCfg, err := buildTLSConfig(c, address)
	if err != nil {
		return fmt.Errorf("TLS config: %w", err)
	}

	cl, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  address,
		Namespace: c.String(tdbg.FlagNamespace),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS: tlsCfg,
		},
	})
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	defer cl.Close()

	return fn(context.Background(), cl)
}

// RunDedup describes the schedule, writes before/after JSON files to outDir,
// and (if execute) sends an UpdateSchedule with duplicates removed. The files
// are written regardless of the execute flag so dry-run output is verifiable.
func RunDedup(ctx context.Context, cl sdkclient.Client, namespace, scheduleID, outDir string, execute bool) error {
	handle := cl.ScheduleClient().GetHandle(ctx, scheduleID)

	desc, err := handle.Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe schedule: %w", err)
	}

	beforeJSON, err := json.MarshalIndent(desc.Schedule, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}
	key := fileKey(namespace, scheduleID)
	beforePath := outDir + "/" + key + "-before.json"
	if err := os.WriteFile(beforePath, beforeJSON, 0o644); err != nil {
		return fmt.Errorf("write before: %w", err)
	}

	// Compute the deduplicated schedule and write the after file. The DoUpdate
	// closure captures the result so we can write it before deciding whether
	// to actually send the update.
	sched := desc.Schedule
	sched.Spec.Calendars = deduplicateCalendars(sched.Spec.Calendars)
	sched.Spec.Intervals = deduplicateIntervals(sched.Spec.Intervals)

	afterJSON, err := json.MarshalIndent(sched, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal deduped schedule: %w", err)
	}
	afterPath := outDir + "/" + key + "-after.json"
	if err := os.WriteFile(afterPath, afterJSON, 0o644); err != nil {
		return fmt.Errorf("write after spec: %w", err)
	}

	nCalBefore := len(desc.Schedule.Spec.Calendars)
	nCalAfter := len(sched.Spec.Calendars)
	nIntBefore := len(desc.Schedule.Spec.Intervals)
	nIntAfter := len(sched.Spec.Intervals)

	if !execute {
		fmt.Printf("  %s: %d→%d calendars, %d→%d intervals (dry run)\n",
			scheduleID, nCalBefore, nCalAfter, nIntBefore, nIntAfter)
		fmt.Printf("    before: %s\n", beforePath)
		fmt.Printf("    after:  %s\n", afterPath)
		return nil
	}

	err = handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
		DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
			return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("update schedule: %w", err)
	}
	fmt.Printf("  %s: %d→%d calendars, %d→%d intervals\n",
		scheduleID, nCalBefore, nCalAfter, nIntBefore, nIntAfter)
	fmt.Printf("    before: %s\n", beforePath)
	fmt.Printf("    after:  %s\n", afterPath)
	return nil
}

// RunForceCAN sends (or in dry-run mode, prints) a force-continue-as-new
// signal for the scheduler workflow of the given schedule ID.
func RunForceCAN(ctx context.Context, cl sdkclient.Client, scheduleID string, execute bool) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	if !execute {
		fmt.Printf("  %s: would signal %q (dry run)\n", scheduleID, scheduler.SignalNameForceCAN)
		return nil
	}
	if err := cl.SignalWorkflow(ctx, workflowID, "", scheduler.SignalNameForceCAN, nil); err != nil {
		return fmt.Errorf("signal workflow: %w", err)
	}
	fmt.Printf("  %s: signalled %q\n", scheduleID, scheduler.SignalNameForceCAN)
	return nil
}

// RunDedupRecreate reads the schedule state from workflow history (bypassing the
// query size limit), deduplicates the spec, and (if execute) deletes the broken
// schedule and recreates it with the clean spec. Use when the workflow is too
// degraded to process an update signal.
func RunDedupRecreate(ctx context.Context, cl sdkclient.Client, namespace, scheduleID, outDir string, execute bool) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	resp, err := cl.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		MaximumPageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("get workflow history: %w", err)
	}
	if len(resp.History.Events) == 0 {
		return fmt.Errorf("no history events found for %s", workflowID)
	}
	attrs := resp.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	if attrs == nil {
		return fmt.Errorf("first event is not WorkflowExecutionStarted")
	}

	var args schedulespb.StartScheduleArgs
	if err := payloads.Decode(attrs.Input, &args); err != nil {
		return fmt.Errorf("decode StartScheduleArgs: %w", err)
	}

	beforeJSON, err := protojson.MarshalOptions{Multiline: true}.Marshal(args.Schedule)
	if err != nil {
		return fmt.Errorf("marshal before schedule: %w", err)
	}
	key := fileKey(namespace, scheduleID)
	beforePath := outDir + "/" + key + "-before.json"
	if err := os.WriteFile(beforePath, beforeJSON, 0o644); err != nil {
		return fmt.Errorf("write before: %w", err)
	}

	spec := args.Schedule.Spec
	nCalBefore := len(spec.StructuredCalendar)
	nIntBefore := len(spec.Interval)
	spec.StructuredCalendar = deduplicateStructuredCalendarsProto(spec.StructuredCalendar)
	spec.Interval = deduplicateIntervalsProto(spec.Interval)
	nCalAfter := len(spec.StructuredCalendar)
	nIntAfter := len(spec.Interval)

	afterJSON, err := protojson.MarshalOptions{Multiline: true}.Marshal(args.Schedule)
	if err != nil {
		return fmt.Errorf("marshal after schedule: %w", err)
	}
	afterPath := outDir + "/" + key + "-after.json"
	if err := os.WriteFile(afterPath, afterJSON, 0o644); err != nil {
		return fmt.Errorf("write after: %w", err)
	}

	if !execute {
		fmt.Printf("  %s: %d→%d calendars, %d→%d intervals (dry run, would recreate)\n",
			scheduleID, nCalBefore, nCalAfter, nIntBefore, nIntAfter)
		fmt.Printf("    before: %s\n", beforePath)
		fmt.Printf("    after:  %s\n", afterPath)
		return nil
	}

	if _, err := cl.WorkflowService().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
		Identity:   "schedutil",
	}); err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if _, err := cl.WorkflowService().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
		Schedule:   args.Schedule,
		Identity:   "schedutil",
		RequestId:  uuid.NewString(),
	}); err != nil {
		return fmt.Errorf("recreate schedule: %w", err)
	}
	fmt.Printf("  %s: %d→%d calendars, %d→%d intervals (recreated)\n",
		scheduleID, nCalBefore, nCalAfter, nIntBefore, nIntAfter)
	fmt.Printf("    before: %s\n", beforePath)
	fmt.Printf("    after:  %s\n", afterPath)
	return nil
}

// deduplicateStructuredCalendarsProto removes duplicate StructuredCalendarSpec
// entries using proto.Equal. Entries read from workflow history are in identical
// wire form so no normalization is needed before comparison.
func deduplicateStructuredCalendarsProto(entries []*schedulepb.StructuredCalendarSpec) []*schedulepb.StructuredCalendarSpec {
	out := make([]*schedulepb.StructuredCalendarSpec, 0, len(entries))
	for _, e := range entries {
		duplicate := false
		for _, seen := range out {
			if proto.Equal(e, seen) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, e)
		}
	}
	return out
}

// deduplicateIntervalsProto removes duplicate IntervalSpec entries using proto.Equal.
func deduplicateIntervalsProto(entries []*schedulepb.IntervalSpec) []*schedulepb.IntervalSpec {
	out := make([]*schedulepb.IntervalSpec, 0, len(entries))
	for _, e := range entries {
		duplicate := false
		for _, seen := range out {
			if proto.Equal(e, seen) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, e)
		}
	}
	return out
}

// ForEachSchedule resolves the schedule ID set and calls fn for each,
// continuing on per-schedule errors and reporting a combined failure at the end.
// If stdin is a pipe, IDs are read from it one per line; otherwise all
// schedules in the namespace are listed.
func ForEachSchedule(ctx context.Context, cl sdkclient.Client, namespace string, fn func(string) error) error {
	var scheduleIDs []string
	if stdinIsPipe() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			scheduleIDs = append(scheduleIDs, line)
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
	} else {
		iter, err := cl.ScheduleClient().List(ctx, sdkclient.ScheduleListOptions{})
		if err != nil {
			return fmt.Errorf("list schedules: %w", err)
		}
		for iter.HasNext() {
			entry, err := iter.Next()
			if err != nil {
				return fmt.Errorf("list schedules: %w", err)
			}
			scheduleIDs = append(scheduleIDs, entry.ID)
		}
	}

	if len(scheduleIDs) == 0 {
		fmt.Printf("No schedules found in namespace %q.\n", namespace)
		return nil
	}

	fmt.Printf("Processing %d schedule(s) in namespace %q:\n", len(scheduleIDs), namespace)
	var failed []string
	for _, sid := range scheduleIDs {
		if err := fn(sid); err != nil {
			fmt.Printf("  ERROR %s: %v\n", sid, err)
			failed = append(failed, sid)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d schedule(s) failed: %v", len(failed), failed)
	}
	return nil
}

// fileKey returns a filesystem-safe key for a namespace+scheduleID pair.
func fileKey(namespace, scheduleID string) string {
	sanitize := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if r == '-' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				b.WriteRune(r)
			} else {
				b.WriteRune('_')
			}
		}
		return b.String()
	}
	return sanitize(namespace) + "_" + sanitize(scheduleID)
}

// deduplicateCalendars removes duplicate ScheduleCalendarSpec entries,
// preserving first occurrence. ScheduleCalendarSpec contains slice fields so
// it is not directly comparable; reflect.DeepEqual is used instead.
func deduplicateCalendars(entries []sdkclient.ScheduleCalendarSpec) []sdkclient.ScheduleCalendarSpec {
	out := make([]sdkclient.ScheduleCalendarSpec, 0, len(entries))
	for _, e := range entries {
		duplicate := false
		for _, seen := range out {
			if reflect.DeepEqual(e, seen) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, e)
		}
	}
	return out
}

// deduplicateIntervals removes duplicate ScheduleIntervalSpec entries,
// preserving first occurrence.
func deduplicateIntervals(entries []sdkclient.ScheduleIntervalSpec) []sdkclient.ScheduleIntervalSpec {
	seen := make(map[sdkclient.ScheduleIntervalSpec]struct{}, len(entries))
	out := make([]sdkclient.ScheduleIntervalSpec, 0, len(entries))
	for _, e := range entries {
		if _, ok := seen[e]; !ok {
			seen[e] = struct{}{}
			out = append(out, e)
		}
	}
	return out
}

// stdinIsPipe reports whether stdin is connected to a pipe or redirect rather
// than an interactive terminal.
func stdinIsPipe() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice == 0
}

// buildTLSConfig mirrors tdbg's createTLSConfig: reads cert/key/CA flags and
// constructs a *tls.Config, or returns nil if no TLS flags are set.
func buildTLSConfig(c *cli.Context, address string) (*tls.Config, error) {
	certPath := c.String(tdbg.FlagTLSCertPath)
	keyPath := c.String(tdbg.FlagTLSKeyPath)
	caPath := c.String(tdbg.FlagTLSCaPath)
	disableHostVerification := c.Bool(tdbg.FlagTLSDisableHostVerification)
	serverName := c.String(tdbg.FlagTLSServerName)

	var cert *tls.Certificate
	var caPool *x509.CertPool

	if caPath != "" {
		pool, err := fetchCACert(caPath)
		if err != nil {
			return nil, fmt.Errorf("load CA cert: %w", err)
		}
		caPool = pool
	}
	if certPath != "" {
		myCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cert = &myCert
	}

	if caPool != nil || cert != nil {
		host := serverName
		if host == "" {
			h, _, _ := net.SplitHostPort(address)
			host = h
		}
		cfg := auth.NewTLSConfigForServer(host, !disableHostVerification)
		if caPool != nil {
			cfg.RootCAs = caPool
		}
		if cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
		return cfg, nil
	}
	if serverName != "" {
		return auth.NewTLSConfigForServer(serverName, !disableHostVerification), nil
	}
	return nil, nil
}

// fetchCACert loads a PEM CA certificate from a file path or HTTPS URL,
// mirroring tdbg's fetchCACert.
func fetchCACert(pathOrURL string) (*x509.CertPool, error) {
	if strings.HasPrefix(pathOrURL, "http://") {
		return nil, errors.New("HTTP is not supported for CA cert URLs; use HTTPS")
	}

	var caBytes []byte
	var err error
	if strings.HasPrefix(pathOrURL, "https://") {
		var resp *http.Response
		resp, err = http.Get(pathOrURL) //nolint:noctx
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		caBytes, err = io.ReadAll(resp.Body)
	} else {
		caBytes, err = os.ReadFile(pathOrURL)
	}
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("failed to parse CA certificate")
	}
	return pool, nil
}
