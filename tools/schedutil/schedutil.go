// schedutil provides two operations for managing individual schedules or entire
// namespaces.
//
// # Targeting
//
// Exactly one of three targeting modes must be used:
//
//	--schedule-id <id>     operate on a single schedule
//	--ids-file <file>      read IDs from a file, one per line ("-" for stdin)
//	(neither)              operate on all schedules in the namespace
//
// # Dry-run vs execute
//
// Without --execute the command always describes each schedule, writes before/
// after JSON files to a temp directory, and exits without applying any changes.
// With --execute the changes are applied and the same files are written.
//
// # Commands
//
//	dedup      Deduplicate StructuredCalendar and Interval entries in a
//	           schedule spec. Writes before/after JSON to a temp directory.
//	force-can  Send a force-continue-as-new signal to the scheduler workflow.
//	           Dry-run prints what would be signalled; --execute sends it.
//
// # Examples
//
//	# Single schedule — dry run (writes files, no update sent)
//	schedutil -namespace prod dedup --schedule-id my-sched
//
//	# Single schedule — apply
//	schedutil -namespace prod dedup --schedule-id my-sched --execute
//
//	# Explicit list from a file
//	schedutil -namespace prod dedup --ids-file affected.txt --execute
//
//	# Piped from another tool ("-" reads stdin)
//	temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
//	    schedutil -namespace prod dedup --ids-file - --execute
//
//	# Whole namespace — dry run first, then execute
//	schedutil -namespace prod dedup
//	schedutil -namespace prod dedup --execute
//
//	# Force CAN on a single schedule
//	schedutil -namespace prod force-can --schedule-id my-sched --execute
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

	"github.com/urfave/cli/v2"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tools/tdbg"
)

const (
	defaultContextTimeoutSeconds = 30
	flagIDsFile                  = "ids-file"
	flagExecute                  = "execute"
)

// Run is the entry point for the schedutil tool.
func Run(args []string) error {
	app := cli.NewApp()
	app.Name = "schedutil"
	app.Usage = "Operations on individual Temporal schedules"
	app.Description = `Target a single schedule, a file of IDs, or an entire namespace.

Without --execute the command describes each schedule, writes before/after JSON
to a temp directory, and exits without applying any changes.

  # Dry run (writes files, no changes applied)
  schedutil -namespace prod dedup --schedule-id my-sched
  schedutil -namespace prod dedup --ids-file affected.txt
  schedutil -namespace prod dedup

  # Apply
  schedutil -namespace prod dedup --schedule-id my-sched --execute
  schedutil -namespace prod dedup --ids-file affected.txt --execute
  schedutil -namespace prod dedup --execute

  # Piped ("-" reads stdin)
  temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
      schedutil -namespace prod dedup --ids-file - --execute

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
		Usage:   "Schedule ID. Omit to target all schedules in the namespace.",
	}
	idsFileFlag := &cli.StringFlag{
		Name:  flagIDsFile,
		Usage: `File of schedule IDs, one per line. Use "-" to read from stdin. Mutually exclusive with --schedule-id.`,
	}
	executeFlag := &cli.BoolFlag{
		Name:  flagExecute,
		Usage: "Apply changes. Without this flag the command runs in dry-run mode: describes each schedule, writes before/after JSON, and exits without modifying anything.",
	}
	app.Commands = []*cli.Command{
		{
			Name:  "dedup",
			Usage: "Deduplicate StructuredCalendar entries in a schedule spec",
			Flags: []cli.Flag{scheduleIDFlag, idsFileFlag, executeFlag},
			Action: func(c *cli.Context) error {
				outDir, err := os.MkdirTemp("", "schedutil-*")
				if err != nil {
					return fmt.Errorf("create output dir: %w", err)
				}
				fmt.Printf("Output directory: %s\n\n", outDir)
				ns := c.String(tdbg.FlagNamespace)
				execute := c.Bool(flagExecute)
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return RunDedup(ctx, cl, ns, sid, outDir, execute)
					}
					return ForEachSchedule(ctx, cl, ns, c.String(flagIDsFile), func(sid string) error {
						return RunDedup(ctx, cl, ns, sid, outDir, execute)
					})
				})
			},
		},
		{
			Name:  "force-can",
			Usage: "Send a force-continue-as-new signal to the scheduler workflow",
			Flags: []cli.Flag{scheduleIDFlag, idsFileFlag, executeFlag},
			Action: func(c *cli.Context) error {
				ns := c.String(tdbg.FlagNamespace)
				execute := c.Bool(flagExecute)
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return RunForceCAN(ctx, cl, sid, execute)
					}
					return ForEachSchedule(ctx, cl, ns, c.String(flagIDsFile), func(sid string) error {
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

	beforeJSON, err := json.MarshalIndent(desc.Schedule.Spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	key := fileKey(namespace, scheduleID)
	beforePath := outDir + "/" + key + "-before.json"
	if err := os.WriteFile(beforePath, beforeJSON, 0o644); err != nil {
		return fmt.Errorf("write before spec: %w", err)
	}

	// Compute the deduplicated spec and write the after file. The DoUpdate
	// closure captures the result so we can write it before deciding whether
	// to actually send the update.
	sched := desc.Schedule
	sched.Spec.Calendars = deduplicateCalendars(sched.Spec.Calendars)
	sched.Spec.Intervals = deduplicateIntervals(sched.Spec.Intervals)

	afterJSON, err := json.MarshalIndent(sched.Spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal deduped spec: %w", err)
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

// ForEachSchedule resolves the schedule ID set (from idsFile or namespace list)
// and calls fn for each, continuing on per-schedule errors and reporting a
// combined failure at the end.
func ForEachSchedule(ctx context.Context, cl sdkclient.Client, namespace, idsFile string, fn func(string) error) error {
	var scheduleIDs []string
	if idsFile != "" {
		ids, err := readScheduleIDs(idsFile)
		if err != nil {
			return err
		}
		scheduleIDs = ids
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

// readScheduleIDs reads one schedule ID per line from path, or from stdin when
// path is "-". Empty lines and lines beginning with "#" are ignored.
func readScheduleIDs(path string) ([]string, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()
		r = f
	}

	var ids []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ids = append(ids, line)
	}
	return ids, sc.Err()
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
