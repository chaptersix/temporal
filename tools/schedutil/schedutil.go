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
// The namespace-wide and file modes require --yes to execute; without it the
// command prints the affected IDs and exits.
//
// # Commands
//
//	dedup      Print the current spec as JSON, then send an UpdateSchedule that
//	           removes duplicate StructuredCalendar entries client-side.
//	force-can  Send a force-continue-as-new signal to the scheduler workflow.
//
// # Examples
//
//	# Single schedule
//	schedutil -namespace prod dedup --schedule-id my-sched
//
//	# Explicit list from a file
//	schedutil -namespace prod dedup --ids-file affected.txt --yes
//
//	# Piped from another tool
//	temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
//	    schedutil -namespace prod dedup --ids-file - --yes
//
//	# Whole namespace (dry run first, then execute)
//	schedutil -namespace prod dedup
//	schedutil -namespace prod dedup --yes
//
//	# Force CAN on a single schedule
//	schedutil -namespace prod force-can --schedule-id my-sched
//
// Global flags mirror tdbg (address, namespace, TLS, context-timeout) and
// honour the same environment variables (TEMPORAL_CLI_ADDRESS, etc.).
package schedutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"bufio"
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
)

// Run is the entry point for the schedutil tool.
func Run(args []string) error {
	app := cli.NewApp()
	app.Name  = "schedutil"
	app.Usage = "Operations on individual Temporal schedules"
	app.Description = `Target a single schedule, a file of IDs, or an entire namespace:

  Single schedule:
    schedutil -namespace prod dedup --schedule-id my-sched
    schedutil -namespace prod force-can --schedule-id my-sched

  Explicit list from a file (one ID per line; "#" lines and blanks ignored):
    schedutil -namespace prod dedup --ids-file affected.txt --yes

  Piped from another tool ("-" reads stdin):
    temporal schedule list -n prod -o json | jq -r '.[].scheduleId' | \
        schedutil -namespace prod dedup --ids-file - --yes

  Whole namespace (omit both flags; dry-run first, then execute):
    schedutil -namespace prod dedup
    schedutil -namespace prod dedup --yes`
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
		Usage: `File of schedule IDs to operate on, one per line. Use "-" to read from stdin. Mutually exclusive with --schedule-id.`,
	}
	yesFlag := &cli.BoolFlag{
		Name:  tdbg.FlagYes,
		Usage: "Required when no --schedule-id is given; without it the command lists affected schedules and exits.",
	}
	app.Commands = []*cli.Command{
		{
			Name:  "dedup",
			Usage: "Remove duplicate StructuredCalendar entries from a schedule spec",
			Flags: []cli.Flag{scheduleIDFlag, idsFileFlag, yesFlag},
			Action: func(c *cli.Context) error {
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					ns := c.String(tdbg.FlagNamespace)
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return RunDedup(ctx, cl, ns, sid)
					}
					return ForEachSchedule(ctx, cl, ns, c.String(flagIDsFile), c.Bool(tdbg.FlagYes), func(sid string) error {
						return RunDedup(ctx, cl, ns, sid)
					})
				})
			},
		},
		{
			Name:  "force-can",
			Usage: "Send a force-continue-as-new signal to the scheduler workflow",
			Flags: []cli.Flag{scheduleIDFlag, idsFileFlag, yesFlag},
			Action: func(c *cli.Context) error {
				return withClient(c, func(ctx context.Context, cl sdkclient.Client) error {
					ns := c.String(tdbg.FlagNamespace)
					if sid := c.String(tdbg.FlagScheduleID); sid != "" {
						return RunForceCAN(ctx, cl, sid)
					}
					return ForEachSchedule(ctx, cl, ns, c.String(flagIDsFile), c.Bool(tdbg.FlagYes), func(sid string) error {
						return RunForceCAN(ctx, cl, sid)
					})
				})
			},
		},
	}
	return app.Run(append([]string{"schedutil"}, args...))
}

// withClient creates an SDK client from the CLI flags and calls fn with an
// unbounded context. Individual RPCs should derive per-call timeouts from
// --context-timeout; the session itself is not deadline-bounded so bulk
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

// RunDedup fetches the current schedule, prints its spec as JSON, deduplicates
// calendar and interval entries client-side, then issues an UpdateSchedule with
// the clean spec. This does not rely on any server-side deduplication fix.
func RunDedup(ctx context.Context, cl sdkclient.Client, namespace, scheduleID string) error {
	handle := cl.ScheduleClient().GetHandle(ctx, scheduleID)

	desc, err := handle.Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe schedule: %w", err)
	}

	specJSON, err := json.MarshalIndent(desc.Schedule.Spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	fmt.Printf("Current spec for schedule %q (namespace %q):\n%s\n\n", scheduleID, namespace, specJSON)
	fmt.Printf("Calendar entries before: %d\n", len(desc.Schedule.Spec.Calendars))
	fmt.Printf("Interval entries before: %d\n", len(desc.Schedule.Spec.Intervals))

	var nCalAfter, nIntAfter int
	err = handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
		DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
			sched := input.Description.Schedule
			sched.Spec.Calendars = deduplicateCalendars(sched.Spec.Calendars)
			sched.Spec.Intervals = deduplicateIntervals(sched.Spec.Intervals)
			nCalAfter = len(sched.Spec.Calendars)
			nIntAfter = len(sched.Spec.Intervals)
			return &sdkclient.ScheduleUpdate{Schedule: &sched}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("update schedule: %w", err)
	}
	fmt.Printf("Calendar entries after:  %d\n", nCalAfter)
	fmt.Printf("Interval entries after:  %d\n", nIntAfter)
	return nil
}

// deduplicateCalendars removes duplicate ScheduleCalendarSpec entries,
// preserving first occurrence. ScheduleCalendarSpec contains slice fields so
// it is not directly comparable; reflect.DeepEqual is used instead. n is
// always small so the O(n²) scan is fine.
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
// preserving first occurrence. ScheduleIntervalSpec fields are time.Duration
// (int64 aliases) and are directly comparable as a map key.
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

// RunForceCAN sends a force-continue-as-new signal to the scheduler workflow
// for the given schedule ID.
func RunForceCAN(ctx context.Context, cl sdkclient.Client, scheduleID string) error {
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	fmt.Printf("Signaling %s with %q...\n", workflowID, scheduler.SignalNameForceCAN)
	if err := cl.SignalWorkflow(ctx, workflowID, "", scheduler.SignalNameForceCAN, nil); err != nil {
		return fmt.Errorf("signal workflow: %w", err)
	}
	fmt.Println("Signal sent.")
	return nil
}

// ForEachSchedule operates on a set of schedule IDs. If idsFile is non-empty
// the IDs are read from that file ("-" means stdin); otherwise all schedules in
// the namespace are listed. Without yes it prints the IDs and returns; with yes
// it calls fn for each, continuing on per-schedule errors and reporting a
// combined failure at the end.
func ForEachSchedule(ctx context.Context, cl sdkclient.Client, namespace, idsFile string, yes bool, fn func(string) error) error {
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

	if !yes {
		fmt.Printf("Schedules in namespace %q that would be affected (%d):\n", namespace, len(scheduleIDs))
		for _, sid := range scheduleIDs {
			fmt.Printf("  %s\n", sid)
		}
		fmt.Println("\nRe-run with --yes to execute.")
		return nil
	}

	var failed []string
	for _, sid := range scheduleIDs {
		fmt.Printf("\n--- %s ---\n", sid)
		if err := fn(sid); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			failed = append(failed, sid)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d schedule(s) failed: %v", len(failed), failed)
	}
	return nil
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
