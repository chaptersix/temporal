package tdbg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/tools/schedutil"
)

// ScheduleDedup deduplicates StructuredCalendar entries in a schedule spec.
// Without --execute it writes before/after JSON to a temp directory and exits.
// With --recreate it reads from workflow history instead of describing the schedule,
// then deletes and recreates it — use when the workflow is too degraded to process an update.
func ScheduleDedup(c *cli.Context, _ ClientFactory) error {
	outDir, err := os.MkdirTemp("", "schedutil-*")
	if err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	fmt.Printf("Output directory: %s\n\n", outDir)

	ns := c.String(FlagNamespace)
	execute := c.Bool(FlagExecute)
	recreate := c.Bool(FlagRecreate)

	return withSdkClient(c, func(ctx context.Context, cl sdkclient.Client) error {
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
	})
}

// ScheduleForceCAN sends a force-continue-as-new signal to the scheduler workflow.
// Without --execute it prints what would be signalled and exits.
func ScheduleForceCAN(c *cli.Context, _ ClientFactory) error {
	ns := c.String(FlagNamespace)
	execute := c.Bool(FlagExecute)
	return withSdkClient(c, func(ctx context.Context, cl sdkclient.Client) error {
		if sid := c.String(FlagScheduleID); sid != "" {
			return schedutil.RunForceCAN(ctx, cl, sid, execute)
		}
		return schedutil.ForEachSchedule(ctx, cl, ns, func(sid string) error {
			return schedutil.RunForceCAN(ctx, cl, sid, execute)
		})
	})
}

// withSdkClient builds an SDK client from the CLI's global TLS and address
// flags and calls fn with a background context.
func withSdkClient(c *cli.Context, fn func(context.Context, sdkclient.Client) error) error {
	address := c.String(FlagAddress)
	if address == "" {
		address = DefaultFrontendAddress
	}

	tlsCfg, err := buildScheduleTLSConfig(c, address)
	if err != nil {
		return fmt.Errorf("TLS config: %w", err)
	}

	cl, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  address,
		Namespace: c.String(FlagNamespace),
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

// buildScheduleTLSConfig constructs a *tls.Config from the CLI's TLS flags,
// returning nil if no TLS options are set.
func buildScheduleTLSConfig(c *cli.Context, address string) (*tls.Config, error) {
	certPath := c.String(FlagTLSCertPath)
	keyPath := c.String(FlagTLSKeyPath)
	caPath := c.String(FlagTLSCaPath)
	disableHostVerification := c.Bool(FlagTLSDisableHostVerification)
	serverName := c.String(FlagTLSServerName)

	var cert *tls.Certificate
	var caPool *x509.CertPool

	if caPath != "" {
		pool, err := fetchScheduleCACert(caPath)
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

// fetchScheduleCACert loads a PEM CA certificate from a file path or HTTPS URL.
func fetchScheduleCACert(pathOrURL string) (*x509.CertPool, error) {
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
		defer func() {
			_ = resp.Body.Close()
		}()
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
