package timezonesource

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
)

// Kind distinguishes canonical UTC, custom TZif and native Windows input.
type Kind uint8

const (
	UTC Kind = iota
	TZif
	WindowsNative
)

// Snapshot owns immutable resolved input and the original preparation instant.
// It is prepared outside CHASM mutations and may be shared by builder caches.
type Snapshot struct {
	name, source string
	kind         Kind
	native       []byte
	data         []byte
	resolvedAt   time.Time
	zone         *occurrence.PreparedTimezone
}

func (s *Snapshot) Kind() Kind                             { return s.kind }
func (s *Snapshot) NativeData() []byte                     { return bytes.Clone(s.native) }
func (s *Snapshot) Name() string                           { return s.name }
func (s *Snapshot) Source() string                         { return s.source }
func (s *Snapshot) Data() []byte                           { return bytes.Clone(s.data) }
func (s *Snapshot) ResolvedAt() time.Time                  { return s.resolvedAt }
func (s *Snapshot) Prepared() *occurrence.PreparedTimezone { return s.zone }
func (s *Snapshot) SourceHash() [32]byte {
	if s.kind == WindowsNative {
		return sha256.Sum256(s.native)
	}
	return sha256.Sum256(s.data)
}
func (s *Snapshot) UTC() bool { return s.kind == UTC }

// SourceConfig supplies process-owned sources. Callers share one Environment
// across builders. Source reads and environment capture occur outside mutation.
type SourceConfig struct {
	LookupEnv    func(string) (string, bool)
	Read         func(source, name string) ([]byte, error)
	Embedded     func(string) ([]byte, error)
	Platform     []string
	GOROOT       string
	Clock        clock.TimeSource
	WindowsLocal func() (occurrence.WindowsTimezoneSource, error)
}

// Environment owns process-global source capture. It does not reconstruct
// arbitrary prior initialization history from the standard library's globals.
type Environment struct {
	config       SourceConfig
	overrideOnce sync.Once
	override     string
	localOnce    sync.Once
	local        *Snapshot
	localErr     error
}

// NewEnvironment creates an isolated source environment. Platform paths are
// explicit so tests and platform providers do not inherit the host filesystem.
func NewEnvironment(config SourceConfig) *Environment {
	if config.Read == nil {
		config.Read = ReadSource
	}
	if config.LookupEnv == nil {
		config.LookupEnv = os.LookupEnv
	}
	if config.Clock == nil {
		config.Clock = clock.NewRealTimeSource()
	}
	config.Platform = append([]string(nil), config.Platform...)
	return &Environment{config: config}
}

type cachedResult struct {
	snapshot *Snapshot
	err      error
}

// Resolver uses the same bounded success/error cache and TTL as SpecBuilder.
type Resolver struct {
	environment *Environment
	cache       cache.Cache
}

func NewResolver(environment *Environment) *Resolver {
	return &Resolver{environment: environment, cache: cache.New(1000, &cache.Options{TTL: 24 * time.Hour, TimeSource: environment.config.Clock})}
}

// Resolve distinguishes absent custom bytes from present-empty invalid input.
// Cache hits retain the first snapshot's exact preparation instant and rules.
func (r *Resolver) Resolve(name string, custom []byte) (*Snapshot, error) {
	if custom != nil {
		return r.environment.prepare(name, "custom", custom)
	}
	if found, ok := r.cache.Get(name).(*cachedResult); ok {
		return found.snapshot, found.err
	}
	snapshot, err := r.environment.resolve(name)
	r.cache.Put(name, &cachedResult{snapshot, err})
	return snapshot, err
}
func (e *Environment) utc(name string) *Snapshot {
	return &Snapshot{name: name, source: "UTC", resolvedAt: e.config.Clock.Now().UTC(), zone: occurrence.PreparedUTC()}
}
func (e *Environment) prepare(name, source string, data []byte) (*Snapshot, error) {
	at := e.config.Clock.Now().UTC()
	zone, err := occurrence.PrepareTimezone(data, at)
	if err != nil {
		return nil, errors.New("malformed time zone information")
	}
	return &Snapshot{kind: TZif, name: name, source: source, data: bytes.Clone(data), resolvedAt: at, zone: zone}, nil
}
func (e *Environment) resolve(name string) (*Snapshot, error) {
	if name == "" || name == "UTC" {
		return e.utc(name), nil
	}
	if name == "Local" {
		e.localOnce.Do(func() {
			if e.config.WindowsLocal != nil {
				e.local, e.localErr = e.resolveWindowsLocal()
			} else {
				e.local = e.resolveUnixLocal()
			}
		})
		return e.local, e.localErr
	}
	if strings.Contains(name, "..") || name[0] == '/' || name[0] == '\\' {
		return nil, errors.New("time: invalid location name")
	}
	e.overrideOnce.Do(func() { e.override, _ = e.config.LookupEnv("ZONEINFO") })
	var firstErr error
	if e.override != "" {
		data, err := e.config.Read(e.override, name)
		if err == nil {
			if snapshot, err := e.prepare(name, e.override, data); err == nil {
				return snapshot, nil
			}
			// Go LoadLocation's outer assignment retains the read error, which is nil
			// here, rather than the inner parser error.
		} else if err != syscall.ENOENT {
			firstErr = err
		}
	}
	snapshot, err := e.resolveSources(name, e.config.Platform)
	if err == nil {
		return snapshot, nil
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, err
}
func (e *Environment) resolveSources(name string, sources []string) (*Snapshot, error) {
	var firstErr error
	for _, source := range sources {
		snapshot, err := e.readSource(name, source, e.config.Read)
		if err == nil {
			return snapshot, nil
		}
		if firstErr == nil && err != syscall.ENOENT {
			firstErr = err
		}
	}
	if e.config.Embedded != nil {
		snapshot, err := e.readSource(name, "embedded", func(_ string, name string) ([]byte, error) { return e.config.Embedded(name) })
		if err == nil {
			return snapshot, nil
		}
		if firstErr == nil && err != syscall.ENOENT {
			firstErr = err
		}
	}
	if e.config.GOROOT != "" {
		snapshot, err := e.readSource(name, e.config.GOROOT, e.config.Read)
		if err == nil {
			return snapshot, nil
		}
		if firstErr == nil && err != syscall.ENOENT {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, errors.New("unknown time zone " + name)
}
func (e *Environment) readSource(name, source string, read func(string, string) ([]byte, error)) (*Snapshot, error) {
	data, err := read(source, name)
	if err != nil {
		return nil, err
	}
	return e.prepare(name, source, data)
}
func (e *Environment) resolveUnixLocal() *Snapshot {
	tz, ok := e.config.LookupEnv("TZ")
	var snapshot *Snapshot
	var err error
	switch {
	case !ok:
		snapshot, err = e.resolveSources("localtime", []string{"/etc"})
	case tz != "":
		tz = strings.TrimPrefix(tz, ":")
		if strings.HasPrefix(tz, "/") {
			snapshot, err = e.resolveSources(tz, []string{""})
		} else if tz != "" && tz != "UTC" {
			snapshot, err = e.resolveSources(tz, e.config.Platform)
		}
	default:
		return e.utc("Local")
	}
	if err != nil || snapshot == nil {
		return e.utc("Local")
	}
	snapshot.name = "Local"
	return snapshot
}

func (e *Environment) resolveWindowsLocal() (*Snapshot, error) {
	source, err := e.config.WindowsLocal()
	if err != nil {
		return e.utc("Local"), nil
	}
	at := e.config.Clock.Now().UTC()
	source.InitializationYear = int64(at.Year())
	zone, err := occurrence.PrepareWindowsTimezone(source)
	if err != nil {
		return nil, err
	}
	return &Snapshot{kind: WindowsNative, name: "Local", source: "windows-api", native: source.Bytes(), resolvedAt: at, zone: zone}, nil
}

func gorootSource(root string) string {
	if root == "" {
		return ""
	}
	return root + "/lib/time/zoneinfo.zip"
}
