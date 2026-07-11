package propertytest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/proto"
)

type OperationalCacheMode string

const (
	OperationalCacheUncached OperationalCacheMode = "uncached"
	OperationalCacheCached   OperationalCacheMode = "cached"
)

type OperationalCacheReport struct {
	Mode                   OperationalCacheMode
	Hit                    bool
	Lookups                int
	ExecutedValidationWork int
	ExecutedMatchingWork   int
	LogicalValidationWork  int
	LogicalMatchingWork    int
}

type operationalCacheEntry struct {
	ready  chan struct{}
	result ComputeResult
	err    error
}

type OperationalCalculator struct {
	mode    OperationalCacheMode
	mu      sync.Mutex
	entries map[[32]byte]*operationalCacheEntry
}

type operationalCompiledEntry struct {
	ready    chan struct{}
	compiled *CompiledSpec
	err      error
}

type OperationalCompiledSpecCache struct {
	mu          sync.Mutex
	entries     map[[32]byte]*operationalCompiledEntry
	beforeBuild func()
}

func NewOperationalCompiledSpecCache() *OperationalCompiledSpecCache {
	return &OperationalCompiledSpecCache{entries: make(map[[32]byte]*operationalCompiledEntry)}
}

func (c *OperationalCompiledSpecCache) Get(
	ctx context.Context,
	builder *SpecBuilder,
	spec *schedulepb.ScheduleSpec,
) (*CompiledSpec, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		return nil, false, err
	}
	key := sha256.Sum256(data)
	c.mu.Lock()
	entry, hit := c.entries[key]
	if !hit {
		entry = &operationalCompiledEntry{ready: make(chan struct{})}
		c.entries[key] = entry
	}
	c.mu.Unlock()
	if hit {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-entry.ready:
			return entry.compiled, true, entry.err
		}
	}
	if c.beforeBuild != nil {
		c.beforeBuild()
	}
	if err := ctx.Err(); err != nil {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		entry.err = err
		close(entry.ready)
		return nil, false, err
	}
	entry.compiled, entry.err = builder.NewCompiledSpec(spec)
	close(entry.ready)
	return entry.compiled, false, entry.err
}

func NewOperationalCalculator(mode OperationalCacheMode) *OperationalCalculator {
	return &OperationalCalculator{mode: mode, entries: make(map[[32]byte]*operationalCacheEntry)}
}

func (c *OperationalCalculator) Compute(
	ctx context.Context,
	spec *schedulepb.ScheduleSpec,
	start time.Time,
	end time.Time,
	seed string,
	options ComputeOptions,
) (ComputeResult, OperationalCacheReport, error) {
	report := OperationalCacheReport{Mode: c.mode}
	if c.mode != OperationalCacheCached || options.WorkTickHook != nil {
		result, err := ComputeMatchingTimes(ctx, spec, start, end, seed, options)
		return result, executedCacheReport(report, result), err
	}
	if err := ctx.Err(); err != nil {
		return ComputeResult{}, report, err
	}
	key, err := operationalRequestKey(spec, start, end, seed, options)
	if err != nil {
		result, computeErr := ComputeMatchingTimes(ctx, spec, start, end, seed, options)
		return result, executedCacheReport(report, result), computeErr
	}
	report.Lookups = 1
	c.mu.Lock()
	entry, hit := c.entries[key]
	if !hit {
		entry = &operationalCacheEntry{ready: make(chan struct{})}
		c.entries[key] = entry
	}
	c.mu.Unlock()
	if hit {
		select {
		case <-ctx.Done():
			return ComputeResult{}, report, ctx.Err()
		case <-entry.ready:
			report.Hit = true
			result := cloneComputeResult(entry.result)
			report.LogicalValidationWork = result.Validation.Work.Total
			report.LogicalMatchingWork = result.Work.Total
			return result, report, entry.err
		}
	}

	result, computeErr := ComputeMatchingTimes(ctx, spec, start, end, seed, options)
	if errors.Is(computeErr, context.Canceled) || errors.Is(computeErr, context.DeadlineExceeded) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
	}
	entry.result = cloneComputeResult(result)
	entry.err = computeErr
	close(entry.ready)
	return result, executedCacheReport(report, result), computeErr
}

func executedCacheReport(report OperationalCacheReport, result ComputeResult) OperationalCacheReport {
	report.ExecutedValidationWork = result.Validation.Work.Total
	report.ExecutedMatchingWork = result.Work.Total
	report.LogicalValidationWork = result.Validation.Work.Total
	report.LogicalMatchingWork = result.Work.Total
	return report
}

func operationalRequestKey(
	spec *schedulepb.ScheduleSpec,
	start time.Time,
	end time.Time,
	seed string,
	options ComputeOptions,
) ([32]byte, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		return [32]byte{}, err
	}
	buffer := make([]byte, 0, len(data)+len(seed)+64)
	buffer = append(buffer, data...)
	buffer = append(buffer, seed...)
	for _, value := range []int64{
		start.UnixNano(), end.UnixNano(), int64(options.MaxResults),
		int64(options.MaxIterations), int64(options.MaxValidationIterations),
	} {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(value))
		buffer = append(buffer, encoded[:]...)
	}
	return sha256.Sum256(buffer), nil
}

func cloneComputeResult(result ComputeResult) ComputeResult {
	result.Times = append([]time.Time(nil), result.Times...)
	return result
}
