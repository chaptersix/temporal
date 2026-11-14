package scheduler2

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	scheduler2pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"go.temporal.io/server/chasm/lib/scheduler2/internal/timezonesource"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

// StagedDefinitionSource owns immutable input-proportional preparation. Construct
// it outside mutation, including reconstruction from a detached source snapshot.
type StagedDefinitionSource struct {
	header   definitionHeader
	owner    [32]byte
	revision int64
	source   *scheduler2pb.DefinitionPreparationSource
	pages    [][]byte
}

// StageDefinitionSource canonicalizes without resolving a timezone. The caller
// resolves canonical timezone input separately through its shared provider.
func StageDefinitionSource(spec *schedulepb.ScheduleSpec, seed string, revision int64) (*StagedDefinitionSource, error) {
	if spec == nil || revision <= 0 {
		return nil, serviceerror.NewInvalidArgument("definition and positive revision are required")
	}
	if err := validateDefinitionBounds(spec); err != nil {
		return nil, err
	}
	canonical, err := legacy.CanonicalizeScheduleSpec(spec)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	hash := sha256.New()
	_, _ = hash.Write(digest[:])
	_, _ = hash.Write([]byte(seed))
	var identity [32]byte
	copy(identity[:], hash.Sum(nil))
	stage := &StagedDefinitionSource{revision: revision, owner: definitionOwner(identity, revision), source: &scheduler2pb.DefinitionPreparationSource{CanonicalSpec: encoded, Seed: []byte(seed)}}
	stage.header = definitionHeader{identity: identity, terms: uint64(len(canonical.StructuredCalendar)) + uint64(len(canonical.Interval)), exclusions: uint64(len(canonical.ExcludeStructuredCalendar)), seedBytes: uint64(len(seed)), start: canonical.StartTime.AsTime(), end: canonical.EndTime.AsTime(), hasStart: canonical.StartTime != nil, hasEnd: canonical.EndTime != nil, jitter: canonical.Jitter.AsDuration()}
	if !stage.header.hasStart {
		stage.header.start = time.Time{}
	}
	if !stage.header.hasEnd {
		stage.header.end = time.Time{}
	}
	terms := make([]occurrence.PreparedSpecTerm, 0, len(canonical.StructuredCalendar)+len(canonical.Interval)+len(canonical.ExcludeStructuredCalendar))
	for _, cal := range canonical.StructuredCalendar {
		terms = append(terms, stage.calendarTerm(cal, uint64(len(terms))))
	}
	for _, iv := range canonical.Interval {
		terms = append(terms, occurrence.PreparedSpecTerm{IsInterval: true, Interval: max(int64(iv.Interval.AsDuration()/time.Second), 1), Phase: max(int64(iv.Phase.AsDuration()/time.Second), 0)})
	}
	for _, cal := range canonical.ExcludeStructuredCalendar {
		terms = append(terms, stage.calendarTerm(cal, uint64(len(terms))))
	}
	for start := 0; start < len(terms); start += definitionTermsPerPage {
		stage.pages = append(stage.pages, encodeDefinitionTerms(terms[start:min(start+definitionTermsPerPage, len(terms))]))
	}
	for start := 0; start < len(seed); start += definitionSeedPageBytes {
		end := min(start+definitionSeedPageBytes, len(seed))
		page := append([]byte{2, byte(end - start)}, seed[start:end]...)
		stage.pages = append(stage.pages, page)
	}
	if err := stage.validate(); err != nil {
		return nil, err
	}
	return stage, nil
}
func validateDefinitionBounds(spec *schedulepb.ScheduleSpec) error {
	if spec.StartTime != nil && spec.StartTime.CheckValid() != nil {
		return serviceerror.NewInvalidArgument("invalid start timestamp")
	}
	if spec.EndTime != nil && spec.EndTime.CheckValid() != nil {
		return serviceerror.NewInvalidArgument("invalid end timestamp")
	}
	if spec.Jitter != nil && (spec.Jitter.CheckValid() != nil || spec.Jitter.AsDuration() < 0) {
		return serviceerror.NewInvalidArgument("jitter must be a nonnegative valid duration")
	}
	return nil
}
func (s *StagedDefinitionSource) calendarTerm(cal *schedulepb.StructuredCalendarSpec, ordinal uint64) occurrence.PreparedSpecTerm {
	masks := occurrence.CalendarMasks{AllYears: len(cal.Year) == 0}
	fields := [][]*schedulepb.Range{cal.Second, cal.Minute, cal.Hour, cal.DayOfMonth, cal.Month, cal.DayOfWeek, cal.Year}
	for f, ranges := range fields {
		for _, r := range ranges {
			end := max(r.End, r.Start)
			step := max(r.Step, 1)
			for value := r.Start; ; {
				if f == 6 {
					bit := value - 2000
					masks.Years[bit/64] |= uint64(1) << (bit % 64)
				} else {
					masks.Fields[f] |= uint64(1) << value
				}
				if step > end-value {
					break
				}
				value += step
			}
		}
	}
	return occurrence.PreparedSpecTerm{Calendar: occurrence.PreparedCalendar{SourceID: definitionTermID(s.owner, ordinal), Masks: masks}}
}
func (s *StagedDefinitionSource) validate() error {
	if s == nil || s.source == nil || s.revision <= 0 || s.owner != definitionOwner(s.header.identity, s.revision) {
		return serviceerror.NewInvalidArgument("definition staging source is uninitialized")
	}
	h, err := decodeDefinitionHeader(s.header.encode())
	if err != nil {
		return err
	}
	_, pages, err := h.pages()
	if err != nil {
		return err
	}
	if pages != uint64(len(s.pages)) || h.seedBytes != uint64(len(s.source.Seed)) {
		return serviceerror.NewInvalidArgument("definition staging page count is invalid")
	}
	return nil
}

// Identity is identical to the existing public enumerator's canonical content identity.
func (s *StagedDefinitionSource) Identity() string { return hex.EncodeToString(s.header.identity[:]) }
func (s *StagedDefinitionSource) Revision() int64  { return s.revision }

// CanonicalSpec returns detached metadata for outside-mutation timezone resolution
// and immediate read-only preview, even before page installation completes.
func (s *StagedDefinitionSource) CanonicalSpec() (*schedulepb.ScheduleSpec, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	var spec schedulepb.ScheduleSpec
	if err := proto.Unmarshal(s.source.CanonicalSpec, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// StageDefinitionSnapshot reconstructs only from a detached source read. It must
// not run inside a mutation callback; all masks and source hashes are rebuilt.
func StageDefinitionSnapshot(source *scheduler2pb.DefinitionPreparationSource, revision int64) (*StagedDefinitionSource, error) {
	if source == nil {
		return nil, serviceerror.NewInvalidArgument("definition snapshot is missing")
	}
	var spec schedulepb.ScheduleSpec
	if err := proto.Unmarshal(source.CanonicalSpec, &spec); err != nil {
		return nil, err
	}
	return StageDefinitionSource(&spec, string(source.Seed), revision)
}
func (s *StagedDefinitionSource) state() *scheduler2pb.DefinitionPreparationState {
	return &scheduler2pb.DefinitionPreparationState{FormatVersion: 1, SourceId: bytes.Clone(s.header.identity[:]), OwnerId: bytes.Clone(s.owner[:]), Revision: s.revision, Header: s.header.encode(), TotalPages: uint64(len(s.pages))}
}

// DefinitionEvaluationPolicy is sampled once outside a logical evaluation job.
// Threshold changes cannot silently resume a prefix under a different policy.
type DefinitionEvaluationPolicy struct {
	WarnIterations, MaxIterations int
	ReturnExcluded                bool
}

func (h definitionHeader) prepared(owner [32]byte, policy DefinitionEvaluationPolicy) occurrence.PreparedSpecHeader {
	var input [49]byte
	copy(input[:32], owner[:])
	putPolicyInt(input[32:40], policy.WarnIterations)
	putPolicyInt(input[40:48], policy.MaxIterations)
	if policy.ReturnExcluded {
		input[48] = 1
	}
	return occurrence.PreparedSpecHeader{SourceID: sha256.Sum256(input[:]), Terms: h.terms, Exclusions: h.exclusions, SeedBytes: h.seedBytes, Start: h.start, End: h.end, HasStart: h.hasStart, HasEnd: h.hasEnd, Jitter: h.jitter, WarnIterations: policy.WarnIterations, MaxIterations: policy.MaxIterations, ReturnExcluded: policy.ReturnExcluded}
}
func putPolicyInt(out []byte, value int) {
	v := uint64(int64(value))
	for i := 7; i >= 0; i-- {
		out[i] = byte(v)
		v >>= 8
	}
}

// Prepared builds a detached reader over immutable staging pages for immediate
// preview. Its work is bounded per read; staging construction remains linear.
func (s *StagedDefinitionSource) Prepared(policy DefinitionEvaluationPolicy) (occurrence.PreparedSpecHeader, occurrence.PreparedSpecReader, error) {
	if err := s.validate(); err != nil {
		return occurrence.PreparedSpecHeader{}, occurrence.PreparedSpecReader{}, err
	}
	reader := definitionReader(s.header, s.owner, func(ordinal uint64) ([]byte, error) {
		if ordinal >= uint64(len(s.pages)) {
			return nil, serviceerror.NewInternal("staged definition page is absent")
		}
		return s.pages[ordinal], nil
	})
	return s.header.prepared(s.owner, policy), reader, nil
}

// StageDefinitionTimezone binds a provider snapshot to this revision's owner.
// Generation increases across replacements, including definition replacements.
func StageDefinitionTimezone(definition *StagedDefinitionSource, snapshot *timezonesource.Snapshot, generation uint64) (*StagedTimezoneSource, error) {
	if err := definition.validate(); err != nil {
		return nil, err
	}
	if snapshot == nil || snapshot.Prepared() == nil {
		return nil, serviceerror.NewInvalidArgument("resolved timezone snapshot is required")
	}
	stage, err := stageTimezoneForDefinition(definition.owner, snapshot.Name(), snapshot.Data(), snapshot.ResolvedAt(), generation, snapshot.Kind() == timezonesource.UTC, snapshot.Prepared())
	if err != nil {
		return nil, err
	}
	switch snapshot.Kind() {
	case timezonesource.UTC, timezonesource.TZif:
	case timezonesource.WindowsNative:
		stage.native = snapshot.NativeData()
	default:
		return nil, serviceerror.NewInvalidArgument("unknown resolved timezone source kind")
	}
	return stage, nil
}

// StageDefinitionTimezoneSnapshot reconstructs a persisted snapshot against its
// original definition owner. The caller checks that owner before replacing data.
func StageDefinitionTimezoneSnapshot(definition *StagedDefinitionSource, source *scheduler2pb.TimezonePreparationSource, generation uint64) (*StagedTimezoneSource, error) {
	if err := definition.validate(); err != nil {
		return nil, err
	}
	if source == nil || source.ResolvedAt == nil || source.ResolvedAt.CheckValid() != nil {
		return nil, serviceerror.NewInvalidArgument("timezone source preparation instant is invalid")
	}
	var zone *occurrence.PreparedTimezone
	var err error
	switch {
	case source.Utc:
		if len(source.TzifData) != 0 || len(source.WindowsNativeData) != 0 {
			return nil, serviceerror.NewInvalidArgument("UTC source has conflicting timezone data")
		}
		zone = occurrence.PreparedUTC()
	case len(source.WindowsNativeData) != 0:
		if len(source.TzifData) != 0 {
			return nil, serviceerror.NewInvalidArgument("timezone source has conflicting variants")
		}
		native, decodeErr := occurrence.WindowsTimezoneSourceFromBytes(source.WindowsNativeData)
		if decodeErr != nil {
			return nil, decodeErr
		}
		zone, err = occurrence.PrepareWindowsTimezone(native)
	default:
		if source.TzifData == nil {
			return nil, serviceerror.NewInvalidArgument("resolved timezone data is required")
		}
		zone, err = occurrence.PrepareTimezone(source.TzifData, source.ResolvedAt.AsTime())
	}
	if err != nil {
		return nil, err
	}
	stage, err := stageTimezoneForDefinition(definition.owner, source.Name, source.TzifData, source.ResolvedAt.AsTime(), generation, source.Utc, zone)
	if err != nil {
		return nil, err
	}
	stage.native = bytes.Clone(source.WindowsNativeData)
	return stage, nil
}
