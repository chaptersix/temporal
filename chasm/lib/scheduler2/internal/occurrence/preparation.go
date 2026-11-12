package occurrence

import "go.temporal.io/api/serviceerror"

const (
	CalendarRangePageSize = 16
	MaxPreparationSteps   = 64
)

// CalendarHeader identifies one immutable, validated calendar source. Fields are
// ordered second, minute, hour, day of month, month, day of week, year.
type CalendarHeader struct {
	SourceID [32]byte
	Ranges   [7]uint32
}

// CalendarRange is a detached range from the canonical calendar source.
type CalendarRange struct{ Start, End, Step int32 }

// RangePage bounds a source fetch independently of total definition size.
type RangePage struct {
	Ranges [CalendarRangePageSize]CalendarRange
	Count  uint8
}

// CalendarMasks keeps compiled matching state independent of input range count.
// An empty year range list matches all years, including years before 2000.
type CalendarMasks struct {
	Fields   [6]uint64
	Years    [2]uint64
	AllYears bool
}

// PreparationState is a value-only continuation. Partial masks are not a usable
// compiled calendar until Done. SourceID fences definition replacement.
type PreparationState struct {
	SourceID [32]byte
	Field    uint8
	Range    uint32
	Next     int32
	InRange  bool
	Partial  CalendarMasks
	Done     bool
}

// PreparationStats counts elementary cursor/value steps and bounded page reads.
type PreparationStats struct{ Steps, Pages int }

// PrepareCalendar performs at most budget elementary steps. Each step advances
// one range value or one field cursor and fetches at most one fixed-size page.
// Source pages must belong to header.SourceID and remain immutable. An error
// returns the original state, so callers cannot publish partial failed work.
func PrepareCalendar(state PreparationState, header CalendarHeader, readPage func(field uint8, page uint32) (RangePage, error), budget int) (PreparationState, PreparationStats, error) {
	original := state
	stats := PreparationStats{}
	fail := func(message string) (PreparationState, PreparationStats, error) {
		return original, stats, serviceerror.NewInternal(message)
	}
	if budget <= 0 || budget > MaxPreparationSteps {
		return original, stats, serviceerror.NewInvalidArgument("calendar preparation budget is outside supported bounds")
	}
	if header.SourceID == [32]byte{} || readPage == nil {
		return fail("calendar preparation source is missing")
	}
	if state == (PreparationState{}) {
		state.SourceID = header.SourceID
		state.Partial.AllYears = header.Ranges[6] == 0
	}
	if !validPreparationSource(state, header) {
		return fail("calendar preparation source or cursor is invalid")
	}
	if state.Done {
		if state.Field != 7 || state.Range != 0 || state.InRange || state.Next != 0 {
			return fail("completed calendar preparation cursor is invalid")
		}
		return state, stats, nil
	}
	if state.Field == 7 || (!state.InRange && state.Next != 0) {
		return fail("calendar preparation cursor is invalid")
	}
	for stats.Steps < budget && !state.Done {
		stats.Steps++
		count := header.Ranges[state.Field]
		if state.Range > count || (state.Range == count && state.InRange) {
			return fail("calendar preparation range cursor is invalid")
		}
		if state.Range == count {
			state.Field++
			state.Range = 0
			state.Done = state.Field == 7
			continue
		}
		stats.Pages++
		r, err := preparationRange(state, header, readPage)
		if err != nil {
			return original, stats, err
		}
		if err := prepareRangeValue(&state, r); err != nil {
			return original, stats, err
		}
	}
	return state, stats, nil
}

// validPreparationMasks validates fixed-size structure, not semantic equality to
// the already processed source prefix. Prefix integrity remains the caller's
// persisted-state and immutable-source obligation.
func validPreparationMasks(s PreparationState) bool {
	allowed := [6]uint64{1<<60 - 1, 1<<60 - 1, 1<<24 - 1, 1<<32 - 2, 1<<13 - 2, 1<<7 - 1}
	for field, mask := range s.Partial.Fields {
		if mask & ^allowed[field] != 0 || (field > int(s.Field) && mask != 0) {
			return false
		}
	}
	if s.Partial.Years[1]>>37 != 0 {
		return false
	}
	if (s.Partial.AllYears || s.Field < 6) && s.Partial.Years != [2]uint64{} {
		return false
	}
	return true
}

func prepareRangeValue(state *PreparationState, r CalendarRange) error {
	low, high := int32(0), int32(59)
	switch state.Field {
	case 0, 1:
	case 2:
		high = 23
	case 3:
		low, high = 1, 31
	case 4:
		low, high = 1, 12
	case 5:
		high = 6
	case 6:
		low, high = 2000, 2100
	default:
		return serviceerror.NewInternal("calendar preparation field is invalid")
	}
	// Only omitted End may normalize below Start in validated source.
	if r.Start < low || r.Start > high || (r.End != 0 && (r.End < r.Start || r.End > high)) || r.Step < 0 {
		return serviceerror.NewInternal("calendar preparation range is outside its field domain")
	}
	if r.End < r.Start {
		r.End = r.Start
	}
	if r.Step == 0 {
		r.Step = 1
	}
	value := r.Start
	if state.InRange {
		value = state.Next
		if value < r.Start || value > r.End || (value-r.Start)%r.Step != 0 {
			return serviceerror.NewInternal("calendar preparation value cursor is invalid")
		}
	}
	if state.Field == 6 {
		bit := value - 2000
		state.Partial.Years[bit/64] |= uint64(1) << (bit % 64)
	} else {
		state.Partial.Fields[state.Field] |= uint64(1) << value
	}
	// Compare before adding, since an accepted step can exceed the field width.
	if r.Step > r.End-value {
		state.Range++
		state.InRange, state.Next = false, 0
	} else {
		state.InRange, state.Next = true, value+r.Step
	}
	return nil
}

func preparationRange(state PreparationState, header CalendarHeader, readPage func(uint8, uint32) (RangePage, error)) (CalendarRange, error) {
	pageIndex := state.Range / CalendarRangePageSize
	page, err := readPage(state.Field, pageIndex)
	if err != nil {
		return CalendarRange{}, err
	}
	expected := min(uint32(CalendarRangePageSize), header.Ranges[state.Field]-pageIndex*CalendarRangePageSize)
	if uint32(page.Count) != expected {
		return CalendarRange{}, serviceerror.NewInternal("calendar preparation page length is invalid")
	}
	return page.Ranges[state.Range%CalendarRangePageSize], nil
}

func validPreparationSource(state PreparationState, header CalendarHeader) bool {
	return state.SourceID == header.SourceID && state.Field <= 7 && state.Partial.AllYears == (header.Ranges[6] == 0) && validPreparationMasks(state)
}
