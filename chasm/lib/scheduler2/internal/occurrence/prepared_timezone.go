package occurrence

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

const preparedTimezonePageSize = 16

type preparedTimezonePage struct {
	identity [32]byte
	ordinal  int
	length   int
	entries  [preparedTimezonePageSize]preparedTZTransition
}
type preparedTimezoneCache struct {
	present    bool
	start, end int64
	offset     int64
}

// PreparedTimezone owns immutable indexed transitions and parsed extension rules.
// Construction and hashing are input-proportional and must run outside mutation.
// Lookup uses only fixed-size pages; it never loads a time.Location or parses text.
type PreparedTimezone struct {
	identity     [32]byte
	count        int
	first        int64
	codecVersion uint8
	cache        preparedTimezoneCache
	rules        preparedTZRules
	pages        []preparedTimezonePage
	readPage     func(int) (preparedTimezonePage, error)
}

type timezoneWork struct{ lookups, pages int }

// PrepareTimezone parses custom TZif data with an explicit loader cache instant.
// Persisted prepared data must retain this cache across reload; it is observable
// with unsorted transitions accepted by Go's TZif loader.
func PrepareTimezone(data []byte, resolvedAt time.Time) (*PreparedTimezone, error) {
	parsed, err := parsePreparedTZData(data)
	if err != nil {
		return nil, err
	}
	z := &PreparedTimezone{count: len(parsed.transitions), first: parsed.firstOffset, rules: parsed.rules}
	sec := resolvedAt.Unix()
	for i, tx := range parsed.transitions {
		if tx.when <= sec && (i+1 == len(parsed.transitions) || sec < parsed.transitions[i+1].when) {
			z.cache = preparedTimezoneCache{present: true, start: tx.when, end: preparedTZOmega, offset: tx.offset}
			if i+1 < len(parsed.transitions) {
				z.cache.end = parsed.transitions[i+1].when
			} else if z.rules.valid {
				offset, start, end := z.rules.lookup(tx.when, sec)
				z.cache.offset, z.cache.start, z.cache.end = int64(offset), start, end
			}
			break
		}
	}
	// Exact source identity plus observable cache state identifies this prepared
	// snapshot. The resolved-at instant itself is not a semantic input afterward.
	hash := sha256.New()
	_, _ = hash.Write([]byte("scheduler2-prepared-timezone-v1"))
	_, _ = hash.Write(data)
	var cache [25]byte
	if z.cache.present {
		cache[0] = 1
	}
	binary.BigEndian.PutUint64(cache[1:9], uint64(z.cache.start))
	binary.BigEndian.PutUint64(cache[9:17], uint64(z.cache.end))
	binary.BigEndian.PutUint64(cache[17:25], uint64(z.cache.offset))
	_, _ = hash.Write(cache[:])
	copy(z.identity[:], hash.Sum(nil))
	z.setTransitions(parsed.transitions)
	return z, nil
}

// PreparedUTC supplies the canonical zero-offset capability without TZif input.
func PreparedUTC() *PreparedTimezone {
	return &PreparedTimezone{identity: sha256.Sum256([]byte("scheduler2-prepared-timezone-UTC-v1"))}
}

// Fingerprint includes rules and loader-cache behavior, rather than a zone name.
func (z *PreparedTimezone) Fingerprint() [32]byte { return z.identity }

func (z *PreparedTimezone) transition(index int, work *timezoneWork) (preparedTZTransition, error) {
	if index < 0 || index >= z.count {
		return preparedTZTransition{}, errors.New("prepared timezone transition index outside bounds")
	}
	ordinal := index / preparedTimezonePageSize
	work.pages++
	var page preparedTimezonePage
	var err error
	if z.readPage != nil {
		page, err = z.readPage(ordinal)
	} else if ordinal < len(z.pages) {
		page = z.pages[ordinal]
	} else {
		err = errors.New("prepared timezone page absent")
	}
	if err != nil {
		return preparedTZTransition{}, err
	}
	expected := min(preparedTimezonePageSize, z.count-ordinal*preparedTimezonePageSize)
	if page.identity != z.identity || page.ordinal != ordinal || page.length != expected {
		return preparedTZTransition{}, errors.New("prepared timezone page identity mismatch")
	}
	return page.entries[index%preparedTimezonePageSize], nil
}

func (z *PreparedTimezone) lookup(sec int64, work *timezoneWork) (offset, start, end int64, err error) {
	work.lookups++
	if z.count == 0 {
		return 0, preparedTZAlpha, preparedTZOmega, nil
	}
	if z.cache.present && z.cache.start <= sec && sec < z.cache.end {
		return z.cache.offset, z.cache.start, z.cache.end, nil
	}
	first, err := z.transition(0, work)
	if err != nil {
		return 0, 0, 0, err
	}
	if sec < first.when {
		return z.first, preparedTZAlpha, first.when, nil
	}
	lo, hi := 0, z.count
	end = preparedTZOmega
	// Preserve Go's exact midpoint and end tracking, even for unsorted input.
	for hi-lo > 1 {
		middle := int(uint(lo+hi) >> 1)
		tx, readErr := z.transition(middle, work)
		if readErr != nil {
			return 0, 0, 0, readErr
		}
		if sec < tx.when {
			end = tx.when
			hi = middle
		} else {
			lo = middle
		}
	}
	chosen, err := z.transition(lo, work)
	if err != nil {
		return 0, 0, 0, err
	}
	if lo == z.count-1 && z.rules.valid {
		ruleOffset, start, end := z.rules.lookup(chosen.when, sec)
		return int64(ruleOffset), start, end, nil
	}
	return chosen.offset, chosen.when, end, nil
}

// wall returns UTC-tagged civil fields after the selected offset is applied.
// The returned value is only for field extraction, never an absolute candidate.
func (z *PreparedTimezone) wall(sec int64, work *timezoneWork) (time.Time, error) {
	offset, _, _, err := z.lookup(sec, work)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(sec+int64(offset), 0).UTC(), nil
}

// date preserves Go Date's initial wall-as-UTC lookup and optional second lookup.
func (z *PreparedTimezone) date(year, month, day, hour, minute, second int, work *timezoneWork) (time.Time, error) {
	unixSec := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC).Unix()
	offset, start, end, err := z.lookup(unixSec, work)
	if err != nil {
		return time.Time{}, err
	}
	if offset != 0 {
		adjusted := unixSec - int64(offset)
		if adjusted < start || adjusted >= end {
			offset, _, _, err = z.lookup(adjusted, work)
			if err != nil {
				return time.Time{}, err
			}
		}
		unixSec -= int64(offset)
	}
	return time.Unix(unixSec, 0).UTC(), nil
}

func (z *PreparedTimezone) setTransitions(transitions []preparedTZTransition) {
	z.pages = make([]preparedTimezonePage, (len(transitions)+preparedTimezonePageSize-1)/preparedTimezonePageSize)
	for i, tx := range transitions {
		page := &z.pages[i/preparedTimezonePageSize]
		page.identity = z.identity
		page.ordinal = i / preparedTimezonePageSize
		page.entries[i%preparedTimezonePageSize] = tx
		page.length++
	}
}
