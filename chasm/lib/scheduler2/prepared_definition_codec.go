package scheduler2

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"go.temporal.io/server/chasm/lib/scheduler2/internal/occurrence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const definitionHeaderBytes = 91
const definitionTermsPerPage = 16
const definitionSeedPageBytes = 24
const definitionTermPageBytes = 1570

type definitionHeader struct {
	identity                     [32]byte
	terms, exclusions, seedBytes uint64
	start, end                   time.Time
	hasStart, hasEnd             bool
	jitter                       time.Duration
}

func (h definitionHeader) encode() []byte {
	out := make([]byte, definitionHeaderBytes)
	out[0] = 1
	copy(out[1:33], h.identity[:])
	binary.BigEndian.PutUint64(out[33:41], h.terms)
	binary.BigEndian.PutUint64(out[41:49], h.exclusions)
	binary.BigEndian.PutUint64(out[49:57], h.seedBytes)
	putDefinitionTime(out[57:70], h.start, h.hasStart)
	putDefinitionTime(out[70:83], h.end, h.hasEnd)
	binary.BigEndian.PutUint64(out[83:91], uint64(h.jitter))
	return out
}
func putDefinitionTime(out []byte, t time.Time, present bool) {
	if !present {
		return
	}
	out[0] = 1
	binary.BigEndian.PutUint64(out[1:9], uint64(t.Unix()))
	binary.BigEndian.PutUint32(out[9:13], uint32(t.Nanosecond()))
}
func readDefinitionTime(in []byte) (time.Time, bool, error) {
	if in[0] > 1 {
		return time.Time{}, false, errors.New("invalid definition timestamp presence")
	}
	sec := int64(binary.BigEndian.Uint64(in[1:9]))
	nano := int32(binary.BigEndian.Uint32(in[9:13]))
	if in[0] == 0 {
		if sec != 0 || nano != 0 {
			return time.Time{}, false, errors.New("invalid absent definition timestamp")
		}
		return time.Time{}, false, nil
	}
	timestamp := &timestamppb.Timestamp{Seconds: sec, Nanos: nano}
	if err := timestamp.CheckValid(); err != nil {
		return time.Time{}, false, err
	}
	return timestamp.AsTime(), true, nil
}
func decodeDefinitionHeader(in []byte) (definitionHeader, error) {
	var h definitionHeader
	if len(in) != definitionHeaderBytes || in[0] != 1 {
		return h, errors.New("invalid definition header")
	}
	copy(h.identity[:], in[1:33])
	h.terms = binary.BigEndian.Uint64(in[33:41])
	h.exclusions = binary.BigEndian.Uint64(in[41:49])
	h.seedBytes = binary.BigEndian.Uint64(in[49:57])
	h.jitter = time.Duration(binary.BigEndian.Uint64(in[83:91]))
	var err error
	h.start, h.hasStart, err = readDefinitionTime(in[57:70])
	if err != nil {
		return h, err
	}
	h.end, h.hasEnd, err = readDefinitionTime(in[70:83])
	if err != nil {
		return h, err
	}
	if h.identity == [32]byte{} || h.jitter < 0 || h.seedBytes > uint64(math.MaxInt)-15 {
		return h, errors.New("invalid definition header identity or bounds")
	}
	_, _, err = h.pages()
	return h, err
}
func definitionPageCount(n, width uint64) uint64 {
	count := n / width
	if n%width != 0 {
		count++
	}
	return count
}
func (h definitionHeader) pages() (terms, total uint64, err error) {
	if h.exclusions > math.MaxUint64-h.terms {
		return 0, 0, errors.New("definition term count overflow")
	}
	terms = definitionPageCount(h.terms+h.exclusions, definitionTermsPerPage)
	seeds := definitionPageCount(h.seedBytes, definitionSeedPageBytes)
	if seeds > math.MaxUint64-terms {
		return 0, 0, errors.New("definition page count overflow")
	}
	return terms, terms + seeds, nil
}
func definitionOwner(identity [32]byte, revision int64) [32]byte {
	var input [40]byte
	copy(input[:32], identity[:])
	binary.BigEndian.PutUint64(input[32:], uint64(revision))
	return sha256.Sum256(input[:])
}
func definitionTermID(owner [32]byte, ordinal uint64) [32]byte {
	var input [40]byte
	copy(input[:32], owner[:])
	binary.BigEndian.PutUint64(input[32:], ordinal)
	return sha256.Sum256(input[:])
}
func encodeDefinitionTerms(terms []occurrence.PreparedSpecTerm) []byte {
	out := make([]byte, 2, definitionTermPageBytes)
	out[0] = 1
	out[1] = byte(len(terms))
	for _, term := range terms {
		if term.IsInterval {
			out = append(out, 2)
			out = binary.BigEndian.AppendUint64(out, uint64(term.Interval))
			out = binary.BigEndian.AppendUint64(out, uint64(term.Phase))
			continue
		}
		out = append(out, 1)
		out = append(out, term.Calendar.SourceID[:]...)
		for _, mask := range term.Calendar.Masks.Fields {
			out = binary.BigEndian.AppendUint64(out, mask)
		}
		for _, mask := range term.Calendar.Masks.Years {
			out = binary.BigEndian.AppendUint64(out, mask)
		}
		if term.Calendar.Masks.AllYears {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}
	}
	return out
}

// decodeDefinitionTerms validates at most sixteen detached records. Identity
// validation belongs to the page owner and cannot be bypassed by interval tags.
func decodeDefinitionTerms(encoded []byte, h definitionHeader, owner [32]byte, ordinal uint64) ([definitionTermsPerPage]occurrence.PreparedSpecTerm, error) {
	var out [definitionTermsPerPage]occurrence.PreparedSpecTerm
	termPages, _, err := h.pages()
	if err != nil {
		return out, err
	}
	if ordinal >= termPages || len(encoded) < 2 || len(encoded) > definitionTermPageBytes || encoded[0] != 1 {
		return out, errors.New("invalid definition term page")
	}
	first := ordinal * definitionTermsPerPage
	count := min(uint64(definitionTermsPerPage), h.terms+h.exclusions-first)
	if uint64(encoded[1]) != count {
		return out, errors.New("invalid definition term page count")
	}
	rest := encoded[2:]
	for i := uint64(0); i < count; i++ {
		term, n, err := decodeDefinitionTerm(rest)
		if err != nil {
			return out, err
		}
		rest = rest[n:]
		if term.IsInterval {
			if first+i >= h.terms {
				return out, errors.New("interval in exclusion page")
			}
		} else if term.Calendar.SourceID != definitionTermID(owner, first+i) {
			return out, errors.New("invalid calendar term identity")
		}
		out[i] = term
	}
	if len(rest) != 0 {
		return out, errors.New("trailing definition term bytes")
	}
	return out, nil
}
func decodeDefinitionTerm(encoded []byte) (occurrence.PreparedSpecTerm, int, error) {
	var term occurrence.PreparedSpecTerm
	if len(encoded) == 0 {
		return term, 0, errors.New("missing definition term")
	}
	switch encoded[0] {
	case 2:
		if len(encoded) < 17 {
			return term, 0, errors.New("truncated definition interval")
		}
		term.IsInterval = true
		term.Interval = int64(binary.BigEndian.Uint64(encoded[1:9]))
		term.Phase = int64(binary.BigEndian.Uint64(encoded[9:17]))
		if term.Interval <= 0 || term.Phase < 0 {
			return term, 0, errors.New("invalid definition interval")
		}
		return term, 17, nil
	case 1:
		if len(encoded) < 98 {
			return term, 0, errors.New("truncated definition calendar")
		}
		copy(term.Calendar.SourceID[:], encoded[1:33])
		offset := 33
		for i := range term.Calendar.Masks.Fields {
			term.Calendar.Masks.Fields[i] = binary.BigEndian.Uint64(encoded[offset : offset+8])
			offset += 8
		}
		for i := range term.Calendar.Masks.Years {
			term.Calendar.Masks.Years[i] = binary.BigEndian.Uint64(encoded[offset : offset+8])
			offset += 8
		}
		term.Calendar.Masks.AllYears = encoded[97] == 1
		if encoded[97] > 1 || !validDefinitionMasks(term.Calendar.Masks) {
			return term, 0, errors.New("invalid definition masks")
		}
		return term, 98, nil
	default:
		return term, 0, errors.New("unknown definition term kind")
	}
}
func validDefinitionMasks(m occurrence.CalendarMasks) bool {
	allowed := [6]uint64{1<<60 - 1, 1<<60 - 1, 1<<24 - 1, 1<<32 - 2, 1<<13 - 2, 1<<7 - 1}
	for i, mask := range m.Fields {
		if mask & ^allowed[i] != 0 {
			return false
		}
	}
	return m.Years[1]>>37 == 0 && (!m.AllYears || m.Years == [2]uint64{})
}
func decodeDefinitionSeed(encoded []byte, h definitionHeader, ordinal uint64) ([]byte, error) {
	termPages, total, err := h.pages()
	if err != nil {
		return nil, err
	}
	if ordinal < termPages || ordinal >= total || len(encoded) < 2 || encoded[0] != 2 {
		return nil, errors.New("invalid definition seed page")
	}
	count := min(uint64(definitionSeedPageBytes), h.seedBytes-(ordinal-termPages)*definitionSeedPageBytes)
	if uint64(encoded[1]) != count || uint64(len(encoded)) != count+2 {
		return nil, errors.New("invalid definition seed page count")
	}
	return encoded[2:], nil
}
