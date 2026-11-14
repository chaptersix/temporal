package occurrence

// The long-input Farm32 arithmetic is adapted from github.com/dgryski/go-farm,
// farmhashmk.go at 3414d57e47da. See prepared_jitter_license.txt.
import (
	"encoding/binary"
	"errors"
	"math/bits"
	"time"

	"github.com/dgryski/go-farm"
)

const farmC1 uint32 = 0xcc9e2d51
const farmC2 uint32 = 0x1b873593

// SeedWindow is a bounded arbitrary-offset read of an immutable jitter seed.
// Count is min(24, seed length-offset); trailing bytes are ignored.
type SeedWindow struct {
	Bytes [24]byte
	Count uint8
}

// JitterHashState retains the exact Farm32 accumulation without retaining seed bytes.
type JitterHashState struct {
	Started, Done bool
	Offset        uint64
	H, G, F, Hash uint32
}

func farmMur(a, h uint32) uint32 {
	a = bits.RotateLeft32(a*farmC1, -17) * farmC2
	h ^= a
	return bits.RotateLeft32(h, -19)*5 + 0xe6546b64
}

func jitterWindow(nominal time.Time, length, offset uint64, count int, read func(uint64) (SeedWindow, error)) ([24]byte, error) {
	var out [24]byte
	prefix, err := nominal.UTC().MarshalBinary()
	if err != nil {
		return out, err
	}
	// UTC MarshalBinary has a fixed 15-byte representation.
	if len(prefix) != 15 || count < 0 || count > 24 || offset > length+15 || uint64(count) > length+15-offset {
		return out, errors.New("invalid jitter window")
	}
	copied := 0
	if offset < 15 {
		copied = copy(out[:count], prefix[offset:])
		offset += uint64(copied)
	}
	if copied == count {
		return out, nil
	}
	seedOffset := offset - 15
	page, err := read(seedOffset)
	if err != nil {
		return out, err
	}
	if uint64(page.Count) != min(uint64(24), length-seedOffset) {
		return out, errors.New("invalid jitter seed window")
	}
	copy(out[copied:count], page.Bytes[:count-copied])
	return out, nil
}

// advanceJitterHash performs one fixed-size hash step. Its caller fences the
// nominal instant and immutable seed identity across continuations.
func advanceJitterHash(s JitterHashState, nominal time.Time, length uint64, read func(uint64) (SeedWindow, error)) (JitterHashState, error) {
	if length > uint64(^uint(0)>>1)-15 || read == nil {
		return s, errors.New("invalid jitter seed source")
	}
	original := s
	total := length + 15
	if s.Done {
		return s, nil
	}
	if !s.Started {
		n := 20
		offset := total - 20
		if total <= 24 {
			n = int(total)
			offset = 0
		}
		b, err := jitterWindow(nominal, length, offset, n, read)
		if err != nil {
			return original, err
		}
		s.Started = true
		if total <= 24 {
			s.Hash = farm.Fingerprint32(b[:n])
			s.Done = true
			return s, nil
		}
		s.H = uint32(total)
		s.G = farmC1 * uint32(total)
		s.F = s.G
		a := func(i int) uint32 {
			return bits.RotateLeft32(binary.LittleEndian.Uint32(b[i:i+4])*farmC1, -17) * farmC2
		}
		s.H = bits.RotateLeft32(s.H^a(16), -19)*5 + 0xe6546b64
		s.H = bits.RotateLeft32(s.H^a(4), -19)*5 + 0xe6546b64
		s.G = bits.RotateLeft32(s.G^a(12), -19)*5 + 0xe6546b64
		s.G = bits.RotateLeft32(s.G^a(8), -19)*5 + 0xe6546b64
		s.F = bits.RotateLeft32(s.F+a(0), -19) + 113
		return s, nil
	}
	if s.Offset > total || s.Offset%20 != 0 {
		return original, errors.New("invalid jitter hash cursor")
	}
	if total-s.Offset > 20 {
		b, err := jitterWindow(nominal, length, s.Offset, 20, read)
		if err != nil {
			return original, err
		}
		a := binary.LittleEndian.Uint32(b[0:4])
		c := binary.LittleEndian.Uint32(b[8:12])
		d := binary.LittleEndian.Uint32(b[12:16])
		e := binary.LittleEndian.Uint32(b[16:20])
		v := binary.LittleEndian.Uint32(b[4:8])
		s.H += a
		s.G += v
		s.F += c
		s.H = farmMur(d, s.H) + e
		s.G = farmMur(c, s.G) + a
		s.F = farmMur(v+e*farmC1, s.F) + d
		s.F += s.G
		s.G += s.F
		s.Offset += 20
		return s, nil
	}
	g := bits.RotateLeft32(bits.RotateLeft32(s.G, -11)*farmC1, -17) * farmC1
	f := bits.RotateLeft32(bits.RotateLeft32(s.F, -11)*farmC1, -17) * farmC1
	h := bits.RotateLeft32(s.H+g, -19)*5 + 0xe6546b64
	h = bits.RotateLeft32(h, -17) * farmC1
	h = bits.RotateLeft32(h+f, -19)*5 + 0xe6546b64
	s.Hash = bits.RotateLeft32(h, -17) * farmC1
	s.Done = true
	return s, nil
}
