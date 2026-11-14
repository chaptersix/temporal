package occurrence

import (
	"encoding/binary"
	"errors"
)

const preparedTZAlpha int64 = -1 << 63
const preparedTZOmega int64 = 1<<63 - 1

type preparedTZZone struct {
	offset   int
	daylight bool
}
type preparedTZTransition struct {
	when   int64
	offset int
}
type preparedTZParsed struct {
	transitions []preparedTZTransition
	firstOffset int
	rules       preparedTZRules
}

// parsePreparedTZData retains Go's positional transition semantics. Advertised
// counts are checked against remaining bytes before allocating their arrays.
func parsePreparedTZData(data []byte) (preparedTZParsed, error) {
	bad := errors.New("malformed prepared timezone data")
	if len(data) < 44 || string(data[:4]) != "TZif" {
		return preparedTZParsed{}, bad
	}
	version := data[4]
	if version != 0 && version != '2' && version != '3' {
		return preparedTZParsed{}, bad
	}
	counts := func(raw []byte) [6]uint64 {
		var n [6]uint64
		for i := range n {
			n[i] = uint64(binary.BigEndian.Uint32(raw[4*i:]))
		}
		return n
	}
	n := counts(data[20:44])
	data = data[44:]
	size := uint64(4)
	if version != 0 {
		skip := n[3]*5 + n[4]*6 + n[5] + n[2]*8 + n[1] + n[0] + 20
		if skip+24 > uint64(len(data)) {
			return preparedTZParsed{}, bad
		}
		data = data[int(skip):]
		n = counts(data[:24])
		data = data[24:]
		size = 8
	}
	needed := n[3]*(size+1) + n[4]*6 + n[5] + n[2]*(size+4) + n[1] + n[0]
	if n[4] == 0 || needed > uint64(len(data)) {
		return preparedTZParsed{}, bad
	}
	times := data[:int(n[3]*size)]
	data = data[len(times):]
	indices := data[:int(n[3])]
	data = data[len(indices):]
	zoneData := data[:int(n[4]*6)]
	data = data[len(zoneData):]
	abbreviations := data[:int(n[5])]
	data = data[len(abbreviations):]
	data = data[int(n[2]*(size+4)+n[1]+n[0]):]
	zones := make([]preparedTZZone, int(n[4]))
	for i := range zones {
		row := zoneData[i*6:]
		if int(row[5]) >= len(abbreviations) {
			return preparedTZParsed{}, bad
		}
		zones[i] = preparedTZZone{offset: int(int32(binary.BigEndian.Uint32(row))), daylight: row[4] != 0}
	}
	tx := make([]preparedTZTransition, int(n[3]))
	firstUsed := false
	for i := range tx {
		if int(indices[i]) >= len(zones) {
			return preparedTZParsed{}, bad
		}
		var when int64
		if size == 4 {
			when = int64(int32(binary.BigEndian.Uint32(times[i*4:])))
		} else {
			when = int64(binary.BigEndian.Uint64(times[i*8:]))
		}
		tx[i] = preparedTZTransition{when: when, offset: zones[indices[i]].offset}
		firstUsed = firstUsed || indices[i] == 0
	}
	first := preparedFirstZone(zones, indices, firstUsed)
	if len(tx) == 0 {
		tx = []preparedTZTransition{{when: preparedTZAlpha, offset: zones[0].offset}}
	}
	var rules preparedTZRules
	if len(data) > 2 && data[0] == '\n' && data[len(data)-1] == '\n' {
		rules = parsePreparedTZRules(string(data[1 : len(data)-1]))
	}
	return preparedTZParsed{transitions: tx, firstOffset: zones[first].offset, rules: rules}, nil
}

func preparedFirstZone(zones []preparedTZZone, indices []byte, firstUsed bool) int {
	first := 0
	if firstUsed {
		first = -1
		if len(indices) > 0 && zones[indices[0]].daylight {
			for i := int(indices[0]) - 1; i >= 0; i-- {
				if !zones[i].daylight {
					first = i
					break
				}
			}
		}
		if first < 0 {
			for i, z := range zones {
				if !z.daylight {
					first = i
					break
				}
			}
		}
		if first < 0 {
			first = 0
		}
	}
	return first
}
