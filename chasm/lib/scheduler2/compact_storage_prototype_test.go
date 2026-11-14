//go:build test_dep

package scheduler2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// These codecs are storage experiments only. They are not registered runtime formats.
// A page has exactly 16 directory entries; metadata and request pages are disjoint.
func compactPage(slots [16][]byte) []byte {
	out := make([]byte, 65)
	out[0] = 1
	for i, slot := range slots {
		binary.LittleEndian.PutUint32(out[1+4*i:], uint32(len(slot)))
		out = append(out, slot...)
	}
	return out
}
func expandCompactPage(wire []byte) ([16][]byte, error) {
	var slots [16][]byte
	if len(wire) < 65 || wire[0] != 1 {
		return slots, errors.New("invalid compact page")
	}
	pos := 65
	for i := range slots {
		n := uint64(binary.LittleEndian.Uint32(wire[1+4*i:]))
		if n > uint64(len(wire)-pos) {
			return slots, errors.New("truncated compact slot")
		}
		if n != 0 {
			slots[i] = bytes.Clone(wire[pos : pos+int(n)])
		}
		pos += int(n)
	}
	if pos != len(wire) {
		return slots, errors.New("trailing compact bytes")
	}
	return slots, nil
}
func compactBlob(dst, value []byte) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(value))), value...)
}
func compactTake(wire *[]byte) ([]byte, error) {
	n, k := binary.Uvarint(*wire)
	if k <= 0 || n > uint64(len(*wire)-k) {
		return nil, errors.New("invalid compact blob")
	}
	value := (*wire)[k : k+int(n)]
	*wire = (*wire)[k+int(n):]
	return value, nil
}
func compactFields(wire []byte) ([][]byte, error) {
	var fields [][]byte
	for len(wire) > 0 {
		_, _, n := protowire.ConsumeField(wire)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		fields = append(fields, wire[:n])
		wire = wire[n:]
	}
	return fields, nil
}

// Template deltas retain exact wire bytes, including unknown fields and duplicate
// tags. They do not regenerate callbacks or re-marshal reconstructed requests.
func compactRequest(template, wire []byte) ([]byte, error) {
	base, err := compactFields(template)
	if err != nil {
		return nil, err
	}
	fields, err := compactFields(wire)
	if err != nil {
		return nil, err
	}
	if len(base) != len(fields) {
		return append([]byte{0}, wire...), nil
	}
	out := []byte{1}
	for i, field := range fields {
		prefix := 0
		for prefix < len(field) && prefix < len(base[i]) && field[prefix] == base[i][prefix] {
			prefix++
		}
		suffix := 0
		for suffix < len(field)-prefix && suffix < len(base[i])-prefix && field[len(field)-1-suffix] == base[i][len(base[i])-1-suffix] {
			suffix++
		}
		out = binary.AppendUvarint(out, uint64(prefix))
		out = binary.AppendUvarint(out, uint64(suffix))
		out = compactBlob(out, field[prefix:len(field)-suffix])
	}
	if len(out) >= len(wire)+1 {
		return append([]byte{0}, wire...), nil
	}
	return out, nil
}
func expandCompactRequest(template, wire []byte) ([]byte, error) {
	if len(wire) == 0 {
		return nil, errors.New("missing request mode")
	}
	if wire[0] == 0 {
		return bytes.Clone(wire[1:]), nil
	}
	if wire[0] != 1 {
		return nil, errors.New("invalid request mode")
	}
	fields, err := compactFields(template)
	if err != nil {
		return nil, err
	}
	wire = wire[1:]
	var out []byte
	for _, field := range fields {
		prefix, n := binary.Uvarint(wire)
		if n <= 0 {
			return nil, errors.New("invalid request prefix")
		}
		wire = wire[n:]
		suffix, n := binary.Uvarint(wire)
		if n <= 0 {
			return nil, errors.New("invalid request suffix")
		}
		wire = wire[n:]
		if prefix > uint64(len(field)) || suffix > uint64(len(field))-prefix {
			return nil, errors.New("invalid request splice")
		}
		middle, err := compactTake(&wire)
		if err != nil {
			return nil, err
		}
		out = append(out, field[:int(prefix)]...)
		out = append(out, middle...)
		out = append(out, field[len(field)-int(suffix):]...)
	}
	if len(wire) != 0 {
		return nil, errors.New("trailing request delta")
	}
	return out, nil
}
func compactOccurrence(op *pb.OperationState) string {
	occurrence := fmt.Sprintf("automatic/%d", op.Nominal)
	if op.ManualId != 0 {
		occurrence = fmt.Sprintf("manual/%d", op.ManualId)
	}
	if op.BackfillId != 0 {
		nominal := fmt.Sprint(op.Nominal)
		if op.NominalTime != nil {
			nominal = op.NominalTime.AsTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		occurrence = fmt.Sprintf("backfill/%d/%s", op.BackfillId, nominal)
	}
	return occurrence
}
func compactOperation(incarnation string, op *pb.OperationState) ([]byte, error) {
	occurrence := compactOccurrence(op)
	action := incarnation + "/" + occurrence + "/action"
	copyOp := common.CloneProto(op)
	mode := byte(0)
	if op.OccurrenceId == occurrence && op.ActionId == action && op.Id == action+"/start" {
		mode = 1
		copyOp.OccurrenceId = ""
		copyOp.ActionId = ""
		copyOp.Id = ""
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(copyOp)
	return append([]byte{mode}, wire...), err
}
func expandCompactOperation(incarnation string, wire []byte) (*pb.OperationState, error) {
	if len(wire) == 0 || wire[0] > 1 {
		return nil, errors.New("invalid operation mode")
	}
	op := &pb.OperationState{}
	if err := proto.Unmarshal(wire[1:], op); err != nil {
		return nil, err
	}
	if wire[0] == 1 {
		op.OccurrenceId = compactOccurrence(op)
		op.ActionId = incarnation + "/" + op.OccurrenceId + "/action"
		op.Id = op.ActionId + "/start"
	}
	return op, nil
}
