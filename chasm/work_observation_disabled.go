//go:build !test_dep

package chasm

import "reflect"

type workObservation struct{}

func newWorkObservation(NodeBackend) workObservation { return workObservation{} }
func (*Node) noteWork(workCounter, int)              {}
func (*Node) noteDecode()                            {}
func (*Node) noteSerialize()                         {}

func (*Node) noteMapKeyCopy(reflect.Value) {}

func ObservePackedRead(Context, int, int)    {}
func ObservePackedWrite(Context, int, int)   {}
func ObservePackedRecordDecode(Context, int) {}
func ObservePackedBackendRead(Context, int)  {}

func ObservePackedRecordEncode(Context, int) {}
