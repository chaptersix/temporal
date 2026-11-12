//go:build !test_dep

package chasm

import "reflect"

type workObservation struct{}

func newWorkObservation(NodeBackend) workObservation { return workObservation{} }
func (*Node) noteWork(workCounter, int)              {}
func (*Node) noteDecode()                            {}
func (*Node) noteSerialize()                         {}

func (*Node) noteMapKeyCopy(reflect.Value) {}
