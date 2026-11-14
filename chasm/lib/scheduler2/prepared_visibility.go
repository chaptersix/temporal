package scheduler2

import "go.temporal.io/server/chasm"

// VisibilityReady keeps initial List and Count publication together until the
// selected prepared profile has a coherent static projection and preview.
func (s *Scheduler) VisibilityReady(ctx chasm.Context) bool {
	if !s.ListControl.IsSet() {
		return true
	}
	control, err := s.listControl(ctx)
	if err != nil {
		panic(err) //nolint:forbidigo // Framework invariant failure, as with Memo and DATA decoding.
	}
	return control.PublishedStamp != 0
}
