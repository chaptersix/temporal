package scheduler2

import (
	"math"

	"go.temporal.io/api/serviceerror"
	pb "go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
)

// prepareStartDispatch reconciles the fixed slot on the editor's detached
// control. An unrelated operation cannot replace an active claim's authority.
func prepareStartDispatch(editor *operationReadinessEditor, replace bool) (bool, error) {
	control := editor.control.StartDispatch
	if claim := control.Claim; claim != nil {
		return setStartDispatchWake(control, &claim.Expiry, replace)
	}
	next, err := editor.minimum()
	if err != nil {
		return false, err
	}
	var wake *int64
	if next != nil {
		wake = &next.ReadyAt
	}
	return setStartDispatchWake(control, wake, replace || editor.claimReleased)
}

func setStartDispatchWake(control *pb.StartDispatchControl, wake *int64, replace bool) (bool, error) {
	if control == nil || control.Token == 0 {
		return false, serviceerror.NewInternal("invalid start dispatcher control")
	}
	armed := wake != nil
	var at int64
	if armed {
		at = *wake
	}
	if !replace && control.Armed == armed && (!armed || control.At == at) {
		return false, nil
	}
	if control.Token == math.MaxUint64 {
		return false, serviceerror.NewFailedPrecondition("start dispatcher token exhausted")
	}
	control.Token++
	control.Armed, control.At = armed, at
	return true, nil
}
