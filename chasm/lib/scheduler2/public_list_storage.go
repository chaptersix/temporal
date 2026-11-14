package scheduler2

import (
	"bytes"
	"errors"
	"math"

	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler2/gen/scheduler2pb/v1"
	"go.temporal.io/server/common"
	legacy "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// publicListPlan is transaction-local. Its publisher performs no source reads.
// The caller reserves MemoVersion before publishing a plan with memoChanged.
type publicListPlan struct {
	control               *scheduler2pb.PublicListControl
	static                *schedulepb.ScheduleListInfo
	staticSlot, clearSlot uint32
	notes                 *string
	memoChanged           bool
}

// stagePublicListStatic is input-proportional ingress preparation. It must run
// outside mutation, including when only the live list truncation limit changes.
func stagePublicListStatic(schedule *schedulepb.Schedule, limit int64) (*schedulepb.ScheduleListInfo, error) {
	if schedule == nil || schedule.Spec == nil || limit < 0 {
		return nil, errors.New("invalid static list input")
	}
	spec := common.CloneProto(schedule.Spec)
	legacy.CleanSpec(spec)
	spec.TimezoneData = nil
	spec.Interval = spec.Interval[:min(int64(len(spec.Interval)), limit)]
	spec.StructuredCalendar = spec.StructuredCalendar[:min(int64(len(spec.StructuredCalendar)), limit)]
	spec.ExcludeStructuredCalendar = spec.ExcludeStructuredCalendar[:min(int64(len(spec.ExcludeStructuredCalendar)), limit)]
	return &schedulepb.ScheduleListInfo{Spec: spec, WorkflowType: common.CloneProto(schedule.GetAction().GetStartWorkflow().GetWorkflowType())}, nil
}

func listSlotOwner(c *scheduler2pb.PublicListControl, slot uint32) []byte {
	if slot == 1 {
		return c.SlotAOwner
	}
	if slot == 2 {
		return c.SlotBOwner
	}
	return nil
}
func setListSlotOwner(c *scheduler2pb.PublicListControl, slot uint32, owner []byte) {
	if slot == 1 {
		c.SlotAOwner = bytes.Clone(owner)
	} else {
		c.SlotBOwner = bytes.Clone(owner)
	}
}
func (s *Scheduler) listSlot(slot uint32) chasm.Field[*schedulepb.ScheduleListInfo] {
	if slot == 1 {
		return s.ListStaticA
	}
	return s.ListStaticB
}
func (s *Scheduler) setListSlot(slot uint32, value chasm.Field[*schedulepb.ScheduleListInfo]) {
	if slot == 1 {
		s.ListStaticA = value
	} else {
		s.ListStaticB = value
	}
}
func validListOwner(owner []byte) bool {
	return len(owner) == 32 && !bytes.Equal(owner, make([]byte, 32))
}

func (s *Scheduler) listControl(ctx chasm.Context) (*scheduler2pb.PublicListControl, error) {
	c, ok := s.ListControl.TryGet(ctx)
	if !ok {
		if s.ListStaticA.IsSet() || s.ListStaticB.IsSet() || s.ListNotes.IsSet() {
			return nil, errors.New("orphan list partition")
		}
		return &scheduler2pb.PublicListControl{}, nil
	}
	if c == nil || c.PublishedSlot > 2 || c.PendingSlot > 2 || len(c.RecentActions) > 5 || len(c.FutureActionTimes) > 5 || c.PublishedListLimit < 0 || c.PendingListLimit < 0 {
		return nil, errors.New("invalid list control")
	}
	if (c.PublishedSlot == 0) != (c.PublishedStamp == 0) || (c.PendingSlot == 0) != (c.PendingStamp == 0) || (c.PendingStamp != 0 && c.PendingStamp <= c.PublishedStamp) || (c.PublishedSlot == 0 && len(c.FutureActionTimes) != 0) {
		return nil, errors.New("invalid list publication stamp")
	}
	if err := s.validateListSlots(c); err != nil {
		return nil, err
	}
	for _, action := range c.RecentActions {
		if action == nil {
			return nil, errors.New("invalid recent list action")
		}
	}
	if err := validListFuture(c.FutureActionTimes); err != nil {
		return nil, err
	}
	return c, nil
}
func (s *Scheduler) validateListSlots(c *scheduler2pb.PublicListControl) error {
	for slot := uint32(1); slot <= 2; slot++ {
		owner := listSlotOwner(c, slot)
		referenced := c.PublishedSlot == slot || c.PendingSlot == slot
		if referenced != s.listSlot(slot).IsSet() || (referenced && !validListOwner(owner)) || (!referenced && len(owner) != 0) {
			return errors.New("invalid list slot ownership")
		}
	}
	return nil
}

func validListFuture(future []*timestamppb.Timestamp) error {
	if len(future) > 5 {
		return errors.New("list future exceeds fixed count")
	}
	for _, at := range future {
		if at == nil || at.CheckValid() != nil {
			return errors.New("invalid list future timestamp")
		}
	}
	return nil
}

func validateStagedListStatic(static *schedulepb.ScheduleListInfo) error {
	if static != nil && (static.Spec == nil || static.Spec.TimezoneData != nil || static.Notes != "" || static.Paused || len(static.RecentActions) != 0 || len(static.FutureActionTimes) != 0) {
		return errors.New("invalid staged list static")
	}
	return nil
}

func (s *Scheduler) prepareListRefresh(ctx chasm.Context, owner [32]byte, static *schedulepb.ScheduleListInfo, limit int64, recent []*schedulepb.ScheduleActionResult, notes *string) (publicListPlan, error) {
	c, err := s.listControl(ctx)
	if err != nil {
		return publicListPlan{}, err
	}
	if owner == [32]byte{} || limit < 0 || len(recent) > 5 {
		return publicListPlan{}, errors.New("invalid list refresh")
	}
	for _, a := range recent {
		if a == nil {
			return publicListPlan{}, errors.New("invalid recent list action")
		}
	}
	stamp := max(c.PublishedStamp, c.PendingStamp)
	if stamp == math.MaxUint64 {
		return publicListPlan{}, errors.New("list preview stamp exhausted")
	}
	if static == nil && (c.PublishedSlot == 0 || !bytes.Equal(listSlotOwner(c, c.PublishedSlot), owner[:]) || limit != c.PublishedListLimit) {
		return publicListPlan{}, errors.New("list static restaging required")
	}
	if err := validateStagedListStatic(static); err != nil {
		return publicListPlan{}, err
	}
	next := common.CloneProto(c)
	next.PendingStamp = stamp + 1
	next.PendingListLimit = limit
	next.RecentActions = nil
	for _, a := range recent {
		next.RecentActions = append(next.RecentActions, common.CloneProto(a))
	}
	var detachedNotes *string
	if notes != nil {
		value := *notes
		detachedNotes = &value
	}
	plan := publicListPlan{control: next, static: static, notes: detachedNotes, memoChanged: notes != nil || !proto.Equal(&scheduler2pb.PublicListControl{RecentActions: c.RecentActions}, &scheduler2pb.PublicListControl{RecentActions: next.RecentActions})}
	if static != nil {
		next.PendingSlot = 1
		if c.PublishedSlot == 1 {
			next.PendingSlot = 2
		}
		plan.staticSlot = next.PendingSlot
		setListSlotOwner(next, next.PendingSlot, owner[:])
	} else {
		next.PendingSlot = c.PublishedSlot
		if c.PendingSlot != 0 && c.PendingSlot != c.PublishedSlot {
			plan.clearSlot = c.PendingSlot
			setListSlotOwner(next, c.PendingSlot, nil)
		}
	}
	return plan, nil
}

// prepareListDynamic exposes recent/notes changes without canceling unfinished
// preview work. Callers request a new preview only when its semantic inputs change.
func (s *Scheduler) prepareListDynamic(ctx chasm.Context, recent []*schedulepb.ScheduleActionResult, notes *string) (publicListPlan, error) {
	c, err := s.listControl(ctx)
	if err != nil {
		return publicListPlan{}, err
	}
	if len(recent) > 5 {
		return publicListPlan{}, errors.New("recent list exceeds fixed count")
	}
	next := common.CloneProto(c)
	next.RecentActions = nil
	for _, action := range recent {
		if action == nil {
			return publicListPlan{}, errors.New("invalid recent list action")
		}
		next.RecentActions = append(next.RecentActions, common.CloneProto(action))
	}
	var detachedNotes *string
	if notes != nil {
		value := *notes
		detachedNotes = &value
	}
	changed := notes != nil || !proto.Equal(&scheduler2pb.PublicListControl{RecentActions: c.RecentActions}, &scheduler2pb.PublicListControl{RecentActions: next.RecentActions})
	return publicListPlan{control: next, notes: detachedNotes, memoChanged: changed}, nil
}

func (s *Scheduler) publishListRefresh(ctx chasm.MutableContext, plan publicListPlan) {
	if plan.static != nil {
		s.setListSlot(plan.staticSlot, chasm.NewDataField(ctx, plan.static))
	}
	if plan.clearSlot != 0 {
		s.setListSlot(plan.clearSlot, chasm.NewEmptyField[*schedulepb.ScheduleListInfo]())
	}
	if plan.notes != nil {
		s.ListNotes = chasm.NewDataField(ctx, &scheduler2pb.Payload{Data: []byte(*plan.notes)})
	}
	s.ListControl = chasm.NewDataField(ctx, plan.control)
	if plan.memoChanged {
		s.publishMemoVersion(ctx)
	}
}

func (s *Scheduler) prepareListWindow(ctx chasm.Context, owner [32]byte, stamp uint64, future []*timestamppb.Timestamp) (publicListPlan, bool, error) {
	c, err := s.listControl(ctx)
	if err != nil {
		return publicListPlan{}, false, err
	}
	if stamp == 0 || c.PendingStamp != stamp || !bytes.Equal(listSlotOwner(c, c.PendingSlot), owner[:]) {
		return publicListPlan{}, false, nil
	}
	if err := validListFuture(future); err != nil {
		return publicListPlan{}, false, err
	}
	next := common.CloneProto(c)
	next.PublishedSlot = c.PendingSlot
	next.PublishedStamp = stamp
	next.PublishedListLimit = c.PendingListLimit
	next.PendingSlot = 0
	next.PendingStamp = 0
	next.PendingListLimit = 0
	next.FutureActionTimes = nil
	for _, at := range future {
		next.FutureActionTimes = append(next.FutureActionTimes, common.CloneProto(at))
	}
	plan := publicListPlan{control: next, memoChanged: true}
	if c.PublishedSlot != 0 && c.PublishedSlot != next.PublishedSlot {
		plan.clearSlot = c.PublishedSlot
		setListSlotOwner(next, c.PublishedSlot, nil)
	}
	return plan, true, nil
}
func (s *Scheduler) publishListWindow(ctx chasm.MutableContext, plan publicListPlan) {
	s.publishListRefresh(ctx, plan)
}

func (s *Scheduler) listMemo(ctx chasm.Context) (*schedulepb.ScheduleListInfo, error) {
	c, err := s.listControl(ctx)
	if err != nil {
		return nil, err
	}
	if c.PublishedSlot == 0 {
		return nil, nil
	}
	info := common.CloneProto(s.listSlot(c.PublishedSlot).Get(ctx))
	if info == nil {
		return nil, errors.New("missing published list static")
	}
	info.Paused = s.Paused
	if notes, ok := s.ListNotes.TryGet(ctx); ok {
		info.Notes = string(notes.Data)
	}
	info.RecentActions = nil
	for _, a := range c.RecentActions {
		info.RecentActions = append(info.RecentActions, common.CloneProto(a))
	}
	info.FutureActionTimes = nil
	for _, at := range c.FutureActionTimes {
		info.FutureActionTimes = append(info.FutureActionTimes, common.CloneProto(at))
	}
	return info, nil
}
func (s *Scheduler) listFirstFuture(ctx chasm.Context) (*timestamppb.Timestamp, error) {
	c, err := s.listControl(ctx)
	if err != nil {
		return nil, err
	}
	if len(c.FutureActionTimes) == 0 {
		return nil, nil
	}
	return common.CloneProto(c.FutureActionTimes[0]), nil
}
