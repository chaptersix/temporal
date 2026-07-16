package scheduler_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

type lifecycleModel struct {
	env             *schedulerModelEnv
	idleTime        time.Duration
	lastEvent       time.Time
	idleDeadline    time.Time
	paused          bool
	notes           string
	backfill        bool
	closed          bool
	createRequestID string
	createN         int
	updateN         int
	staleTasks      []tasks.Task
}

func TestSchedulerLifecycleModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		config := defaultModelEnvConfig()
		config.interval = time.Minute
		config.specEnd = config.startTime.Add(-time.Minute)
		config.idleTime = time.Duration(rapid.IntRange(2, 10).Draw(t, "idle minutes")) * time.Minute
		config.maxBufferSize = 32
		config.generatorReserve = 0
		model := &lifecycleModel{
			env: newSchedulerModelEnv(t, config), idleTime: config.idleTime,
			lastEvent: config.startTime, idleDeadline: config.startTime.Add(config.idleTime),
			createRequestID: "create-request",
		}
		model.check(t)
		t.Repeat(map[string]func(*rapid.T){
			"duplicate create":        model.duplicateCreate,
			"pause or unpause":        model.togglePause,
			"update idle schedule":    model.updateSchedule,
			"advance before idle":     model.advanceBeforeIdle,
			"deliver obsolete idle":   model.deliverObsoleteIdle,
			"add pending backfill":    model.addPendingBackfill,
			"advance while held open": model.advanceWhileHeldOpen,
			"advance to idle":         model.advanceToIdle,
			"delete":                  model.deleteSchedule,
			"closed APIs":             model.checkClosedAPIs,
			"reload execution":        model.reload,
			"":                        model.check,
		})
	})
}

func (m *lifecycleModel) reload(t *rapid.T) {
	m.env.reload(t)
	m.check(t)
}

func TestSchedulerSentinelLifecycleModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		env := newSentinelModelEnv(t)
		created := env.internal(t).createTime
		deadline := created.Add(scheduler.SentinelIdleTime)
		idleTask := findPureTaskAt(t, env, deadline)
		closed := false
		replaced := false

		check := func(t *rapid.T) {
			internal := env.internal(t)
			if replaced {
				if internal.sentinel || internal.closed {
					t.Fatalf("replacement schedule state: sentinel=%v closed=%v", internal.sentinel, internal.closed)
				}
				env.describe(t)
				env.requireNoRunnableTasks(t)
				return
			}
			if !internal.sentinel || internal.closed != closed || !internal.createTime.Equal(created) {
				t.Fatalf("sentinel state: got sentinel=%v closed=%v create=%s, want closed=%v create=%s",
					internal.sentinel, internal.closed, internal.createTime, closed, created)
			}
			checkSentinelAPIs(t, env, closed)
			if !closed {
				task := findPureTaskAt(t, env, deadline)
				if task.GetVisibilityTime() != idleTask.GetVisibilityTime() {
					t.Fatalf("sentinel deadline changed: got %s, want %s", task.GetVisibilityTime(), deadline)
				}
			}
			env.requireNoRunnableTasks(t)
		}

		check(t)
		t.Repeat(map[string]func(*rapid.T){
			"duplicate sentinel": func(t *rapid.T) {
				if replaced {
					_, err := env.handler.CreateSentinel(env.engineCtx, &schedulerpb.CreateSentinelRequest{
						Namespace: namespace, NamespaceId: namespaceID, ScheduleId: env.scheduleID,
					})
					var alreadyExists *serviceerror.AlreadyExists
					if !errors.As(err, &alreadyExists) {
						t.Fatalf("sentinel over real schedule: got %v, want AlreadyExists", err)
					}
					check(t)
					return
				}
				_, err := env.handler.CreateSentinel(env.engineCtx, &schedulerpb.CreateSentinelRequest{
					Namespace: namespace, NamespaceId: namespaceID, ScheduleId: env.scheduleID,
				})
				mustNoError(t, err)
				if closed {
					closed = false
					created = env.timeSource.Now()
					deadline = created.Add(scheduler.SentinelIdleTime)
					idleTask = findPureTaskAt(t, env, deadline)
				}
				check(t)
			},
			"create schedule": func(t *rapid.T) {
				if replaced {
					t.Skip("sentinel was already replaced")
				}
				_, err := env.handler.CreateSchedule(env.engineCtx, &schedulerpb.CreateScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.CreateScheduleRequest{
						Namespace: namespace, ScheduleId: env.scheduleID, RequestId: "sentinel-create",
						Schedule: modelSchedule(defaultModelEnvConfig()),
					},
				})
				if closed {
					mustNoError(t, err)
					env.drain(t)
					replaced = true
				} else {
					requireErrorIs(t, err, scheduler.ErrSentinel)
				}
				check(t)
			},
			"advance before expiry": func(t *rapid.T) {
				if closed {
					t.Skip("sentinel is closed")
				}
				remainingSeconds := int(deadline.Sub(env.timeSource.Now()) / time.Second)
				if remainingSeconds <= 1 {
					t.Skip("sentinel deadline is next")
				}
				advance := rapid.IntRange(1, remainingSeconds-1).Draw(t, "seconds")
				env.timeSource.Update(env.timeSource.Now().Add(time.Duration(advance) * time.Second))
				env.drain(t)
				check(t)
			},
			"expire": func(t *rapid.T) {
				if closed || replaced {
					t.Skip("sentinel is closed")
				}
				env.timeSource.Update(deadline)
				env.drain(t)
				closed = true
				result := env.redeliver(t, idleTask)
				if !result.Dropped || result.Executed != 0 {
					t.Fatalf("closed sentinel idle redelivery: %+v", result)
				}
				check(t)
			},
			"APIs": check,
		})
	})
}

func (m *lifecycleModel) duplicateCreate(t *rapid.T) {
	sameRequest := rapid.Bool().Draw(t, "same request")
	requestID := m.createRequestID
	if !sameRequest {
		m.createN++
		requestID = fmt.Sprintf("lifecycle-create-%d", m.createN)
	}
	before := m.env.internal(t)
	_, err := m.env.handler.CreateSchedule(m.env.engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID, RequestId: requestID,
			Schedule: modelSchedule(m.env.config),
		},
	})
	if sameRequest || m.closed {
		mustNoError(t, err)
	} else {
		var alreadyExists *serviceerror.AlreadyExists
		if !errors.As(err, &alreadyExists) {
			t.Fatalf("duplicate create error: got %v, want AlreadyExists", err)
		}
	}
	if m.closed && !sameRequest {
		m.env.drain(t)
		m.closed = false
		m.paused = false
		m.notes = ""
		m.backfill = false
		m.lastEvent = m.env.timeSource.Now()
		m.idleDeadline = m.lastEvent.Add(m.idleTime)
		m.createRequestID = requestID
		m.staleTasks = nil
	} else {
		after := m.env.internal(t)
		if before.closed != after.closed || before.conflictToken != after.conflictToken ||
			!before.createTime.Equal(after.createTime) {
			t.Fatal("duplicate create mutated scheduler state")
		}
	}
	m.check(t)
}

func (m *lifecycleModel) togglePause(t *rapid.T) {
	if m.closed || m.backfill {
		t.Skip("schedule cannot toggle pause")
	}
	m.saveIdleTask(t)
	pause := !m.paused
	note := "paused by lifecycle model"
	patch := &schedulepb.SchedulePatch{Pause: note}
	if !pause {
		note = "unpaused by lifecycle model"
		patch = &schedulepb.SchedulePatch{Unpause: note}
	}
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID, Patch: patch,
		},
	})
	mustNoError(t, err)
	m.env.drain(t)
	m.paused = pause
	m.notes = note
	m.lastEvent = m.env.timeSource.Now()
	if m.paused {
		m.idleDeadline = time.Time{}
	} else {
		m.idleDeadline = m.lastEvent.Add(m.idleTime)
	}
	m.check(t)
}

func (m *lifecycleModel) updateSchedule(t *rapid.T) {
	if m.closed || m.backfill {
		t.Skip("schedule cannot update")
	}
	m.saveIdleTask(t)
	description := m.env.describe(t)
	schedule := proto.CloneOf(description.GetSchedule())
	m.updateN++
	schedule.State.Notes = "lifecycle update " + time.Duration(m.updateN).String()
	_, err := m.env.handler.UpdateSchedule(m.env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID, Schedule: schedule,
			ConflictToken: slices.Clone(description.GetConflictToken()),
		},
	})
	mustNoError(t, err)
	m.env.drain(t)
	m.notes = schedule.State.Notes
	m.lastEvent = m.env.timeSource.Now()
	if m.paused {
		m.idleDeadline = time.Time{}
	} else {
		m.idleDeadline = m.lastEvent.Add(m.idleTime)
	}
	m.check(t)
}

func (m *lifecycleModel) advanceBeforeIdle(t *rapid.T) {
	if m.closed || m.paused || m.backfill || m.idleDeadline.IsZero() {
		t.Skip("schedule has no active idle deadline")
	}
	remainingSeconds := int(m.idleDeadline.Sub(m.env.timeSource.Now()) / time.Second)
	if remainingSeconds <= 1 {
		t.Skip("idle deadline is next")
	}
	advance := rapid.IntRange(1, remainingSeconds-1).Draw(t, "seconds")
	m.env.timeSource.Update(m.env.timeSource.Now().Add(time.Duration(advance) * time.Second))
	m.env.drain(t)
	m.check(t)
}

func (m *lifecycleModel) deliverObsoleteIdle(t *rapid.T) {
	if m.closed || m.backfill {
		t.Skip("cannot isolate obsolete idle delivery")
	}
	var candidates []tasks.Task
	for _, task := range m.staleTasks {
		if task.GetVisibilityTime().After(m.env.timeSource.Now()) {
			continue
		}
		if !m.paused && !m.idleDeadline.IsZero() && !m.idleDeadline.After(task.GetVisibilityTime()) {
			continue
		}
		candidates = append(candidates, task)
	}
	if len(candidates) == 0 {
		var next tasks.Task
		for _, task := range m.staleTasks {
			if !m.paused && !m.idleDeadline.IsZero() && !m.idleDeadline.After(task.GetVisibilityTime()) {
				continue
			}
			if next == nil || task.GetVisibilityTime().Before(next.GetVisibilityTime()) {
				next = task
			}
		}
		if next == nil {
			t.Skip("no obsolete idle task")
		}
		m.env.timeSource.Update(next.GetVisibilityTime())
		candidates = append(candidates, next)
	}
	task := candidates[rapid.IntRange(0, len(candidates)-1).Draw(t, "idle task")]
	result := m.env.redeliver(t, task)
	if !result.Dropped || result.Executed != 0 {
		t.Fatalf("obsolete idle task result: %+v", result)
	}
	m.check(t)
}

func (m *lifecycleModel) addPendingBackfill(t *rapid.T) {
	if m.closed || m.backfill {
		t.Skip("cannot add backfill")
	}
	m.saveIdleTask(t)
	capacity := m.env.config.maxBufferSize/2 - m.env.config.generatorReserve
	end := m.env.config.specEnd
	start := end.Add(-time.Duration(capacity*2-1) * m.env.config.interval)
	_, err := m.env.handler.PatchSchedule(m.env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
			Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(start), EndTime: timestamppb.New(end),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}}},
		},
	})
	mustNoError(t, err)
	m.env.drain(t)
	m.backfill = true
	m.idleDeadline = time.Time{}
	if m.env.internal(t).backfillers == 0 {
		t.Fatal("large backfill completed instead of remaining pending")
	}
	m.check(t)
}

func (m *lifecycleModel) advanceWhileHeldOpen(t *rapid.T) {
	if m.closed || (!m.paused && !m.backfill) {
		t.Skip("schedule is not held open")
	}
	m.env.timeSource.Update(m.env.timeSource.Now().Add(2 * m.idleTime))
	m.env.drain(t)
	if m.env.internal(t).closed {
		t.Fatal("held-open schedule closed")
	}
	m.check(t)
}

func (m *lifecycleModel) advanceToIdle(t *rapid.T) {
	if m.closed || m.paused || m.backfill || m.idleDeadline.IsZero() {
		t.Skip("schedule cannot idle-close")
	}
	idleTask := findPureTaskAt(t, m.env, m.idleDeadline)
	m.env.timeSource.Update(m.idleDeadline)
	m.env.drain(t)
	m.closed = true
	result := m.env.redeliver(t, idleTask)
	if !result.Dropped || result.Executed != 0 {
		t.Fatalf("closed idle task redelivery: %+v", result)
	}
	m.check(t)
}

func (m *lifecycleModel) deleteSchedule(t *rapid.T) {
	_, err := m.env.handler.DeleteSchedule(m.env.engineCtx, &schedulerpb.DeleteScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DeleteScheduleRequest{
			Namespace: namespace, ScheduleId: m.env.scheduleID,
		},
	})
	if m.closed {
		requireErrorIs(t, err, scheduler.ErrClosed)
	} else {
		mustNoError(t, err)
		m.closed = true
	}
	m.env.drain(t)
	m.check(t)
}

func (m *lifecycleModel) checkClosedAPIs(t *rapid.T) {
	if !m.closed {
		t.Skip("schedule is open")
	}
	checkClosedAPIs(t, m.env)
	m.check(t)
}

func (m *lifecycleModel) saveIdleTask(t *rapid.T) {
	if m.idleDeadline.IsZero() {
		return
	}
	m.staleTasks = append(m.staleTasks, findPureTaskAt(t, m.env, m.idleDeadline))
}

func (m *lifecycleModel) check(t *rapid.T) {
	t.Helper()
	internal := m.env.internal(t)
	if internal.closed != m.closed || internal.sentinel {
		t.Fatalf("lifecycle state: got closed=%v sentinel=%v, want closed=%v",
			internal.closed, internal.sentinel, m.closed)
	}
	if m.closed {
		checkClosedAPIs(t, m.env)
		m.env.requireNoRunnableTasks(t)
		return
	}
	description := m.env.describe(t)
	if description.GetSchedule().GetState().GetPaused() != m.paused ||
		description.GetSchedule().GetState().GetNotes() != m.notes {
		t.Fatalf("lifecycle schedule state: got paused=%v notes=%q, want paused=%v notes=%q",
			description.GetSchedule().GetState().GetPaused(), description.GetSchedule().GetState().GetNotes(),
			m.paused, m.notes)
	}
	if m.paused || m.backfill {
		if !internal.idleCloseTime.IsZero() {
			t.Fatalf("held-open schedule has idle close time %s", internal.idleCloseTime)
		}
	} else if !internal.idleCloseTime.Equal(m.idleDeadline) {
		t.Fatalf("idle close time: got %s, want %s", internal.idleCloseTime, m.idleDeadline)
	}
	if m.backfill && internal.backfillers == 0 {
		t.Fatal("pending backfill disappeared")
	}
	m.env.checkRequestIDs(t)
	m.env.requireNoRunnableTasks(t)
}

func newSentinelModelEnv(t *rapid.T) *schedulerModelEnv {
	env := newSchedulerModelEnv(t, defaultModelEnvConfig())
	env.scheduleID = "sentinel-schedule-id"
	env.ref = chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID, BusinessID: env.scheduleID,
	})
	_, err := env.handler.CreateSentinel(env.engineCtx, &schedulerpb.CreateSentinelRequest{
		Namespace: namespace, NamespaceId: namespaceID, ScheduleId: env.scheduleID,
	})
	mustNoError(t, err)
	env.drain(t)
	return env
}

func findPureTaskAt(t *rapid.T, env *schedulerModelEnv, visibility time.Time) tasks.Task {
	t.Helper()
	queued, err := env.engine.Tasks(env.ref)
	mustNoError(t, err)
	for _, task := range queued[tasks.CategoryTimer] {
		if _, ok := task.(*tasks.ChasmTaskPure); ok && task.GetVisibilityTime().Equal(visibility) {
			return task
		}
	}
	t.Fatalf("no pure task at %s", visibility)
	return nil
}

func checkClosedAPIs(t *rapid.T, env *schedulerModelEnv) {
	t.Helper()
	_, err := env.handler.DescribeSchedule(env.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: env.scheduleID},
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
	_, err = env.handler.ListScheduleMatchingTimes(env.engineCtx, &schedulerpb.ListScheduleMatchingTimesRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
			Namespace: namespace, ScheduleId: env.scheduleID,
			StartTime: timestamppb.New(env.timeSource.Now()), EndTime: timestamppb.New(env.timeSource.Now()),
		},
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
	_, err = env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID, Schedule: modelSchedule(env.config),
		},
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
	_, err = env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID, Patch: &schedulepb.SchedulePatch{},
		},
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
	_, err = env.handler.DeleteSchedule(env.engineCtx, &schedulerpb.DeleteScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DeleteScheduleRequest{Namespace: namespace, ScheduleId: env.scheduleID},
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
	_, err = env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: env.scheduleID,
	})
	requireErrorIs(t, err, scheduler.ErrClosed)
}

func checkSentinelAPIs(t *rapid.T, env *schedulerModelEnv, closed bool) {
	t.Helper()
	_, err := env.handler.DescribeSchedule(env.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: env.scheduleID},
	})
	requireErrorIs(t, err, scheduler.ErrSentinel)
	_, err = env.handler.ListScheduleMatchingTimes(env.engineCtx, &schedulerpb.ListScheduleMatchingTimesRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
			Namespace: namespace, ScheduleId: env.scheduleID,
			StartTime: timestamppb.New(env.timeSource.Now()), EndTime: timestamppb.New(env.timeSource.Now()),
		},
	})
	requireErrorIs(t, err, scheduler.ErrSentinel)
	_, err = env.handler.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID, Schedule: modelSchedule(env.config),
		},
	})
	requireErrorIs(t, err, scheduler.ErrSentinel)
	_, err = env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace: namespace, ScheduleId: env.scheduleID, Patch: &schedulepb.SchedulePatch{},
		},
	})
	requireErrorIs(t, err, scheduler.ErrSentinel)
	_, err = env.handler.DeleteSchedule(env.engineCtx, &schedulerpb.DeleteScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DeleteScheduleRequest{Namespace: namespace, ScheduleId: env.scheduleID},
	})
	if closed {
		requireErrorIs(t, err, scheduler.ErrClosed)
	} else {
		requireErrorIs(t, err, scheduler.ErrSentinel)
	}
	_, err = env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID, ScheduleId: env.scheduleID,
	})
	requireErrorIs(t, err, scheduler.ErrSentinelBlocked)
}

func requireErrorIs(t *rapid.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error: got %v, want %v", err, target)
	}
}
