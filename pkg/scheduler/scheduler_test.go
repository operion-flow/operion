package scheduler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	toSync    []models.Schedule
	firstRuns []time.Time
	firstErrs []error
	syncErr   error
	due       []models.Schedule
	dueErr    error
	firings   []models.Firing
	fireErrs  []error
	onEmpty   func()
}

func (f *fakeStore) Sync(_ context.Context, first func(models.Schedule) (time.Time, error)) error {
	for _, schedule := range f.toSync {
		nextRunAt, err := first(schedule)
		if err != nil {
			f.firstErrs = append(f.firstErrs, err)

			continue
		}

		f.firstRuns = append(f.firstRuns, nextRunAt)
	}

	return f.syncErr
}

func (f *fakeStore) ProcessDue(ctx context.Context, fire func(context.Context, models.Schedule) (models.Firing, error)) (bool, error) {
	if f.dueErr != nil {
		err := f.dueErr
		f.dueErr = nil

		return true, err
	}

	if len(f.due) == 0 {
		f.onEmpty()

		return false, nil
	}

	schedule := f.due[0]
	f.due = f.due[1:]

	firing, err := fire(ctx, schedule)
	if err != nil {
		f.fireErrs = append(f.fireErrs, err)

		return true, err
	}

	f.firings = append(f.firings, firing)

	return true, nil
}

func run(t *testing.T, store *fakeStore, now time.Time, logs io.Writer) error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	store.onEmpty = cancel

	s := New(store, slog.New(slog.NewTextHandler(logs, nil)), time.Millisecond)
	s.now = func() time.Time { return now }

	return s.Run(ctx)
}

func weekdayMorning(nextRunAt time.Time) models.Schedule {
	return models.Schedule{
		WorkflowID: "wf",
		NodeID:     "schedule",
		Expression: "0 9 * * *",
		Timezone:   "America/Sao_Paulo",
		NextRunAt:  nextRunAt,
	}
}

func TestRunSyncsAndFiresDueSchedules(t *testing.T) {
	scheduled := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	now := scheduled.Add(5 * time.Second)
	store := &fakeStore{
		toSync: []models.Schedule{weekdayMorning(time.Time{})},
		due:    []models.Schedule{weekdayMorning(scheduled)},
	}

	err := run(t, store, now, io.Discard)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []time.Time{scheduled.Add(24 * time.Hour)}, store.firstRuns)
	require.Len(t, store.firings, 1)

	firing := store.firings[0]
	assert.Equal(t, scheduled.Add(24*time.Hour), firing.NextRunAt)
	assert.Equal(t, "wf", firing.Execution.WorkflowID)
	assert.Equal(t, models.ExecutionStatusRunning, firing.Execution.Status)
	assert.Equal(t, map[string]any{"scheduled_time": "2026-10-15T12:00:00Z"}, firing.Execution.TriggerData)
	assert.Equal(t, firing.Execution.ID, firing.Job.ExecutionID)
	assert.Equal(t, "schedule", firing.Job.NodeID)
	assert.Equal(t, "external", firing.Job.Port)
	assert.Equal(t, firing.Execution.TriggerData, firing.Job.Data)
	assert.Equal(t, now, firing.Job.RunAt)
}

func TestRunKeepsGoingAfterErrors(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 5, 0, time.UTC)
	badTimezone := weekdayMorning(now)
	badTimezone.Timezone = "Mars/Olympus"
	badExpression := weekdayMorning(now)
	badExpression.Expression = "every monday"

	store := &fakeStore{
		toSync:  []models.Schedule{badExpression},
		syncErr: errors.New("sync failed"),
		dueErr:  errors.New("claim failed"),
		due:     []models.Schedule{badTimezone, weekdayMorning(now)},
	}

	var logs bytes.Buffer

	err := run(t, store, now, &logs)

	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, logs.String(), "failed to sync schedules")
	assert.Contains(t, logs.String(), "failed to fire schedule")
	require.NotEmpty(t, store.firstErrs)
	assert.ErrorContains(t, store.firstErrs[0], "expected exactly 5 fields")
	require.Len(t, store.fireErrs, 1)
	assert.ErrorContains(t, store.fireErrs[0], "Mars/Olympus")
	require.Len(t, store.firings, 1)
	assert.Equal(t, "wf", store.firings[0].Execution.WorkflowID)
}

func TestRunDoesNotLogErrorsCausedByCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := &fakeStore{syncErr: context.Canceled, dueErr: context.Canceled}

	var logs bytes.Buffer

	err := New(store, slog.New(slog.NewTextHandler(&logs, nil)), time.Millisecond).Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, logs.String(), "level=ERROR")
}
