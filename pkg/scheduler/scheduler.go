package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type Store interface {
	Sync(ctx context.Context, first func(models.Schedule) (time.Time, error)) error
	ProcessDue(ctx context.Context, fire func(context.Context, models.Schedule) (models.Firing, error)) (bool, error)
}

type Scheduler struct {
	store  Store
	logger *slog.Logger
	idle   time.Duration
	now    func() time.Time
}

func New(store Store, logger *slog.Logger, idle time.Duration) *Scheduler {
	return &Scheduler{store: store, logger: logger, idle: idle, now: time.Now}
}

func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if err := s.store.Sync(ctx, s.first); err != nil {
			s.logger.ErrorContext(ctx, "failed to sync schedules", "error", err)
		}

		s.fireDue(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.idle):
		}
	}
}

func (s *Scheduler) fireDue(ctx context.Context) {
	for {
		processed, err := s.store.ProcessDue(ctx, s.fire)
		if err != nil {
			s.logger.ErrorContext(ctx, "failed to fire schedule", "error", err)

			return
		}

		if !processed {
			return
		}
	}
}

func (s *Scheduler) first(schedule models.Schedule) (time.Time, error) {
	return next(schedule, s.now())
}

func (s *Scheduler) fire(ctx context.Context, schedule models.Schedule) (models.Firing, error) {
	now := s.now().UTC()

	nextRunAt, err := next(schedule, now)
	if err != nil {
		return models.Firing{}, err
	}

	data := map[string]any{"scheduled_time": schedule.NextRunAt.UTC().Format(time.RFC3339)}
	activation := engine.TriggerActivation(schedule.NodeID, data)
	execution := &models.ExecutionContext{
		ID:          uuid.NewString(),
		WorkflowID:  schedule.WorkflowID,
		Status:      models.ExecutionStatusRunning,
		NodeResults: map[string]models.NodeResult{},
		TriggerData: data,
		CreatedAt:   now,
	}

	s.logger.InfoContext(ctx, "schedule fired",
		"execution_id", execution.ID,
		"workflow_id", schedule.WorkflowID,
		"node_id", schedule.NodeID,
		"scheduled_time", data["scheduled_time"],
		"next_run_at", nextRunAt,
	)

	return models.Firing{
		Execution: execution,
		Job: models.Job{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      activation.NodeID,
			Port:        activation.Port,
			Data:        activation.Data,
			RunAt:       now,
		},
		NextRunAt: nextRunAt,
	}, nil
}

func next(schedule models.Schedule, after time.Time) (time.Time, error) {
	location, err := time.LoadLocation(schedule.Timezone)
	if err != nil {
		return time.Time{}, err
	}

	parsed, err := cron.ParseStandard(schedule.Expression)
	if err != nil {
		return time.Time{}, err
	}

	return parsed.Next(after.In(location)).UTC(), nil
}
