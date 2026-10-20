package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Jobs interface {
	ProcessNext(
		ctx context.Context,
		process func(ctx context.Context, job models.Job, execution *models.ExecutionContext) ([]models.Job, error),
	) (bool, error)
}

type Workflows interface {
	GetByID(ctx context.Context, id string) (*models.Workflow, error)
}

type Worker struct {
	jobs      Jobs
	workflows Workflows
	engine    *engine.Engine
	logger    *slog.Logger
	idle      time.Duration
}

func New(jobs Jobs, workflows Workflows, e *engine.Engine, logger *slog.Logger, idle time.Duration) *Worker {
	return &Worker{jobs: jobs, workflows: workflows, engine: e, logger: logger, idle: idle}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		processed, err := w.jobs.ProcessNext(ctx, w.process)
		if err != nil && ctx.Err() == nil {
			w.logger.ErrorContext(ctx, "failed to process job", "error", err)
		}

		if processed && err == nil {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.idle):
		}
	}
}

func (w *Worker) process(ctx context.Context, job models.Job, execution *models.ExecutionContext) ([]models.Job, error) {
	ctx, span := otel.Tracer("operion/worker").Start(ctx, "worker.process", trace.WithAttributes(
		attribute.String("execution_id", execution.ID),
		attribute.String("workflow_id", execution.WorkflowID),
		attribute.String("node_id", job.NodeID),
	))
	defer span.End()

	if execution.Status != models.ExecutionStatusRunning {
		w.logger.InfoContext(ctx, "job skipped, execution already finished",
			"execution_id", execution.ID, "node_id", job.NodeID, "status", execution.Status)

		return nil, nil
	}

	workflow, err := w.workflows.GetByID(ctx, execution.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow %s: %w", execution.WorkflowID, err)
	}

	var next []models.Job

	if workflow == nil {
		now := time.Now().UTC()
		execution.Status = models.ExecutionStatusFailed
		execution.ErrorMessage = fmt.Sprintf("workflow %q not found", execution.WorkflowID)
		execution.CompletedAt = &now
	} else {
		execution.Variables = workflow.Variables

		deferred, err := w.engine.Run(ctx, workflow, execution, engine.Activation{NodeID: job.NodeID, Port: job.Port, Data: job.Data, Attempt: job.Attempt})
		if err != nil {
			return nil, err
		}

		for _, later := range deferred {
			next = append(next, models.Job{
				ID:          uuid.NewString(),
				ExecutionID: execution.ID,
				NodeID:      later.NodeID,
				Port:        later.Port,
				Data:        later.Data,
				RunAt:       later.RunAt,
				Attempt:     later.Attempt,
			})
		}
	}

	span.SetAttributes(attribute.String("status", string(execution.Status)))
	w.logger.InfoContext(ctx, "job processed",
		"execution_id", execution.ID,
		"workflow_id", execution.WorkflowID,
		"node_id", job.NodeID,
		"status", execution.Status,
		"error", execution.ErrorMessage,
		"deferred", len(next),
	)

	return next, nil
}
