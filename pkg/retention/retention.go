package retention

import (
	"context"
	"log/slog"
	"time"
)

const (
	batchSize   = 500
	maxInterval = time.Hour
)

type Store interface {
	DeleteFinishedExecutions(ctx context.Context, finishedBefore time.Time, limit int) (int, error)
}

type Cleaner struct {
	store    Store
	logger   *slog.Logger
	keep     time.Duration
	interval time.Duration
	now      func() time.Time
}

func New(store Store, logger *slog.Logger, keep time.Duration) *Cleaner {
	return &Cleaner{store: store, logger: logger, keep: keep, interval: min(keep, maxInterval), now: time.Now}
}

func (c *Cleaner) Run(ctx context.Context) error {
	for {
		c.sweep(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.interval):
		}
	}
}

func (c *Cleaner) sweep(ctx context.Context) {
	finishedBefore := c.now().Add(-c.keep)

	for {
		deleted, err := c.store.DeleteFinishedExecutions(ctx, finishedBefore, batchSize)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "failed to delete finished executions", "error", err)
			}

			return
		}

		if deleted < batchSize {
			return
		}
	}
}
