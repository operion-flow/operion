package retention

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeStore struct {
	left    int
	cutoffs []time.Time
	limits  []int
	err     error
}

func (f *fakeStore) DeleteFinishedExecutions(_ context.Context, finishedBefore time.Time, limit int) (int, error) {
	f.cutoffs = append(f.cutoffs, finishedBefore)
	f.limits = append(f.limits, limit)

	if f.err != nil {
		return 0, f.err
	}

	deleted := min(f.left, limit)
	f.left -= deleted

	return deleted, nil
}

func TestSweepDeletesInBatchesUntilNothingIsLeft(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{left: 2*batchSize + 7}

	cleaner := New(store, slog.New(slog.DiscardHandler), 72*time.Hour)
	cleaner.now = func() time.Time { return now }
	cleaner.sweep(t.Context())

	assert.Zero(t, store.left)
	assert.Equal(t, []int{batchSize, batchSize, batchSize}, store.limits)

	for _, cutoff := range store.cutoffs {
		assert.Equal(t, now.Add(-72*time.Hour), cutoff)
	}
}

func TestSweepStopsOnAStoreError(t *testing.T) {
	store := &fakeStore{left: batchSize * 3, err: errors.New("database is down")}

	New(store, slog.New(slog.DiscardHandler), time.Hour).sweep(t.Context())

	assert.Len(t, store.limits, 1)
}

func TestTheSweepRunsAtMostHourly(t *testing.T) {
	assert.Equal(t, time.Second, New(&fakeStore{}, nil, time.Second).interval)
	assert.Equal(t, time.Hour, New(&fakeStore{}, nil, 30*24*time.Hour).interval)
}
