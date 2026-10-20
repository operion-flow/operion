package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/dukex/operion/pkg/models"
)

var errRetryAttempts = errors.New("retry attempts must be at least 1")

func failure(outputs map[string]models.NodeResult, err error) string {
	if err != nil {
		return err.Error()
	}

	if result, ok := outputs[errorOutputPort]; ok {
		return errorMessage(result)
	}

	return ""
}

func retryLater(execution *models.ExecutionContext, node *models.WorkflowNode, current Activation, failure string) (Deferred, bool) {
	attempt := current.Attempt + 1
	if failure == "" || node.Retry == nil || attempt >= node.Retry.Attempts || current.Port == ResumePort {
		return Deferred{}, false
	}

	delay, err := backoff(node.Retry, attempt)
	if err != nil {
		return Deferred{}, false
	}

	now := time.Now().UTC()
	runAt := now.Add(delay)

	execution.NodeResults[node.ID] = models.NodeResult{
		NodeID:    node.ID,
		Data:      map[string]any{"attempt": attempt, "retry_at": runAt.Format(time.RFC3339)},
		Status:    string(models.NodeStatusRetrying),
		Error:     failure,
		Timestamp: now,
	}

	return Deferred{
		Activation: Activation{NodeID: node.ID, Port: current.Port, Data: current.Data, Attempt: attempt},
		RunAt:      runAt,
	}, true
}

func backoff(policy *models.RetryPolicy, attempt int) (time.Duration, error) {
	delay, limit, err := retryDelays(policy)
	if err != nil {
		return 0, err
	}

	for range attempt - 1 {
		if delay >= limit {
			break
		}

		delay *= 2
	}

	return min(delay, limit), nil
}

func retryDelays(policy *models.RetryPolicy) (time.Duration, time.Duration, error) {
	initial, err := time.ParseDuration(policy.Initial)
	if err != nil || initial <= 0 {
		return 0, 0, fmt.Errorf("retry initial must be a positive Go duration such as 1s, got %q", policy.Initial)
	}

	limit, err := time.ParseDuration(policy.Max)
	if err != nil || limit < initial {
		return 0, 0, fmt.Errorf("retry max must be a Go duration no shorter than initial, got %q", policy.Max)
	}

	return initial, limit, nil
}

func validateRetry(policy *models.RetryPolicy) error {
	if policy == nil {
		return nil
	}

	if policy.Attempts < 1 {
		return errRetryAttempts
	}

	_, _, err := retryDelays(policy)

	return err
}
