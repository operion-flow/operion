package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
	"github.com/dukex/operion/pkg/secrets"
)

const (
	triggerInputPort = "external"
	errorOutputPort  = "error"
	ResumePort       = protocol.ResumePort
)

type NodeCreator interface {
	CreateNode(ctx context.Context, nodeType, nodeID string, config map[string]any) (protocol.Node, error)
}

type Activation struct {
	NodeID  string
	Port    string
	Data    map[string]any
	Attempt int
}

func TriggerActivation(triggerNodeID string, data map[string]any) Activation {
	return Activation{NodeID: triggerNodeID, Port: triggerInputPort, Data: data}
}

type Deferred struct {
	Activation

	RunAt time.Time
}

type SecretSource interface {
	Values(ctx context.Context, tenantID string) (map[string]string, error)
}

type Engine struct {
	nodes   NodeCreator
	secrets SecretSource
}

func New(nodes NodeCreator) *Engine {
	return &Engine{nodes: nodes}
}

func (e *Engine) UseSecrets(source SecretSource) {
	e.secrets = source
}

func (e *Engine) Run(ctx context.Context, workflow *models.Workflow, execution *models.ExecutionContext, start Activation) ([]Deferred, error) {
	g := newGraph(workflow)
	queue := []Activation{start}

	var deferred []Deferred

	if execution.NodeResults == nil {
		execution.NodeResults = make(map[string]models.NodeResult)
	}

	if e.secrets != nil {
		values, err := e.secrets.Values(ctx, workflow.TenantID)
		if err != nil {
			return nil, fmt.Errorf("failed to load the secrets of tenant %q: %w", workflow.TenantID, err)
		}

		execution.Secrets = values
	}

	mask := secrets.NewMask(execution.Secrets)

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		current := queue[0]
		queue = queue[1:]

		next, later, err := e.step(ctx, g, execution, current, mask)
		if err != nil {
			finish(execution, models.ExecutionStatusFailed, mask.Text(err.Error()))

			return nil, nil
		}

		queue = append(queue, next...)
		deferred = append(deferred, later...)
	}

	if len(deferred) == 0 {
		finish(execution, models.ExecutionStatusCompleted, "")
	}

	return deferred, nil
}

func (e *Engine) step(ctx context.Context, g graph, execution *models.ExecutionContext, current Activation, mask *secrets.Mask) ([]Activation, []Deferred, error) {
	node, ok := g.nodes[current.NodeID]
	if !ok {
		return nil, nil, fmt.Errorf("node %q not found in workflow", current.NodeID)
	}

	instance, err := e.nodes.CreateNode(ctx, node.Type, node.ID, node.Config)
	if err != nil {
		return nil, nil, fmt.Errorf("node %q: %w", node.ID, err)
	}

	inputs := map[string]models.NodeResult{
		current.Port: {NodeID: node.ID, Data: current.Data, Status: string(models.NodeStatusSuccess)},
	}

	if waiter, ok := instance.(protocol.Waiter); ok && current.Port != ResumePort {
		runAt, err := waiter.WaitUntil(*execution, inputs)
		if err != nil {
			return nil, nil, fmt.Errorf("node %q: %w", node.ID, err)
		}

		return nil, hold(execution, node.ID, map[string]any{"resume_at": runAt.UTC().Format(time.RFC3339)}, runAt, current.Data), nil
	}

	outputs, err := instance.Execute(*execution, inputs)

	var suspended *protocol.SuspendError
	if errors.As(err, &suspended) {
		return nil, hold(execution, node.ID, mask.Data(suspended.Data), suspended.Until, nil), nil
	}

	if retry, ok := retryLater(execution, node, current, mask.Text(failure(outputs, err))); ok {
		return nil, []Deferred{retry}, nil
	}

	if err != nil {
		return nil, nil, fmt.Errorf("node %q: %w", node.ID, err)
	}

	var next []Activation

	for _, port := range slices.Sorted(maps.Keys(outputs)) {
		result := outputs[port]
		result.Data = mask.Data(result.Data)
		result.Error = mask.Text(result.Error)
		result.NodeID = node.ID
		result.Timestamp = time.Now().UTC()
		execution.NodeResults[node.ID] = result

		targets := g.targets[models.MakePortID(node.ID, port)]
		if port == errorOutputPort && len(targets) == 0 {
			return nil, nil, fmt.Errorf("node %q: %s", node.ID, errorMessage(result))
		}

		for _, target := range targets {
			next = append(next, Activation{NodeID: target.nodeID, Port: target.port, Data: result.Data})
		}
	}

	return next, nil, nil
}

func hold(execution *models.ExecutionContext, nodeID string, result map[string]any, until time.Time, data map[string]any) []Deferred {
	execution.NodeResults[nodeID] = models.NodeResult{
		NodeID:    nodeID,
		Data:      result,
		Status:    string(models.NodeStatusWaiting),
		Timestamp: time.Now().UTC(),
	}

	return []Deferred{{Activation: Activation{NodeID: nodeID, Port: ResumePort, Data: data}, RunAt: until}}
}

func finish(execution *models.ExecutionContext, status models.ExecutionStatus, message string) {
	now := time.Now().UTC()
	execution.Status = status
	execution.ErrorMessage = message
	execution.CompletedAt = &now
}

func errorMessage(result models.NodeResult) string {
	if result.Error != "" {
		return result.Error
	}

	if message, ok := result.Data["error"].(string); ok {
		return message
	}

	return "failed without an error message"
}

type portRef struct {
	nodeID string
	port   string
}

type graph struct {
	nodes   map[string]*models.WorkflowNode
	targets map[string][]portRef
}

func newGraph(workflow *models.Workflow) graph {
	g := graph{
		nodes:   make(map[string]*models.WorkflowNode, len(workflow.Nodes)),
		targets: make(map[string][]portRef, len(workflow.Connections)),
	}

	for _, node := range workflow.Nodes {
		g.nodes[node.ID] = node
	}

	for _, connection := range workflow.Connections {
		nodeID, port, _ := models.ParsePortID(connection.TargetPort)
		g.targets[connection.SourcePort] = append(g.targets[connection.SourcePort], portRef{nodeID: nodeID, port: port})
	}

	return g
}
