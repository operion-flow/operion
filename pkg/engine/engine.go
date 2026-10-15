package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/protocol"
)

const (
	triggerInputPort = "external"
	errorOutputPort  = "error"
)

type NodeCreator interface {
	CreateNode(ctx context.Context, nodeType, nodeID string, config map[string]any) (protocol.Node, error)
}

type Activation struct {
	NodeID string
	Port   string
	Data   map[string]any
}

func TriggerActivation(triggerNodeID string, data map[string]any) Activation {
	return Activation{NodeID: triggerNodeID, Port: triggerInputPort, Data: data}
}

type Engine struct {
	nodes NodeCreator
}

func New(nodes NodeCreator) *Engine {
	return &Engine{nodes: nodes}
}

func (e *Engine) Run(ctx context.Context, workflow *models.Workflow, execution *models.ExecutionContext, start Activation) error {
	g := newGraph(workflow)
	queue := []Activation{start}

	if execution.NodeResults == nil {
		execution.NodeResults = make(map[string]models.NodeResult)
	}

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		current := queue[0]
		queue = queue[1:]

		next, err := e.step(ctx, g, execution, current)
		if err != nil {
			finish(execution, models.ExecutionStatusFailed, err.Error())

			return nil
		}

		queue = append(queue, next...)
	}

	finish(execution, models.ExecutionStatusCompleted, "")

	return nil
}

func (e *Engine) step(ctx context.Context, g graph, execution *models.ExecutionContext, current Activation) ([]Activation, error) {
	node, ok := g.nodes[current.NodeID]
	if !ok {
		return nil, fmt.Errorf("node %q not found in workflow", current.NodeID)
	}

	instance, err := e.nodes.CreateNode(ctx, node.Type, node.ID, node.Config)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", node.ID, err)
	}

	inputs := map[string]models.NodeResult{
		current.Port: {NodeID: node.ID, Data: current.Data, Status: string(models.NodeStatusSuccess)},
	}

	outputs, err := instance.Execute(*execution, inputs)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", node.ID, err)
	}

	var next []Activation

	for _, port := range slices.Sorted(maps.Keys(outputs)) {
		result := outputs[port]
		result.NodeID = node.ID
		result.Timestamp = time.Now().UTC()
		execution.NodeResults[node.ID] = result

		targets := g.targets[models.MakePortID(node.ID, port)]
		if port == errorOutputPort && len(targets) == 0 {
			return nil, fmt.Errorf("node %q: %s", node.ID, errorMessage(result))
		}

		for _, target := range targets {
			next = append(next, Activation{NodeID: target.nodeID, Port: target.port, Data: result.Data})
		}
	}

	return next, nil
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
