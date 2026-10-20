package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/dukex/operion/pkg/models"
)

func (e *Engine) Validate(ctx context.Context, workflow *models.Workflow) error {
	var errs []error

	inputs := make(map[string]bool)
	outputs := make(map[string]bool)

	for _, node := range workflow.Nodes {
		instance, err := e.nodes.CreateNode(ctx, node.Type, node.ID, node.Config)
		if err != nil {
			errs = append(errs, fmt.Errorf("node %q: %w", node.ID, err))

			continue
		}

		if err := instance.Validate(node.Config); err != nil {
			errs = append(errs, fmt.Errorf("node %q: %w", node.ID, err))
		}

		if err := validateRetry(node.Retry); err != nil {
			errs = append(errs, fmt.Errorf("node %q: %w", node.ID, err))
		}

		for _, port := range instance.InputPorts() {
			inputs[models.MakePortID(node.ID, port.Name)] = true
		}

		for _, port := range instance.OutputPorts() {
			outputs[models.MakePortID(node.ID, port.Name)] = true
		}
	}

	for _, connection := range workflow.Connections {
		if !outputs[connection.SourcePort] {
			errs = append(errs, fmt.Errorf("connection %q: output port %q does not exist", connection.ID, connection.SourcePort))
		}

		if !inputs[connection.TargetPort] {
			errs = append(errs, fmt.Errorf("connection %q: input port %q does not exist", connection.ID, connection.TargetPort))
		}
	}

	errs = append(errs, checkPaths(workflow)...)

	return errors.Join(errs...)
}

func checkPaths(workflow *models.Workflow) []error {
	next := make(map[string][]string)

	for _, connection := range workflow.Connections {
		source, _, _ := models.ParsePortID(connection.SourcePort)
		target, _, _ := models.ParsePortID(connection.TargetPort)
		next[source] = append(next[source], target)
	}

	const (
		visiting = 1
		visited  = 2
	)

	state := make(map[string]int)

	var errs []error

	var visit func(nodeID string)

	visit = func(nodeID string) {
		state[nodeID] = visiting

		for _, target := range next[nodeID] {
			switch state[target] {
			case visiting:
				errs = append(errs, fmt.Errorf("node %q is part of a cycle", target))
			case 0:
				visit(target)
			}
		}

		state[nodeID] = visited
	}

	for _, node := range workflow.Nodes {
		if node.IsTriggerNode() {
			visit(node.ID)
		}
	}

	for _, node := range workflow.Nodes {
		if state[node.ID] == 0 {
			errs = append(errs, fmt.Errorf("node %q is not reachable from a trigger", node.ID))
		}
	}

	return errs
}
