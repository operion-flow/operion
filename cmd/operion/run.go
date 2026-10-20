package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/nodes/notify"
	"github.com/dukex/operion/pkg/registry"
	cli "github.com/urfave/cli/v3"
)

var errExecutionFailed = errors.New("execution failed")

func newRunCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Run a workflow file against an event file in memory and print the execution",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "workflow", Usage: "Path to the workflow JSON", Required: true},
			&cli.StringFlag{Name: "event", Usage: "Path to the event JSON passed to the trigger", Required: true},
			&cli.StringFlag{Name: "trigger", Usage: "Trigger node ID, required when the workflow has more than one trigger"},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			return runWorkflow(ctx, command.String("workflow"), command.String("event"), command.String("trigger"), command.Root().Writer)
		},
	}
}

func runWorkflow(ctx context.Context, workflowPath, eventPath, triggerID string, out io.Writer) error {
	var workflow models.Workflow
	if err := readJSON(workflowPath, &workflow); err != nil {
		return err
	}

	var event map[string]any
	if err := readJSON(eventPath, &event); err != nil {
		return err
	}

	triggerID, err := pickTrigger(&workflow, triggerID)
	if err != nil {
		return err
	}

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes(notify.SMTP{})
	e := engine.New(reg)

	if err := e.Validate(ctx, &workflow); err != nil {
		return fmt.Errorf("invalid workflow: %w", err)
	}

	execution := &models.ExecutionContext{
		ID:          "local",
		WorkflowID:  workflow.ID,
		Status:      models.ExecutionStatusRunning,
		TriggerData: event,
		Variables:   workflow.Variables,
		CreatedAt:   time.Now().UTC(),
	}

	deferred, err := e.Run(ctx, &workflow, execution, engine.TriggerActivation(triggerID, event))
	for err == nil && len(deferred) > 0 && execution.Status != models.ExecutionStatusFailed {
		var later []engine.Deferred

		later, err = e.Run(ctx, &workflow, execution, deferred[0].Activation)
		deferred = append(deferred[1:], later...)
	}

	if err != nil {
		return err
	}

	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(execution); err != nil {
		return err
	}

	if execution.Status == models.ExecutionStatusFailed {
		return fmt.Errorf("%w: %s", errExecutionFailed, execution.ErrorMessage)
	}

	return nil
}

func readJSON(path string, target any) error {
	content, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err
	}

	if err := json.Unmarshal(content, target); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	return nil
}

func pickTrigger(workflow *models.Workflow, triggerID string) (string, error) {
	if triggerID != "" {
		return triggerID, nil
	}

	var triggers []string

	for _, node := range workflow.Nodes {
		if node.IsTriggerNode() {
			triggers = append(triggers, node.ID)
		}
	}

	if len(triggers) != 1 {
		return "", fmt.Errorf("workflow has %d trigger nodes, pass --trigger to pick one", len(triggers))
	}

	return triggers[0], nil
}
