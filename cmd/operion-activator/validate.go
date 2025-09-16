package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/dukex/operion/pkg/cmd"
	"github.com/dukex/operion/pkg/models"
	"github.com/dukex/operion/pkg/services"
	"github.com/go-playground/validator/v10"
	"github.com/urfave/cli/v3"
)

var validate *validator.Validate

// Static error variables for linter compliance.
var (
	ErrInvalidTriggerNodes = errors.New("invalid trigger nodes found")
	ErrInvalidNodes        = errors.New("invalid nodes found")
)

func NewValidateCommand() *cli.Command {
	return &cli.Command{
		Name:    "validate",
		Aliases: []string{"v"},
		Usage:   "Validate source configurations and trigger mappings",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-url",
				Usage:    "Database connection URL for persistence",
				Required: true,
			},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			validate = validator.New(validator.WithRequiredStructEnabled())

			logger := slog.With(
				"module", "operion-activator",
				"action", "validate",
			)

			persistence := cmd.NewPersistence(ctx, logger, command.String("database-url"))

			defer func() {
				if err := persistence.Close(ctx); err != nil {
					return
				}
			}()

			workflowService := services.NewWorkflow(persistence)

			result, err := workflowService.ListWorkflows(ctx, &services.ListWorkflowsRequest{
				PerPage:   100,
				Page:      1,
				SortBy:    "created_at",
				SortOrder: "desc",
			})
			if err != nil {
				return fmt.Errorf("failed to fetch workflows: %w", err)
			}
			workflows := result.Workflows

			logger.Info("Validating source trigger nodes", "workflows", len(workflows))

			_, _ = fmt.Fprintln(os.Stdout, "Source Trigger Node Validation Results:")
			_, _ = fmt.Fprintln(os.Stdout, "========================================")

			validTriggerNodes := 0
			invalidTriggerNodes := 0
			validNodes := 0
			invalidNodes := 0

			for _, workflow := range workflows {
				_, _ = fmt.Fprintf(os.Stdout, "\nWorkflow: %s (%s)\n", workflow.Name, workflow.ID)

				// Find trigger nodes in the workflow
				triggerNodes := make([]*models.WorkflowNode, 0)
				for _, node := range workflow.Nodes {
					if node.IsTriggerNode() {
						triggerNodes = append(triggerNodes, node)
					}
				}

				if len(triggerNodes) == 0 {
					_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: No trigger nodes found for this workflow.\n")
					invalidTriggerNodes++

					continue
				}

				for _, triggerNode := range triggerNodes {
					sourceID := "(none)"
					if triggerNode.SourceID != nil {
						sourceID = *triggerNode.SourceID
					}
					_, _ = fmt.Fprintf(os.Stdout, "  Trigger Node: %s (SourceID: %s)\n", triggerNode.ID, sourceID)

					// Validate struct fields
					err = validate.Struct(triggerNode)
					if err != nil {
						var validationErrors validator.ValidationErrors
						if errors.As(err, &validationErrors) {
							_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: %v\n", validationErrors)
						} else {
							_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: %v\n", err)
						}
						invalidTriggerNodes++

						continue
					}

					// Validate that SourceID is not empty (required for activator)
					if triggerNode.SourceID == nil || *triggerNode.SourceID == "" {
						_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: SourceID is required for activator-based trigger nodes\n")
						invalidTriggerNodes++

						continue
					}

					// TODO: Add validation for source event types once source events are defined
					// This could validate that the trigger configuration includes valid event types
					// that the activator can process

					_, _ = fmt.Fprintf(os.Stdout, "    ✅ VALID\n")
					validTriggerNodes++
				}

				// Validate all nodes (including action nodes)
				for _, node := range workflow.Nodes {
					if node.IsTriggerNode() {
						continue // Already validated above
					}
					_, _ = fmt.Fprintf(os.Stdout, "  Action Node: %s\n", node.Name)

					err = validate.Struct(node)
					if err != nil {
						var validationErrors validator.ValidationErrors
						if errors.As(err, &validationErrors) {
							_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: %v\n", validationErrors)
						} else {
							_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: %v\n", err)
						}
						invalidNodes++
					} else {
						validNodes++
						_, _ = fmt.Fprintf(os.Stdout, "    ✅ VALID\n")
					}
				}
			}

			_, _ = fmt.Fprintf(os.Stdout, "\nValidation Summary:\n")
			_, _ = fmt.Fprintf(os.Stdout, "  Total trigger nodes: %d\n", invalidTriggerNodes+validTriggerNodes)
			_, _ = fmt.Fprintf(os.Stdout, "  Valid trigger nodes: %d\n", validTriggerNodes)
			_, _ = fmt.Fprintf(os.Stdout, "  Invalid trigger nodes: %d\n", invalidTriggerNodes)
			_, _ = fmt.Fprintf(os.Stdout, "  Total action nodes: %d\n", invalidNodes+validNodes)
			_, _ = fmt.Fprintf(os.Stdout, "  Valid action nodes: %d\n", validNodes)
			_, _ = fmt.Fprintf(os.Stdout, "  Invalid action nodes: %d\n", invalidNodes)

			if invalidTriggerNodes > 0 {
				return fmt.Errorf("%w: %d", ErrInvalidTriggerNodes, invalidTriggerNodes)
			}

			if invalidNodes > 0 {
				return fmt.Errorf("%w: %d", ErrInvalidNodes, invalidNodes)
			}

			_, _ = fmt.Fprintln(os.Stdout, "All trigger nodes and action nodes are valid for activator processing! ✅")

			return nil
		},
	}
}
