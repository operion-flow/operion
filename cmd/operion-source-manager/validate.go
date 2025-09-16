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
	ErrInvalidTriggers        = errors.New("invalid triggers found")
	ErrInvalidProviderConfigs = errors.New("invalid source provider configurations found")
)

func NewValidateCommand() *cli.Command {
	return &cli.Command{
		Name:    "validate",
		Aliases: []string{"v"},
		Usage:   "Validate source provider configurations and workflow triggers",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-url",
				Usage:    "Database connection URL for persistence",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "plugins-path",
				Usage:    "Path to the directory containing source provider plugins",
				Value:    "./plugins",
				Required: false,
			},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			validate = validator.New(validator.WithRequiredStructEnabled())

			logger := slog.With(
				"module", "operion-source-manager",
				"action", "validate",
			)

			persistence := cmd.NewPersistence(ctx, logger, command.String("database-url"))
			defer func() {
				if err := persistence.Close(ctx); err != nil {
					return
				}
			}()

			registry := cmd.NewRegistry(ctx, logger, command.String("plugins-path"))

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

			logger.Info("Validating source provider configurations", "workflows", len(workflows))

			_, _ = fmt.Fprintln(os.Stdout, "Source Provider Validation Results:")
			_, _ = fmt.Fprintln(os.Stdout, "===================================")

			validTriggers := 0
			invalidTriggers := 0
			validProviders := 0
			invalidProviders := 0

			// Get available source providers from registry
			sourceProviders := registry.GetProviders()
			_, _ = fmt.Fprintf(os.Stdout, "Available source providers: %d\n", len(sourceProviders))
			for name, factory := range sourceProviders {
				_, _ = fmt.Fprintf(os.Stdout, "  - %s: %s\n", name, factory.Description())
			}

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
					invalidTriggers++

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
						invalidTriggers++

						continue
					}

					// Validate that SourceID is not empty
					if triggerNode.SourceID == nil || *triggerNode.SourceID == "" {
						_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: SourceID is required for source-based trigger nodes\n")
						invalidTriggers++

						continue
					}

					// Use ProviderID as the source provider type - this should always be set
					var sourceProviderType string
					if triggerNode.ProviderID != nil {
						sourceProviderType = *triggerNode.ProviderID
					}
					if sourceProviderType == "" {
						panic(fmt.Sprintf("ProviderID is empty for workflow %s, trigger node %s - this should not happen", workflow.ID, triggerNode.ID))
					}

					// Validate that the source provider exists
					if factory, exists := sourceProviders[sourceProviderType]; exists {
						_, _ = fmt.Fprintf(os.Stdout, "    ✅ VALID: Source provider '%s' found\n", sourceProviderType)

						// Validate configuration schema if possible
						schema := factory.Schema()
						if schema != nil {
							_, _ = fmt.Fprintf(os.Stdout, "    📋 Configuration schema available\n")
						}

						validProviders++
					} else {
						_, _ = fmt.Fprintf(os.Stdout, "    ❌ INVALID: Source provider '%s' not found\n", sourceProviderType)
						invalidProviders++
					}

					validTriggers++
				}
			}

			_, _ = fmt.Fprintf(os.Stdout, "\nValidation Summary:\n")
			_, _ = fmt.Fprintf(os.Stdout, "  Total triggers: %d\n", invalidTriggers+validTriggers)
			_, _ = fmt.Fprintf(os.Stdout, "  Valid triggers: %d\n", validTriggers)
			_, _ = fmt.Fprintf(os.Stdout, "  Invalid triggers: %d\n", invalidTriggers)
			_, _ = fmt.Fprintf(os.Stdout, "  Valid source providers: %d\n", validProviders)
			_, _ = fmt.Fprintf(os.Stdout, "  Invalid source providers: %d\n", invalidProviders)

			if invalidTriggers > 0 {
				return fmt.Errorf("%w: %d", ErrInvalidTriggers, invalidTriggers)
			}

			if invalidProviders > 0 {
				return fmt.Errorf("%w: %d", ErrInvalidProviderConfigs, invalidProviders)
			}

			_, _ = fmt.Fprintln(os.Stdout, "All source provider configurations are valid! ✅")

			return nil
		},
	}
}
