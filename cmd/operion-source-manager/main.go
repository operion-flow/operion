package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/dukex/operion/pkg/cmd"
	"github.com/dukex/operion/pkg/log"
	trc "github.com/dukex/operion/pkg/tracer"
	"github.com/google/uuid"
	cli "github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:                  "operion-source-manager",
		Usage:                 "Start the Operion source provider manager service",
		EnableShellCompletion: true,
		Commands: []*cli.Command{
			NewValidateCommand(),
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "manager-id",
				Aliases: []string{"id"},
				Usage:   "Custom source manager ID (auto-generated if not provided)",
				Value:   "",
				Sources: cli.EnvVars("SOURCE_MANAGER_ID"),
			},
			&cli.StringFlag{
				Name:     "database-url",
				Usage:    "Database connection URL for persistence",
				Required: true,
				Sources:  cli.EnvVars("DATABASE_URL"),
			},
			&cli.StringFlag{
				Name:     "plugins-path",
				Usage:    "Path to the directory containing source provider plugins",
				Value:    "./plugins",
				Required: false,
				Sources:  cli.EnvVars("PLUGINS_PATH"),
			},
			&cli.StringFlag{
				Name:    "providers",
				Usage:   "Comma-separated list of source providers to run (e.g., 'scheduler,webhook'). If empty, runs all available providers.",
				Value:   "",
				Sources: cli.EnvVars("SOURCE_PROVIDERS"),
			},
			&cli.StringFlag{
				Name:     "event-bus",
				Usage:    "Event bus type (kafka, rabbitmq, etc.)",
				Required: true,
				Sources:  cli.EnvVars("EVENT_BUS_TYPE"),
			},
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "Log level (debug, info, warn, error)",
				Value:   "info",
				Sources: cli.EnvVars("LOG_LEVEL"),
			},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			log.Setup(command.String("log-level"))

			tracerProvider, err := trc.InitTracer(ctx, "operion-source-manager")
			if err != nil {
				return fmt.Errorf("failed to initialize tracer: %w", err)
			}
			defer func() {
				if err := tracerProvider.Shutdown(ctx); err != nil {
					slog.Error("Failed to shutdown tracer provider", "error", err)
				}
			}()

			managerID := command.String("manager-id")
			if managerID == "" {
				managerID = "source-manager-" + uuid.New().String()[:8]
			}

			// Parse provider filter
			var providerFilter []string
			if providersStr := command.String("providers"); providersStr != "" {
				providerFilter = strings.Split(providersStr, ",")
				for i, provider := range providerFilter {
					providerFilter[i] = strings.TrimSpace(provider)
				}
			}

			logger := log.WithModule("operion-source-manager").With("manager_id", managerID)

			logger.Info("Initializing Operion Source Provider Manager",
				"manager_id", managerID,
				"providers", providerFilter)

			registry := cmd.NewRegistry(ctx, logger, command.String("plugins-path"))

			sourceEventBus := cmd.NewSourceEventBus(command.String("event-bus"), logger)
			defer func() {
				if err := sourceEventBus.Close(); err != nil {
					logger.Error("Failed to close source event bus", "error", err)
				}
			}()

			// Create event bus for trigger lifecycle events
			eventBus := cmd.NewEventBus(command.String("event-bus"), logger)
			defer func() {
				if err := eventBus.Close(); err != nil {
					logger.Error("Failed to close event bus", "error", err)
				}
			}()

			persistence := cmd.NewPersistence(ctx, logger, command.String("database-url"))
			defer func() {
				if err := persistence.Close(ctx); err != nil {
					logger.Error("Failed to close persistence", "error", err)
				}
			}()

			manager := NewProviderManager(
				managerID,
				persistence,
				sourceEventBus,
				eventBus,
				logger,
				registry,
				providerFilter,
			)

			manager.Start(ctx)

			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		panic(err)
	}
}
