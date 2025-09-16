package main

import (
	"context"
	"os"

	"github.com/dukex/operion/pkg/cmd"
	"github.com/dukex/operion/pkg/log"
	cli "github.com/urfave/cli/v3"
)

const defaultPort = 9091

func main() {
	logger := log.WithModule("api")

	cmd := &cli.Command{
		Name:                  "operion-api",
		Usage:                 "Create and manage workflows",
		EnableShellCompletion: true,
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "Port to run the API server on",
				Value:   defaultPort,
				Sources: cli.EnvVars("PORT"),
			},
			&cli.StringFlag{
				Name:     "database-url",
				Usage:    "Database connection URL for persistence",
				Required: true,
				Sources:  cli.EnvVars("DATABASE_URL"),
			},
			&cli.StringFlag{
				Name:     "plugins-path",
				Usage:    "Path to the directory containing action plugins",
				Value:    "./plugins",
				Required: false,
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

			logger.InfoContext(ctx, "Initializing Operion API")

			registry := cmd.NewRegistry(ctx, logger, command.String("plugins-path"))
			persistence := cmd.NewPersistence(ctx, logger, command.String("database-url"))

			defer func() {
				err := persistence.Close(ctx)
				if err != nil {
					logger.ErrorContext(ctx, "Failed to close persistence", "error", err)
				}
			}()

			// Create event bus for trigger lifecycle events
			eventBus := cmd.NewEventBus(command.String("event-bus"), logger)
			defer func() {
				if err := eventBus.Close(); err != nil {
					logger.ErrorContext(ctx, "Failed to close event bus", "error", err)
				}
			}()

			api := NewAPI(
				logger,
				persistence,
				registry,
				eventBus,
			)

			err := api.Start(command.Int("port"))
			if err != nil {
				logger.ErrorContext(ctx, "Failed to start event-driven worker", "error", err)
			}

			return nil
		},
	}

	err := cmd.Run(context.Background(), os.Args)
	if err != nil {
		panic(err)
	}
}
