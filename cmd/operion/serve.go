package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/log"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/scheduler"
	"github.com/dukex/operion/pkg/worker"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/healthcheck"
	cli "github.com/urfave/cli/v3"
)

func newServeCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Serve the workflow API and webhooks, fire schedules and run executions from the jobs table in one process",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "database-url", Usage: "PostgreSQL connection URL", Required: true, Sources: cli.EnvVars("DATABASE_URL")},
			&cli.IntFlag{Name: "port", Usage: "HTTP port", Value: 9091, Sources: cli.EnvVars("PORT")},
			&cli.DurationFlag{Name: "poll-interval", Usage: "How long the worker and the scheduler wait when nothing is due", Value: time.Second},
			&cli.StringFlag{Name: "log-level", Usage: "debug, info, warn or error", Value: "info", Sources: cli.EnvVars("LOG_LEVEL")},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			return serve(ctx, command.String("database-url"), command.Int("port"), command.Duration("poll-interval"), command.String("log-level"))
		},
	}
}

func serve(ctx context.Context, databaseURL string, port int, pollInterval time.Duration, logLevel string) error {
	log.Setup(logLevel)

	logger := log.WithModule("operion")

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := postgresql.NewPersistence(ctx, logger, databaseURL)
	if err != nil {
		return err
	}

	defer func() { _ = store.Close(context.WithoutCancel(ctx)) }()

	reg := registry.NewRegistry(logger)
	reg.RegisterDefaultNodes()

	e := engine.New(reg)
	jobs := store.Jobs()
	w := worker.New(jobs, store.WorkflowRepository(), e, logger, pollInterval)

	app := fiber.New()
	app.Get(healthcheck.DefaultLivenessEndpoint, healthcheck.NewHealthChecker())
	app.Get(healthcheck.DefaultReadinessEndpoint, healthcheck.NewHealthChecker())
	ingest.NewWebhook(store, jobs, logger).Register(app)
	api.New(workflow.NewRepository(store), workflow.NewPublishingService(store, e), store.ExecutionContextRepository()).Register(app)

	workerDone := make(chan struct{})

	go func() {
		_ = w.Run(ctx)

		close(workerDone)
	}()

	schedulerDone := make(chan struct{})

	go func() {
		_ = scheduler.New(store.Schedules(), logger, pollInterval).Run(ctx)

		close(schedulerDone)
	}()

	logger.InfoContext(ctx, "operion serving", "port", port)

	err = app.Listen(":"+strconv.Itoa(port), fiber.ListenConfig{GracefulContext: ctx, DisableStartupMessage: true})

	stop()
	<-workerDone
	<-schedulerDone

	return err
}
