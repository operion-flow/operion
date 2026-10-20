package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dukex/operion/pkg/api"
	"github.com/dukex/operion/pkg/engine"
	"github.com/dukex/operion/pkg/ingest"
	"github.com/dukex/operion/pkg/log"
	"github.com/dukex/operion/pkg/persistence/postgresql"
	"github.com/dukex/operion/pkg/registry"
	"github.com/dukex/operion/pkg/scheduler"
	"github.com/dukex/operion/pkg/tracer"
	"github.com/dukex/operion/pkg/worker"
	"github.com/dukex/operion/pkg/workflow"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/healthcheck"
	"github.com/segmentio/kafka-go"
	cli "github.com/urfave/cli/v3"
)

func newServeCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Serve the workflow API and webhooks, consume Kafka, fire schedules and run executions from the jobs table in one process",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "database-url", Usage: "PostgreSQL connection URL", Required: true, Sources: cli.EnvVars("DATABASE_URL")},
			&cli.IntFlag{Name: "port", Usage: "HTTP port", Value: 9091, Sources: cli.EnvVars("PORT")},
			&cli.StringFlag{Name: "api-token", Usage: "Bearer token every API route requires; webhooks and health checks stay open", Required: true, Sources: cli.EnvVars("OPERION_API_TOKEN")},
			&cli.StringFlag{Name: "public-url", Usage: "Base URL remote nodes call back to, such as http://operion:9091; without it a remote node that answers 202 fails", Sources: cli.EnvVars("OPERION_PUBLIC_URL")},
			&cli.DurationFlag{Name: "poll-interval", Usage: "How long the background loops wait when nothing is due", Value: time.Second},
			&cli.StringFlag{Name: "log-level", Usage: "debug, info, warn or error", Value: "info", Sources: cli.EnvVars("LOG_LEVEL")},
			&cli.StringSliceFlag{Name: "kafka-brokers", Usage: "Kafka brokers for trigger:event and trigger:kafka workflows; Kafka is off when empty", Sources: cli.EnvVars("KAFKA_BROKERS")},
			&cli.StringFlag{Name: "kafka-group", Usage: "Kafka consumer group", Value: "operion", Sources: cli.EnvVars("KAFKA_GROUP")},
			&cli.StringFlag{Name: "schema-registry-url", Usage: "Confluent Schema Registry for avro and protobuf events; they are skipped when empty", Sources: cli.EnvVars("SCHEMA_REGISTRY_URL")},
			&cli.StringFlag{Name: "schema-registry-username", Usage: "Schema Registry basic auth username", Sources: cli.EnvVars("SCHEMA_REGISTRY_USERNAME")},
			&cli.StringFlag{Name: "schema-registry-password", Usage: "Schema Registry basic auth password", Sources: cli.EnvVars("SCHEMA_REGISTRY_PASSWORD")},
			&cli.StringFlag{Name: "node-types", Usage: "JSON file of remote node types keyed by id, registered on start", Sources: cli.EnvVars("NODE_TYPES_FILE")},
		},
		Action: func(ctx context.Context, command *cli.Command) error {
			return serve(ctx, serveOptions{
				databaseURL:  command.String("database-url"),
				port:         command.Int("port"),
				apiToken:     command.String("api-token"),
				publicURL:    command.String("public-url"),
				pollInterval: command.Duration("poll-interval"),
				logLevel:     command.String("log-level"),
				kafkaBrokers: command.StringSlice("kafka-brokers"),
				kafkaGroup:   command.String("kafka-group"),
				schemaRegistry: ingest.SchemaRegistry{
					URL:      command.String("schema-registry-url"),
					Username: command.String("schema-registry-username"),
					Password: command.String("schema-registry-password"),
				},
				nodeTypes: command.String("node-types"),
			})
		},
	}
}

type serveOptions struct {
	databaseURL    string
	port           int
	apiToken       string
	publicURL      string
	pollInterval   time.Duration
	logLevel       string
	kafkaBrokers   []string
	kafkaGroup     string
	schemaRegistry ingest.SchemaRegistry
	nodeTypes      string
}

func serve(ctx context.Context, options serveOptions) error {
	log.Setup(options.logLevel)

	logger := log.WithModule("operion")

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		provider, err := tracer.InitTracer(ctx, "operion")
		if err != nil {
			return err
		}

		defer func() { _ = provider.Shutdown(context.WithoutCancel(ctx)) }()
	}

	nodeTypes, err := readNodeTypes(options.nodeTypes)
	if err != nil {
		return err
	}

	store, err := postgresql.NewPersistence(ctx, logger, options.databaseURL)
	if err != nil {
		return err
	}

	defer func() { _ = store.Close(context.WithoutCancel(ctx)) }()

	for _, nodeType := range nodeTypes {
		if err := store.RemoteNodeTypes().Save(ctx, &nodeType); err != nil {
			return err
		}
	}

	reg := registry.NewRegistry()
	reg.RegisterDefaultNodes()
	reg.UseRemoteTypes(store.RemoteNodeTypes())
	reg.UseCallbackURL(options.publicURL)

	e := engine.New(reg)
	jobs := store.Jobs()

	loops := []func(context.Context) error{
		worker.New(jobs, store.WorkflowRepository(), e, logger, options.pollInterval).Run,
		scheduler.New(store.Schedules(), logger, options.pollInterval).Run,
	}

	if len(options.kafkaBrokers) > 0 {
		newReader := func(topics []string) ingest.KafkaReader {
			return kafka.NewReader(kafka.ReaderConfig{
				Brokers:     options.kafkaBrokers,
				GroupID:     options.kafkaGroup,
				GroupTopics: topics,
				StartOffset: kafka.LastOffset,
			})
		}

		handle := ingest.NewKafka(store, store.EventDefinitions(), jobs, options.schemaRegistry, logger).Handle
		loops = append(loops, ingest.NewKafkaConsumer(newReader, store, handle, options.pollInterval, logger).Run)
	}

	app := fiber.New()
	app.Get(healthcheck.DefaultLivenessEndpoint, healthcheck.NewHealthChecker())
	app.Get(healthcheck.DefaultReadinessEndpoint, healthcheck.NewHealthChecker())
	ingest.NewWebhook(store, jobs, logger).Register(app)
	ingest.NewCallbacks(jobs).Register(app)
	api.New(workflow.NewRepository(store), workflow.NewPublishingService(store, e, store.EventDefinitions()), store.ExecutionContextRepository(), store.EventDefinitions(), jobs, store.RemoteNodeTypes()).Register(app, options.apiToken)

	var running sync.WaitGroup

	for _, loop := range loops {
		running.Add(1)

		go func() {
			defer running.Done()

			_ = loop(ctx)
		}()
	}

	logger.InfoContext(ctx, "operion serving", "port", options.port, "kafka", len(options.kafkaBrokers) > 0)

	err = app.Listen(":"+strconv.Itoa(options.port), fiber.ListenConfig{GracefulContext: ctx, DisableStartupMessage: true})

	stop()
	running.Wait()

	return err
}
