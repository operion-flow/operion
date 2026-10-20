# Operion

Operion runs event-driven workflows written in Go. A workflow is a graph of nodes: a trigger (a webhook or a schedule) starts an execution, and each node's output port decides which nodes run next. A single binary, `operion`, serves the HTTP API and runs executions from PostgreSQL.

## Quick start

Build the binary (Go 1.24 or later):

```bash
go build -o operion ./cmd/operion
```

### Run a workflow in memory

`operion run` validates a workflow file and runs it against an event, with no database. Save this as `workflow.json`:

```json
{
  "name": "greet",
  "description": "Greets whoever calls the webhook",
  "nodes": [
    {"id": "trigger", "name": "trigger", "type": "trigger:webhook", "category": "trigger", "enabled": true, "config": {"webhook_path": "/greet"}},
    {"id": "greet", "name": "greet", "type": "transform", "category": "action", "enabled": true, "config": {"expression": "hello {{.node_results.trigger.body.name}}"}}
  ],
  "connections": [{"id": "c1", "source_port": "trigger:success", "target_port": "greet:main"}]
}
```

and this as `event.json`, in the shape the webhook trigger receives:

```json
{"body": {"name": "Ana"}}
```

Then run:

```bash
./operion run --workflow workflow.json --event event.json
```

It prints the execution as JSON, with `"status": "completed"` and `"result": "hello Ana"` under `node_results.greet.data`. It exits with an error when the execution fails.

### Serve with PostgreSQL

`operion serve` runs the API, the webhooks, the scheduler and the worker in one process. It applies its migrations on start.

```bash
DATABASE_URL="postgres://localhost:5432/operion?sslmode=disable" ./operion serve
```

Create the workflow, publish it, call its webhook and read the execution:

```bash
curl -s -X POST localhost:9091/workflows -H 'X-Tenant-Id: acme' -H 'Content-Type: application/json' -d @workflow.json
# {"id": "<workflow-id>", "status": "draft", "tenant_id": "acme", ...}

curl -s -X POST localhost:9091/workflows/<workflow-id>/publish -H 'X-Tenant-Id: acme'
# {"id": "<workflow-id>", "status": "published", ...}

curl -s -X POST localhost:9091/webhooks/acme/greet -H 'Content-Type: application/json' -d '{"name": "Ana"}'
# {"executions": ["<execution-id>"]}

curl -s localhost:9091/executions/<execution-id> -H 'X-Tenant-Id: acme'
# {"id": "<execution-id>", "status": "completed", "node_results": {...}, ...}
```

## Commands

`operion run`

| Flag | Description |
|---|---|
| `--workflow` | Path to the workflow JSON (required) |
| `--event` | Path to the event JSON passed to the trigger (required) |
| `--trigger` | Trigger node ID, required when the workflow has more than one trigger |

`operion serve`

| Flag | Environment | Default | Description |
|---|---|---|---|
| `--database-url` | `DATABASE_URL` | | PostgreSQL connection URL (required) |
| `--port` | `PORT` | `9091` | HTTP port |
| `--poll-interval` | | `1s` | How long the worker and the scheduler wait when nothing is due |
| `--log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `--kafka-brokers` | `KAFKA_BROKERS` | | Kafka brokers for `trigger:event` and `trigger:kafka` workflows; Kafka is off when empty |
| `--kafka-group` | `KAFKA_GROUP` | `operion` | Kafka consumer group |

Traces are exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set.

## HTTP API

Workflow and execution routes belong to a tenant, sent in the `X-Tenant-Id` header. A request without it is a 400, and another tenant's workflow or execution is a 404. Authentication will replace the header.

| Method and path | Description |
|---|---|
| `POST /workflows` | Create a workflow for the tenant. It is always stored as a draft |
| `GET /workflows` | List the tenant's workflows |
| `GET /workflows/:id` | Get a workflow |
| `POST /workflows/:id/publish` | Validate the graph and publish. A graph the engine cannot run returns 422 with every problem found |
| `GET /workflows/:id/executions` | List the workflow's executions, newest first; `?limit=` up to 500, default 50 |
| `GET /executions/:id` | Get an execution with its status and node results |
| `POST /webhooks/<tenant>/<path>` | Start one execution per published workflow of `<tenant>` whose webhook trigger listens on `<path>` |
| `GET /catalog/events` | List the events tenants can build workflows on |
| `PUT /platform/event-definitions/:key`, `GET /platform/event-definitions/:key` | Register and read an event definition (platform operator) |
| `GET /livez`, `GET /readyz` | Health checks |

Errors are returned as `application/problem+json`.

## Nodes

Triggers:

- `trigger:webhook`: `webhook_path`. The event carries `body`, `headers`, `query`, `method` and `url`.
- `trigger:scheduler`: `cron_expression` (standard five fields) and optional `timezone`.
- `trigger:event`: `event`, the key of a catalog event. The event carries `event`, `tenant_id`, `payload` (the decoded message), `headers`, `key`, `topic`, `partition`, `offset` and `timestamp`.
- `trigger:kafka`: `topic`. Every message on the topic starts every published workflow listening on it, whatever its tenant, so tenant workflows use `trigger:event`.

### Event catalog

The platform operator registers the events tenants can build on with `PUT /platform/event-definitions/:key`:

```json
{
  "name": "Employee Criado",
  "topic": "tp_employee_created",
  "format": "json",
  "tenant_from": "payload",
  "tenant_key": "company.id"
}
```

`tenant_from` says where the tenant is: `payload` reads the dotted path `tenant_key` from the message, and `header` reads the Kafka header named `tenant_key`. A message on the topic starts only the published `trigger:event` workflows of that tenant. A message that is not a JSON object, or has no tenant, is logged and skipped. Only `json` is decoded for now; `avro` and `protobuf` definitions are skipped until their decoders land. Publishing a workflow whose `trigger:event` names an event missing from the catalog returns 422.

Actions: `httprequest`, `transform`, `log`, `conditional` (ports `true` and `false`) and `switch`. Every action has an `error` port; an error on an unconnected `error` port fails the execution.

Waits: `wait` holds its branch for `duration` (a Go duration such as `72h`) and `wait_until` holds it until `until`, a template that renders an RFC 3339 time. While a branch waits, its node result has status `waiting` and the execution stays `running`; the worker continues the branch from a job due at that time. `operion run` does not wait and continues the branch right away.

Node configs are Go templates. A node reads earlier results with `{{.node_results.<node_id>.<field>}}`, the event with `{{.trigger_data...}}` and workflow variables with `{{.variables...}}`.

## Development

```bash
make build          # ./bin/operion
make test           # Postgres tests serially, the rest in parallel
make test-coverage  # coverage.out and coverage.html
make fmt
make lint           # golangci-lint on changes since origin/main
```

The Postgres tests start a container with testcontainers. Set `TEST_DATABASE_URL` to run them against an existing database instead.

## License

[BSD 2-Clause](LICENSE)
