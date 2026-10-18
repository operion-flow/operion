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
curl -s -X POST localhost:9091/workflows -H 'Content-Type: application/json' -d @workflow.json
# {"id": "<workflow-id>", "status": "draft", ...}

curl -s -X POST localhost:9091/workflows/<workflow-id>/publish
# {"id": "<workflow-id>", "status": "published", ...}

curl -s -X POST localhost:9091/webhooks/greet -H 'Content-Type: application/json' -d '{"name": "Ana"}'
# {"executions": ["<execution-id>"]}

curl -s localhost:9091/executions/<execution-id>
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

## HTTP API

| Method and path | Description |
|---|---|
| `POST /workflows` | Create a workflow. It is always stored as a draft |
| `GET /workflows` | List workflows |
| `GET /workflows/:id` | Get a workflow |
| `POST /workflows/:id/publish` | Validate the graph and publish. A graph the engine cannot run returns 422 with every problem found |
| `GET /executions/:id` | Get an execution with its status and node results |
| `POST /executions/:id/nodes/:node_id/resume` | Continue a waiting node now. A JSON object in the body becomes the node's result; without a body the node passes on the data it held. Returns 409 when the node is not waiting |
| `POST /webhooks/<path>` | Start one execution per published workflow whose webhook trigger listens on `<path>` |
| `GET /livez`, `GET /readyz` | Health checks |

Errors are returned as `application/problem+json`.

## Nodes

Triggers:

- `trigger:webhook`: `webhook_path`. The event carries `body`, `headers`, `query`, `method` and `url`.
- `trigger:scheduler`: `cron_expression` (standard five fields) and optional `timezone`.

Actions: `httprequest`, `transform`, `log`, `conditional` (ports `true` and `false`) and `switch`. Every node has an `error` port; an error on an unconnected `error` port fails the execution.

Waits: `wait` holds its branch for `duration` (a Go duration such as `72h`) and `wait_until` holds it until `until`, a template that renders an RFC 3339 time. While a branch waits, its node result has status `waiting` and the execution stays `running`; the worker continues the branch from a job due at that time. `operion run` does not wait and continues the branch right away.

Retries: any action node accepts `"retry": {"attempts": 3, "initial": "10s", "max": "5m"}` next to its `config`. When the node fails, by an error or by its `error` port, the worker runs it again from a job after `initial`, doubling the wait up to `max`, until it has run `attempts` times in total. Meanwhile its node result has status `retrying` with the last error, and the execution stays `running`. Only after the last attempt does the failure follow the `error` port or fail the execution. `operion run` retries right away.

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
