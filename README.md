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
| `--schema-registry-url` | `SCHEMA_REGISTRY_URL` | | Confluent Schema Registry for `avro` and `protobuf` events; they are skipped when empty |
| `--schema-registry-username` | `SCHEMA_REGISTRY_USERNAME` | | Schema Registry basic auth username |
| `--schema-registry-password` | `SCHEMA_REGISTRY_PASSWORD` | | Schema Registry basic auth password |

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
| `POST /executions/:id/nodes/:node_id/resume` | Continue a waiting node now. A JSON object in the body becomes the node's result; without a body the node passes on the data it held. Returns 409 when the node is not waiting |
| `POST /webhooks/<tenant>/<path>` | Start one execution per published workflow of `<tenant>` whose webhook trigger listens on `<path>` |
| `GET /catalog/events` | List the events tenants can build workflows on |
| `PUT /platform/event-definitions/:key`, `GET /platform/event-definitions/:key` | Register and read an event definition (platform operator) |
| `PUT /platform/node-types/:id` | Register or replace a remote node type |
| `GET /platform/node-types/:id` | Get a remote node type |
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

`tenant_from` says where the tenant is: `payload` reads the dotted path `tenant_key` from the decoded message, and `header` reads the Kafka header named `tenant_key`. A message on the topic starts only the published `trigger:event` workflows of that tenant. Publishing a workflow whose `trigger:event` names an event missing from the catalog returns 422.

`format` says how the message is decoded into `payload`:

- `json`: the message is a JSON object.
- `avro` and `protobuf`: the message is in the Confluent wire format (a zero byte, the 4-byte schema ID and, for protobuf, the message indexes), and its schema is fetched from `--schema-registry-url` by ID once per process. Protobuf messages follow the protobuf JSON mapping (64-bit integers become strings) with the field names of the `.proto`, fields left at their default value included. Schemas with references to other subjects are not supported yet.

A message that cannot be decoded, has no tenant, or whose schema is not in the registry is logged and skipped. Without `--schema-registry-url`, `avro` and `protobuf` events are skipped too. When the registry cannot be reached or answers with an error, the message is not committed and is read again.

Actions: `httprequest`, `transform`, `log`, `conditional` (ports `true` and `false`) and `switch`. Every action has an `error` port; an error on an unconnected `error` port fails the execution.

Waits: `wait` holds its branch for `duration` (a Go duration such as `72h`) and `wait_until` holds it until `until`, a template that renders an RFC 3339 time. While a branch waits, its node result has status `waiting` and the execution stays `running`; the worker continues the branch from a job due at that time. `operion run` does not wait and continues the branch right away.

Retries: any action node accepts `"retry": {"attempts": 3, "initial": "10s", "max": "5m"}` next to its `config`. When the node fails, by an error or by its `error` port, the worker runs it again from a job after `initial`, doubling the wait up to `max`, until it has run `attempts` times in total. Meanwhile its node result has status `retrying` with the last error, and the execution stays `running`. Only after the last attempt does the failure follow the `error` port or fail the execution. `operion run` retries right away.

Node configs are Go templates. A node reads earlier results with `{{.node_results.<node_id>.<field>}}`, the event with `{{.trigger_data...}}` and workflow variables with `{{.variables...}}`.

### Remote nodes

A node type can live in any service that speaks HTTP. The platform registers it once:

```bash
curl -X PUT localhost:9091/platform/node-types/create_benefit -H 'Content-Type: application/json' -d '{
  "name": "Criar benefício",
  "url": "http://benefits.internal/operion",
  "outputs": ["approved", "rejected"],
  "timeout_seconds": 10,
  "schema": {"required": ["amount"]}
}'
```

Workflows then use `create_benefit` as a node type. `outputs` defaults to `["success"]`, `error` is always added, `timeout_seconds` defaults to 30 (at most 300) and `schema.required` lists the config fields a workflow must set. A built-in node type with the same id wins.

When the node runs, Operion sends `POST <url>` with `Content-Type: application/json`:

```json
{
  "execution_id": "...",
  "workflow_id": "...",
  "node_id": "benefit",
  "config": {"amount": "100"},
  "input": {},
  "trigger_data": {},
  "node_results": {"<node_id>": {}},
  "variables": {}
}
```

`config` is already rendered. The service answers 2xx with the port to follow and its data:

```json
{"port": "approved", "data": {"benefit_id": "b-1"}}
```

A non-2xx status, a timeout, a body that is not JSON or a port outside `outputs` goes to the `error` port with `{"error": "<reason>"}`. Answering `"port": "error"` does the same with your own data. Responses are read up to 1 MiB.

Register the type with a `"secret"` of at least 16 characters and every call is signed. The API stores the secret and never returns it. Each request carries `X-Operion-Timestamp`, in Unix seconds, and `X-Operion-Signature: sha256=<hex>`, the HMAC-SHA256 of `<timestamp>.<raw body>` with the secret. The service recomputes it, compares in constant time and refuses old timestamps:

```go
func verified(secret string, r *http.Request, body []byte) bool {
	timestamp := r.Header.Get("X-Operion-Timestamp")

	sent, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(sent, 0)).Abs() > 5*time.Minute {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)

	return hmac.Equal([]byte(r.Header.Get("X-Operion-Signature")), []byte("sha256="+hex.EncodeToString(mac.Sum(nil))))
}
```

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
