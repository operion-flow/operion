# Operion

Operion runs event-driven workflows written in Go. A workflow is a graph of nodes: a trigger (a webhook or a schedule) starts an execution, and each node's output port decides which nodes run next. A single binary, `operion`, serves the HTTP API and runs executions from PostgreSQL.

## Quick start

Build the binary (Go 1.27 or later):

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
export OPERION_SECRETS_KEY="$(openssl rand -base64 32)"
DATABASE_URL="postgres://localhost:5432/operion?sslmode=disable" OPERION_API_TOKEN=dev-token ./operion serve
```

`OPERION_SECRETS_KEY` encrypts the tenants' [secrets](#secrets). Keep it: secrets saved under one key cannot be read with another.

Create the workflow, publish it, call its webhook and read the execution:

```bash
curl -s -X POST localhost:9091/workflows -H 'Authorization: Bearer dev-token' -H 'X-Tenant-Id: acme' -H 'Content-Type: application/json' -d @workflow.json
# {"id": "<workflow-id>", "status": "draft", "tenant_id": "acme", ...}

curl -s -X POST localhost:9091/workflows/<workflow-id>/publish -H 'Authorization: Bearer dev-token' -H 'X-Tenant-Id: acme'
# {"id": "<workflow-id>", "status": "published", ...}

curl -s -X POST localhost:9091/webhooks/acme/greet -H 'Content-Type: application/json' -d '{"name": "Ana"}'
# {"executions": ["<execution-id>"]}

curl -s localhost:9091/executions/<execution-id> -H 'Authorization: Bearer dev-token' -H 'X-Tenant-Id: acme'
# {"id": "<execution-id>", "status": "completed", "node_results": {...}, ...}
```

## Commands

`operion run`

| Flag | Description |
|---|---|
| `--workflow` | Path to the workflow JSON (required) |
| `--event` | Path to the event JSON passed to the trigger (required) |
| `--trigger` | Trigger node ID, required when the workflow has more than one trigger |
| `--node-types` | JSON file of remote node types keyed by id, for workflows that use them |
| `--secrets` | JSON file of secret values by name, such as `{"crm_token": "..."}`, for workflows that use `{{secret "name"}}` |

`operion serve`

| Flag | Environment | Default | Description |
|---|---|---|---|
| `--database-url` | `DATABASE_URL` | | PostgreSQL connection URL (required) |
| `--port` | `PORT` | `9091` | HTTP port |
| `--api-token` | `OPERION_API_TOKEN` | | Bearer token every API route requires (required) |
| `--secrets-key` | `OPERION_SECRETS_KEY` | | Base64 of the 32-byte key that encrypts tenant secrets, such as `openssl rand -base64 32` (required) |
| `--public-url` | `OPERION_PUBLIC_URL` | | Base URL remote nodes call back to, such as `http://operion:9091`; without it a remote node that answers 202 fails |
| `--poll-interval` | | `1s` | How long the worker and the scheduler wait when nothing is due |
| `--log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `--kafka-brokers` | `KAFKA_BROKERS` | | Kafka brokers for `trigger:event` and `trigger:kafka` workflows; Kafka is off when empty |
| `--kafka-group` | `KAFKA_GROUP` | `operion` | Kafka consumer group |
| `--schema-registry-url` | `SCHEMA_REGISTRY_URL` | | Confluent Schema Registry for `avro` and `protobuf` events; they are skipped when empty |
| `--schema-registry-username` | `SCHEMA_REGISTRY_USERNAME` | | Schema Registry basic auth username |
| `--schema-registry-password` | `SCHEMA_REGISTRY_PASSWORD` | | Schema Registry basic auth password |
| `--node-types` | `NODE_TYPES_FILE` | | JSON file of remote node types keyed by id, registered on start |

Traces are exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set.

## HTTP API

Every route below except the webhooks needs `Authorization: Bearer <token>` with the token given to `serve`; without it the answer is a 401. The health checks `/livez` and `/readyz` are open too. The token belongs to a trusted backend, such as the platform's own backend for its users, and it can call every route, including `/platform/*`.

Workflow, execution and secret routes belong to a tenant, sent in the `X-Tenant-Id` header. A request without it is a 400, and another tenant's workflow or execution is a 404. Operion trusts the header, so the backend holding the token decides which tenant a call acts for.

| Method and path | Description |
|---|---|
| `POST /workflows` | Create a workflow for the tenant. It is always stored as a draft |
| `GET /workflows` | List the tenant's workflows |
| `GET /workflows/:id` | Get a workflow |
| `POST /workflows/:id/publish` | Validate the graph and publish. A graph the engine cannot run returns 422 with every problem found |
| `GET /workflows/:id/executions` | List the workflow's executions, newest first; `?limit=` up to 500, default 50 |
| `GET /executions/:id` | Get an execution with its status and node results |
| `POST /executions/:id/nodes/:node_id/resume` | Continue a waiting node now. A JSON object in the body becomes the node's result; without a body the node passes on the data it held. A remote node waiting for its callback needs the answer itself, `{"port": "<output>", "data": {...}}`, and returns 400 without a `port`. Returns 409 when the node is not waiting or was already resumed |
| `PUT /secrets/:name` | Save a secret of the tenant from `{"value": "..."}`, replacing one with the same name; 204. The name is a lowercase letter followed by up to 63 lowercase letters, digits or underscores |
| `GET /secrets` | List the tenant's secrets as `name` and `updated_at`; values are never returned |
| `DELETE /secrets/:name` | Delete a secret; 204, or 404 when the tenant has no secret with that name |
| `POST /webhooks/<tenant>/<path>` | Start one execution per published workflow of `<tenant>` whose webhook trigger listens on `<path>` |
| `POST /callbacks/<execution>/<node>/<token>` | The `callback_url` a remote node answers through later; see [Remote nodes](#remote-nodes) |
| `GET /catalog/events` | List the events tenants can build workflows on |
| `PUT /platform/event-definitions/:key`, `GET /platform/event-definitions/:key` | Register and read an event definition (platform operator) |
| `PUT /platform/node-types/:id` | Register or replace a remote node type |
| `GET /platform/node-types/:id` | Get a remote node type |
| `GET /livez` | Liveness: 200 while the process serves HTTP |
| `GET /readyz` | Readiness: 200 when Postgres answers a ping within a second, 503 otherwise |

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

Actions: `httprequest`, `transform`, `log`, `conditional` (ports `true` and `false`) and `switch`. Each of them has an `error` port; an error on an unconnected `error` port fails the execution.

To post to Slack, point an `httprequest` at the channel's incoming webhook with a body such as `{"text": "Welcome, {{.node_results.trigger.body.name}}"}`.

Waits: `wait` holds its branch for `duration` (a Go duration such as `72h`) and `wait_until` holds it until `until`, a template that renders an RFC 3339 time. While a branch waits, its node result has status `waiting` and the execution stays `running`; the worker continues the branch from a job due at that time. `operion run` does not wait and continues the branch right away. Both have only a `success` port.

Retries: any action node accepts `"retry": {"attempts": 3, "initial": "10s", "max": "5m"}` next to its `config`. When the node fails, by an error or by its `error` port, the worker runs it again from a job after `initial`, doubling the wait up to `max`, until it has run `attempts` times in total. Meanwhile its node result has status `retrying` with the last error, and the execution stays `running`. Only after the last attempt does the failure follow the `error` port or fail the execution. `operion run` retries right away.

Node configs are Go templates. A node reads earlier results with `{{.node_results.<node_id>.<field>}}`, the event with `{{.trigger_data...}}` and workflow variables with `{{.variables...}}`. Templates cannot read the server's environment variables.

### Secrets

Credentials such as an API token or a Slack webhook URL stay out of workflows. A tenant saves them through `PUT /secrets/:name`, and a node reads them with `{{secret "name"}}`, for example in an `httprequest` header `"Authorization": "Bearer {{secret \"crm_token\"}}"`. An execution only reads the secrets of its workflow's tenant, and a name the tenant does not have fails the node.

Values are encrypted in Postgres with AES-256-GCM under `--secrets-key`, which stays outside the database, and the API never returns them. Before a node result or an error is stored, every secret value of 8 characters or more in it becomes `***`, so executions do not show them; shorter values are not masked. Masking happens when the result is recorded, so later nodes read `***` too: a node that needs the secret reads it with `{{secret}}` itself.

### Remote nodes

A node type can live in any service that speaks HTTP, in any language, deployed next to Operion as its own container. [`examples/benefits`](examples/benefits) is a complete one with a Dockerfile and a compose file. The platform registers it once:

```bash
curl -X PUT localhost:9091/platform/node-types/create_benefit -H 'Authorization: Bearer dev-token' -H 'Content-Type: application/json' -d '{
  "name": "Criar benefício",
  "url": "http://benefits.internal/operion",
  "outputs": ["approved", "rejected"],
  "timeout_seconds": 10,
  "schema": {"required": ["amount"]}
}'
```

Workflows then use `create_benefit` as a node type. `outputs` defaults to `["success"]`, `error` is always added, `timeout_seconds` defaults to 30 (at most 300) and `schema.required` lists the config fields a workflow must set. A built-in node type with the same id wins.

Node types can also live in a file, a JSON object keyed by node type id with the same body as the `PUT`:

```json
{
  "create_benefit": {
    "name": "Criar benefício",
    "url": "http://benefits.internal/operion",
    "outputs": ["approved", "rejected"],
    "schema": {"required": ["amount"]}
  }
}
```

`operion serve --node-types node-types.json` (or `NODE_TYPES_FILE`) registers every entry on start, replacing a stored type with the same id, and does not start when an entry is invalid. Removing an entry from the file does not remove it from the database. `operion run --node-types node-types.json` resolves remote nodes from the file without a database, so a workflow can be tried against a node running on your machine.

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

A service that needs longer than the call, such as an approval or a provisioning queue, answers `202 Accepted` instead, optionally with `{"timeout_seconds": 259200}` (default 24 hours, at most a year). Operion holds the node, with status `waiting` and `answer_by`, and the branch continues when the service posts the answer to the `callback_url` it received in the request:

```bash
curl -X POST "$CALLBACK_URL" -H 'Content-Type: application/json' -d '{"port": "approved", "data": {"benefit_id": "b-1"}}'
```

The body is the same `port` and `data` as a direct answer. The callback answers 202 when it is taken, 409 when the node was already answered or its deadline passed, and 404 for any other URL; the first answer wins. The URL carries a single-use token, so the service needs no Operion credentials. If no answer arrives by the deadline, the node goes to the `error` port. A retry policy covers the call itself, not an error that arrives through the callback. `serve` sends `callback_url` only when it has `--public-url`, and `operion run` cannot wait, so there a 202 fails at once.

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
