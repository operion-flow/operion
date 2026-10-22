# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Operion is a workflow engine in Go. A workflow is a graph of nodes connected by ports. A trigger starts an execution, and the engine follows the output ports of each node until nothing is left to run. One binary, `operion`, does everything, and PostgreSQL holds all state. The README has the user-facing quick start and HTTP API.

## Where things live

- `cmd/operion`: the binary. `run` executes a workflow file in memory; `serve` runs the HTTP API, webhooks, the scheduler and the worker in one process.
- `pkg/engine`: `Run` executes a workflow graph from an activation; `Validate` checks node types, configs, ports, cycles and reachability before publishing.
- `pkg/worker`: claims due rows from the Postgres `jobs` table with `FOR UPDATE SKIP LOCKED` and runs them with the engine in one transaction.
- `pkg/scheduler`: keeps the `schedules` table in sync with published `trigger:scheduler` nodes and fires the due ones.
- `pkg/ingest`: turns incoming events into work: webhooks (`POST /webhooks/<tenant>/*`) start executions, callbacks (`POST /callbacks/<execution>/<node>/<token>`) resume a remote node that answered 202, and Kafka messages start executions, matched to `trigger:kafka` by topic and to `trigger:event` by catalog event and tenant. `decoder.go` decodes `avro` and `protobuf` catalog events with schemas from the Confluent Schema Registry.
- `pkg/api`: workflow and execution endpoints, errors as `application/problem+json`.
- `pkg/workflow`: workflow repository and publishing; publishing calls `Engine.Validate`.
- `pkg/persistence/postgresql`: Postgres storage. Migrations are the versioned map in `migration.go`; add a new version for every schema change.
- `pkg/nodes`: built-in nodes. `pkg/registry/nodes.go` registers them at compile time; `pkg/protocol/node.go` is the node contract.
- `pkg/template`: Go templates in node configs. A node reads earlier results as `{{.node_results.<node_id>.<field>}}`, the event as `{{.trigger_data...}}`, workflow variables as `{{.variables...}}` and secrets as `{{secret "name"}}`.
- `pkg/secret`: secrets are the `OPERION_SECRET_<NAME>` environment variables; `FromEnviron` reads them and `Mask` replaces their values with `***` in node results and errors before the engine stores them.
- `pkg/models`: shared types.

## Commands

```bash
make build          # ./bin/operion
make test           # Postgres tests serially, the rest in parallel
make test-coverage  # coverage.out and coverage.html
make fmt            # go fmt ./...
make lint           # golangci-lint on changes since origin/main
make mod-check      # go mod verify and go mod tidy
```

The Postgres tests start a container with testcontainers, which needs Docker. Set `TEST_DATABASE_URL` to run them against an existing database instead.

## Coding standards

- Struct tags are vertically aligned within a struct; the `tagalign` linter enforces it.
- Run `golangci-lint run --fix <folder or file>` on what you changed, and `go fmt ./...` before building.
- CI (`.github/workflows/test-and-coverage.yml`) runs gofmt, go vet, staticcheck, deadcode, golangci-lint on new issues, the tests with `-race`, and `make ./bin/operion`.
