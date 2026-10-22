# Benefits node

A remote node in under 200 lines of Go with no dependencies. It grants a benefit right away up to 500, and for larger amounts it answers `202` and calls back after a review, the way an approval or a provisioning queue would. It checks the signature Operion sends, and Operion's tests run it against the real remote node client, so the example cannot drift from the contract.

Copy this folder to start a node in Go, or read `main.go` as the reference for another language: `ServeHTTP` is the whole contract, `verified` is the signature check and `answerLater` is the callback.

## Run it with Operion

```bash
cd examples/benefits
docker compose up --build
```

`compose.yaml` builds Operion and this node, and `serve` registers the node type from `node-types.json` at start. Use your own secret in both files, and your own `OPERION_SECRETS_KEY` from `openssl rand -base64 32` instead of the development one.

```bash
token='Authorization: Bearer dev-token'

id=$(curl -s -X POST localhost:9091/workflows -H "$token" -H 'X-Tenant-Id: acme' -H 'Content-Type: application/json' -d @workflow.json | jq -r .id)
curl -s -X POST localhost:9091/workflows/$id/publish -H "$token" -H 'X-Tenant-Id: acme'

curl -s -X POST localhost:9091/webhooks/acme/employees -H 'Content-Type: application/json' -d '{"amount": 100}'
curl -s -X POST localhost:9091/webhooks/acme/employees -H 'Content-Type: application/json' -d '{"amount": 1000}'
```

Read each execution with `curl -s localhost:9091/executions/<id> -H "$token" -H 'X-Tenant-Id: acme'`. The first completes at once. The second stays `running`, with `benefit` waiting, until the node calls back about five seconds later.
