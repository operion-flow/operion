---
title: "Debugging Workflows"
description: "Step-by-step guide for troubleshooting workflow execution issues in Operion"
tags: [workflows, debugging, troubleshooting, how-to]
---

# Workflow Debugging How-to

**Audience:** Developers building workflows and integrating with Operion  
**Type:** How-to (problem → solution)  
**Applies to:** API, Worker, Activator, Source Manager, Visual Editor

---

## What you’ll learn

- Read and use execution logs to troubleshoot
- Debug step-by-step workflow execution
- Test and validate template evaluations
- Interpret error traces and messages
- Recognize common failure patterns
- Use built-in tools/commands to fix issues fast

---

## Quick mental model

Operion is event-driven:

1. **Source Providers** (via **Source Manager**) emit **source events** (e.g., scheduler/webhook).  
2. **Activator** maps source events → **WorkflowTriggered** for matching workflows.  
3. **Worker** executes workflow **step-by-step**:
   - Publishes `WorkflowStepAvailable` → executes action → publishes `WorkflowStepFinished`/`WorkflowStepFailed`.
   - Moves to `OnSuccess`/`OnFailure` next step until `WorkflowFinished`.

👉 **Where to look for issues**
- **Triggering issues** → Source Manager & Activator logs
- **Execution/step issues** → Worker logs
- **API/CRUD/schema issues** → API logs

---

## Enable detailed logs

All services support `LOG_LEVEL=debug`:

```bash
LOG_LEVEL=debug ./bin/api
LOG_LEVEL=debug ./bin/operion-source-manager --database-url file://./data --providers scheduler
LOG_LEVEL=debug ./bin/operion-activator --database-url file://./data
LOG_LEVEL=debug ./bin/operion-worker --database-url file://./data
```

---

## Debugging workflows
1. Confirm workflow exists
```bash
curl -s http://localhost:3000/workflows | jq '.[] | {id,name,status}'
curl -s http://localhost:3000/health
```

2. Trace execution in logs

#### Activator
```bash
INFO  activator  workflow_id=... trigger=kafka msg="Workflow triggered"
```

#### Worker
```bash
INFO  worker workflow_id=... step_id=step-1 msg="Executing step"
INFO  worker workflow_id=... step_id=step-1 msg="Step finished successfully"
INFO  worker workflow_id=... event=WorkflowFinished
```

#### On failure
```bash
ERROR worker workflow_id=... step_id=fetch error="http 500 from upstream"
INFO  worker workflow_id=... event=WorkflowStepFailed step_id=fetch
INFO  worker workflow_id=... event=WorkflowStepAvailable next_step_id=log_error
```

3. Debug templates

* `.step_results.<uid>` → previous step results
* `.trigger_data` → data from trigger

Common errors:

* `map has no entry for key "id"` → wrong key/UID
* `<nil pointer evaluating>` → value missing
* Invalid JSON → log before consuming

### Add a debug log step:
```json
{
  "action_id": "log",
  "uid": "debug_ctx",
  "configuration": { "message": "userId={{.step_results.get_user.id}}" }
}
```

---

## Error interpretation
* API: `workflow not found` → wrong ID
* Source Manager: `invalid cron expression` → fix cron
* Activator: `no matching workflows for event` → check filters
* Worker: `http request failed` → check URL/auth; `plugin.Open` → rebuild with `-buildmode=plugin`

---

## Common failure patterns
| Symptom             | Likely Cause                    | Fix                          |
|---------------------|---------------------------------|------------------------------|
| Trigger never fires | Wrong provider/topic/cron       | Check Source Manager logs    |
| First step fails    | Bad action config               | Test action in isolation     |
| Template errors     | Wrong UID/shape                 | Log `.step_results`          |
| Workflow ends early | Missing `OnSuccess`/`OnFailure` | Add wiring                   |
| Plugin won’t load   | Build mismatch                  | `go build -buildmode=plugin` |
| DB errors           | Bad `DATABASE_URL`              | Fix connection & permissions |

---

## Tools & checks
```bash
# Validate providers
./bin/operion-source-manager validate --database-url file://./data

# List available actions
curl -s http://localhost:3000/registry/actions | jq '.[].id'

# Health check
curl -s http://localhost:3000/health | jq .
```

### SQL (Postgres):
```bash
SELECT id, name, status FROM workflows ORDER BY updated_at DESC LIMIT 20;
SELECT uid, id, action_id FROM workflow_steps WHERE workflow_id='<wf-id>';
```

---

## **Example: Bitcoin workflow**

**Symptom**: `fetch_price` step fails intermittently.
Debugging:
* Check worker logs: HTTP 500
* Add retries in step config
* Log raw response before parsing
* Reproduce in single-step workflow

---

## **Checklist**
* `LOG_LEVEL=debug`
* Confirm workflow active
* Did trigger fire?
* Which step failed?
* Check wiring (`OnSuccess`/`OnFailure`)
* Validate templates (log `.step_results`)
* Rebuild plugins if needed
* Check DB + `/health`
* Re-run failing step in isolation

---

## **When to file a bug**

Include:
* Service & version
* Workflow ID + Step UID
* ExecutionID
* Action/Trigger config (no secrets)
* Relevant log lines
* Minimal repro workflow

---