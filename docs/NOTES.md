# Field notes

Things that cost real time while building this on n8n 2.37.10. Every one of
them fails *silently* — the workflow imports, activates and reports success
while doing nothing useful. Worth knowing before you hit them.

## 1. `import:workflow` requires an `id` in the JSON

```
SQLITE_CONSTRAINT: NOT NULL constraint failed: workflow_entity.id
```

The exported JSON from the UI has one; a hand-built file does not. Using a
fixed id also makes re-import idempotent — it updates the same workflow
instead of creating a new copy each time.

## 2. `triggerCount` must be set, or the webhook is never registered

This is the expensive one. The workflow imports, `active = 1`, the row appears
in `webhook_entity`, the event log says `workflow.activated` — and the endpoint
still answers 404.

n8n computes `triggerCount` when a workflow is saved through the UI.
`import:workflow` does not, so it stays `0`, and a workflow with zero known
triggers never binds its webhook. Setting `triggerCount: 1` in the exported
JSON fixes it.

Diagnosis worth remembering: compare against a workflow that *does* work.

```sql
SELECT id, name, active, triggerCount, activeVersionId FROM workflow_entity;
```

A working one showed `triggerCount: 1`; ours showed `0`. That single column
was the whole bug.

## 3. Activation changes need a restart when using the CLI

`n8n update:workflow --active=true` is superseded by `n8n publish:workflow`,
and both print:

> Note: Changes will not take effect if n8n is running.

They mean it. Import, publish, then restart — and give it a few seconds after
`/healthz` returns ok, because webhook registration finishes *after* the health
endpoint starts answering.

## 4. `respondToWebhook`: the status code lives under `options`

```js
// ignored - everything answers 200
{ respondWith: 'json', responseCode: 400, responseBody: ... }

// correct
{ respondWith: 'json', responseBody: ..., options: { responseCode: 400 } }
```

Nothing warns you. A validation failure returns `200` with an error body, which
every client will read as success.

## 5. A Code node's `$input` is the *previous* node's output, not the trigger's

The delivery node sits after the idempotency claim, whose output is just
`{ claimed: true }`. Reading `$input.first().json.event` gave `undefined`:

```
TypeError: Cannot read properties of undefined (reading 'email') [line 40]
```

Reach back by node name instead:

```js
const validated = $('Validate payload').first().json;
```

Obvious in hindsight, easy to write, and it fails at runtime rather than at
import.

## 6. `n8n execute` collides with a running instance

```
n8n Task Broker's port 5679 is already in use.
```

`execute` spins up its own broker. Give it different ports:

```bash
N8N_RUNNERS_BROKER_PORT=5690 N8N_PORT=5691 n8n execute --id <id>
```

## 7. ECONNRESET in the test harness, not in the pipeline

`verify.mjs` blocks its event loop for ~2s while `n8n execute` runs. That is
long enough for the server to close an idle keep-alive socket, so the next
`fetch` reuses a dead one and throws `ECONNRESET`.

Sending `connection: close` fixes it. Worth recognising, because the instinct
is to go hunting in the pipeline — where nothing is actually wrong.

---

## Verified run

`node scripts/verify.mjs` — 17/17 checks passed, exit 0.

```
1. A valid event is delivered exactly once .................... 2/2
2. Redelivering the same event creates no second contact ...... 3/3
3. A malformed payload is rejected at the front door .......... 4/4
4. A hard-down downstream dead-letters instead of losing it ... 5/5
   (delivery took 3427ms across retries - the backoff is real)
5. Once the downstream recovers, the queue drains ............. 3/3
```

Audit trail from that run:

```
delivery     succeeded              evt_alpha
idempotency  duplicate_suppressed   evt_alpha
idempotency  duplicate_suppressed   evt_alpha
idempotency  duplicate_suppressed   evt_alpha
replay       succeeded              evt_bravo
```

Three redeliveries suppressed, one dead-lettered event replayed to success,
and one contact per real event.
