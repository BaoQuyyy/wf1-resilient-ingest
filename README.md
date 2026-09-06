# WF1 — Resilient ingest pipeline

A webhook ingest pipeline that survives the things that actually break in
production: duplicate deliveries, a downstream that rate-limits you, a
downstream that is simply down, and malformed payloads.

**The claim this repo makes is narrow and testable: no event is ever silently
lost, and no event is ever processed twice.** `scripts/verify.mjs` proves both.

![Workflow canvas](docs/screenshots/canvas.png)

Note what takes up most of the canvas: the two failure branches. The happy
path is four nodes. Everything else is what happens when the downstream is
down, the payload is malformed, or the same event arrives twice.

---

## The problem

The default shape of an ingest workflow — *webhook → HTTP request → done* —
fails in four ways that only show up once real traffic hits it:

| What happens | Default workflow | This pipeline |
|---|---|---|
| The sender retries a delivery | Two contacts created | Suppressed at the storage layer |
| Downstream returns 429 | Fails, event lost | Backs off, obeys `Retry-After` |
| Downstream is down for 10 minutes | Fails, event lost | Dead-lettered, alerted, replayable |
| Payload is malformed | Fails halfway through, partial state | Rejected at the front door with a reason |

The first one is the expensive one. A client noticing duplicated orders is a
lost contract; a client noticing a *missing* order is a lost contract twice
over.

---

## Design decisions

### 1. Idempotency is enforced by a PRIMARY KEY, not by an `if`

The pipeline claims the event's key before doing anything else:

```sql
INSERT OR IGNORE INTO idempotency_keys (key, ...) VALUES (?, ...)
```

`changes === 1` means we won the claim and own this event. Anything else means
someone already has it, and we stop.

A "check whether it exists, then insert" pair looks equivalent and is not: two
concurrent deliveries can both pass the check before either inserts. Pushing
the decision into a uniqueness constraint makes the race impossible rather
than unlikely — the storage engine arbitrates, and exactly one caller wins.

The downstream CRM enforces the same uniqueness on its own table, so even a
pipeline bug cannot produce two contacts.

### 2. Retryable and non-retryable failures are treated differently

`429` and `5xx` are retried. `4xx` is not — the same payload will be rejected
identically forever, so retrying it just delays the moment a human finds out.
Non-retryable failures go straight to the dead-letter queue.

This is why delivery is a Code node rather than n8n's built-in *Retry On Fail*
setting, which retries on a fixed interval and cannot tell the two apart.

### 3. Backoff is exponential, with jitter, and defers to `Retry-After`

Waits are 1s, 2s, 4s rather than a fixed interval — retrying a rate-limited API
at a fixed interval is how you stay rate-limited. Random jitter is added so
that concurrent executions which failed together do not retry in lockstep. If
the server sent `Retry-After`, that number wins over our guess.

### 4. Failure is a state in the system, not the absence of success

Exhausted retries produce a dead-letter row containing the full original
payload, the last error and the attempt count — plus an alert. The webhook
answers `202 Accepted`, not `500`, because the event genuinely is accepted:
it is durable and it will be retried.

### 5. The replay path releases the idempotency claim

`02-dead-letter-replay.json` deletes the key claim before re-delivering.
Without that step the replay would suppress itself as a duplicate — and a
dead-letter queue you cannot drain is just a slower way of losing data.

Rows that fail again stay `pending`. Nothing is marked resolved unless it
actually succeeded.

### 6. Validation happens before any state is touched

No key is claimed and no downstream call is made until the payload passes
schema validation, so a malformed event cannot leave half-written state
behind. The caller gets `400` and the specific list of what was wrong.

---

## What is mocked, and what changes in production

This runs entirely on your machine with no external accounts.

| Demo | Production |
|---|---|
| SQLite via `node:sqlite` | Postgres — same three tables, same queries |
| `POST /alerts` sink | Slack incoming webhook or PagerDuty |
| Mock CRM with a chaos switch | The real CRM API |

The workflow talks to the store over HTTP, so swapping SQLite for Postgres
changes the service implementation and not the pipeline. The chaos switch
exists so failure modes can be *demonstrated* rather than described.

---

## Running it

Requires Node 22.5+ (for `node:sqlite`). No other dependencies.

One command brings everything up and verifies it:

```bash
node scripts/dev.mjs
```

It starts the mock services, rebuilds and imports the workflows, restarts n8n,
waits for the webhook to actually bind, and runs the full verification. It
prints one line per step on success and everything it knows on failure, so an
unattended run only asks for attention when something is wrong.

```bash
node scripts/dev.mjs status    # what is running
node scripts/dev.mjs restart   # after editing a workflow
node scripts/dev.mjs stop
```

<details>
<summary>The same steps by hand</summary>

```bash
# 1. downstream stand-ins (leave running)
node mock-services/server.js

# 2. build and import both workflows, then publish WF1
node workflows/build.mjs
n8n import:workflow --input=workflows/01-resilient-ingest.json
n8n import:workflow --input=workflows/02-dead-letter-replay.json
n8n publish:workflow --id=wf1ResilientIngst

# 3. start n8n (CLI activation only takes effect on a restart)
n8n start

# 4. prove it
WF1_REPLAY_ID=wf1bDeadLetterRp node scripts/verify.mjs
```

</details>

Give n8n a few seconds after `/healthz` returns ok — webhook registration
finishes after the health endpoint starts answering, so an immediate request
can still 404.

`docs/NOTES.md` collects the n8n-specific traps hit while building this. They
all fail silently: the workflow imports, activates and reports success while
the endpoint stays dead.

Editing the pipeline logic means editing `workflows/build.mjs` and
re-running it — hand-edited workflow JSON buries the interesting logic in an
escaped one-line string, which makes it unreviewable. Generating it keeps the
JavaScript diffable and the exported JSON a build artifact.

## Driving the failure modes by hand

```bash
# downstream goes hard down
curl -X POST localhost:4000/admin/chaos -H 'content-type: application/json' -d '{"mode":"down"}'

# downstream is intermittent
curl -X POST localhost:4000/admin/chaos -H 'content-type: application/json' -d '{"mode":"flaky","failRate":0.6}'

# back to normal
curl -X POST localhost:4000/admin/chaos -H 'content-type: application/json' -d '{"mode":"healthy"}'

# what is in the queue right now
curl localhost:4000/store/dead-letter?status=pending
curl localhost:4000/admin/stats
```

---

## Verified

`node scripts/verify.mjs` — **17/17 checks passed**, exit 0, on n8n 2.37.10 /
Node 24. The dead-letter run took 3427ms across its retries, which is the
backoff actually waiting rather than failing fast.

## What `verify.mjs` asserts

1. A valid event returns `201` and creates exactly one contact.
2. Three redeliveries of the same `event_id` all return `200 duplicate_ignored`
   and the contact count stays at one.
3. A malformed payload returns `400` with the specific validation errors, and
   claims no idempotency key.
4. With the CRM hard-down: the webhook returns `202`, one row lands in the
   dead-letter queue, an alert is raised, no contact is created, and the call
   took longer than 2.5s — proving the backoff waited rather than failing fast.
5. After the CRM recovers, the replay workflow drains the queue: the row is
   marked resolved and the lead finally exists.
