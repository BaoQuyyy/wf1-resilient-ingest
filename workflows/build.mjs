/**
 * Builds the n8n workflow JSON from source.
 *
 * Hand-edited workflow JSON is unreviewable: the interesting logic ends up as
 * an escaped one-line string inside a node parameter, so a code review can
 * only see that "something changed". Generating it from this file keeps the
 * JavaScript readable and diffable, and makes the exported workflow a build
 * artifact rather than something maintained by hand.
 *
 *   node workflows/build.mjs
 */
import { writeFileSync, mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const OUT = HERE

const SERVICES_URL = process.env.WF1_SERVICES_URL ?? 'http://localhost:4000'

// ---------------------------------------------------------------------------
// Node helpers
// ---------------------------------------------------------------------------
let idCounter = 0
const nid = (name) => `${name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-${++idCounter}`

const node = (name, type, typeVersion, parameters, position, extra = {}) => ({
  parameters,
  id: nid(name),
  name,
  type,
  typeVersion,
  position,
  ...extra
})

const code = (name, jsCode, position, extra = {}) =>
  node(name, 'n8n-nodes-base.code', 2, { jsCode }, position, extra)

const http = (name, parameters, position, extra = {}) =>
  node(name, 'n8n-nodes-base.httpRequest', 4.2, parameters, position, extra)

const respond = (name, { code: statusCode, body }, position) =>
  node(
    name,
    'n8n-nodes-base.respondToWebhook',
    1.1,
    {
      respondWith: 'json',
      responseBody: body,
      // The status code lives under options for this node version. Setting it
      // as a top-level parameter is silently ignored and everything answers
      // 200 - which would make a validation failure look like a success.
      options: { responseCode: statusCode }
    },
    position
  )

/** A boolean IF node driven by a single expression that must evaluate true/false. */
const ifTrue = (name, expression, position) =>
  node(
    name,
    'n8n-nodes-base.if',
    2.2,
    {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'strict', version: 2 },
        conditions: [
          {
            id: nid('cond'),
            leftValue: expression,
            rightValue: '',
            operator: { type: 'boolean', operation: 'true', singleValue: true }
          }
        ],
        combinator: 'and'
      },
      options: {}
    },
    position
  )

/** POST JSON to the services API, with the body supplied as an expression. */
const postJson = (name, path, bodyExpression, position, extra = {}) =>
  http(
    name,
    {
      method: 'POST',
      url: `${SERVICES_URL}${path}`,
      sendBody: true,
      specifyBody: 'json',
      jsonBody: bodyExpression,
      options: {}
    },
    position,
    extra
  )

const connect = (connections, from, to, outputIndex = 0) => {
  connections[from] ??= { main: [] }
  while (connections[from].main.length <= outputIndex) connections[from].main.push([])
  connections[from].main[outputIndex].push({ node: to, type: 'main', index: 0 })
}

// ---------------------------------------------------------------------------
// Workflow 1: resilient ingest
// ---------------------------------------------------------------------------

const VALIDATE_JS = `
// ---------------------------------------------------------------------------
// Schema validation at the front door.
//
// Rejecting malformed payloads here - before any state is claimed and before
// any downstream call is made - is what stops a bad message from failing
// halfway through and leaving the system in an in-between state.
//
// The client gets a specific reason, not a generic 500.
// ---------------------------------------------------------------------------
const raw = $input.first().json;
const body = raw.body ?? raw;

const errors = [];

if (typeof body.event_id !== 'string' || body.event_id.trim() === '') {
  errors.push('event_id must be a non-empty string');
}
if (typeof body.email !== 'string' || !/^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$/.test(body.email)) {
  errors.push('email must be a valid address');
}
if (typeof body.name !== 'string' || body.name.trim() === '') {
  errors.push('name must be a non-empty string');
}

return [{
  json: {
    valid: errors.length === 0,
    errors,
    // The client supplies event_id; we use it verbatim as the idempotency key.
    // Deriving it from a hash of the payload instead would break legitimate
    // retries where the client corrects a field and resends.
    idempotency_key: typeof body.event_id === 'string' ? body.event_id : null,
    event: {
      event_id: body.event_id ?? null,
      email: body.email ?? null,
      name: body.name ?? null,
      source: body.source ?? null
    }
  }
}];
`.trim()

const DELIVER_JS = `
// ---------------------------------------------------------------------------
// Deliver to the CRM, with exponential backoff and honest error classification.
//
// Why this is a Code node and not the built-in "Retry On Fail" setting:
//
//   1. n8n's built-in retry waits a FIXED interval. Hammering a rate-limited
//      API at a fixed interval is how you stay rate-limited. This backs off
//      exponentially (1s, 2s, 4s) with jitter, so a fleet of workers that all
//      failed at once does not retry in lockstep.
//
//   2. The built-in retry cannot tell 429 from 422. Retrying a 422 is pure
//      waste - the same payload will be rejected identically forever. Only
//      429 and 5xx are retried here; 4xx fails fast and goes straight to the
//      dead-letter queue where a human can look at it.
//
//   3. A 429 usually carries Retry-After. Obeying the server's own number is
//      strictly better than guessing.
// ---------------------------------------------------------------------------
const SERVICES_URL = ${JSON.stringify(SERVICES_URL)};
const MAX_ATTEMPTS = 3;
const BASE_DELAY_MS = 1000;

// The immediately preceding node is the idempotency claim, whose output is
// just { claimed: true }. The event itself has to be read back from the
// validation node by name - reaching for $input here silently loses it.
const validated = $('Validate payload').first().json;
const event = validated.event;
const key = validated.idempotency_key;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let attempts = 0;
let lastError = null;
let lastStatus = null;

for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
  attempts = attempt;

  let res;
  try {
    res = await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/crm/contacts',
      headers: { 'idempotency-key': key },
      body: { email: event.email, name: event.name, source: event.source },
      json: true,
      returnFullResponse: true,
      // We want to inspect the status ourselves rather than have the helper
      // throw, because the status is exactly what decides retry vs fail-fast.
      ignoreHttpStatusErrors: true
    });
  } catch (err) {
    // A refused connection or DNS failure never produces a status code. It is
    // transient by nature, so it is treated as retryable - but it must not be
    // allowed to throw out of this node, or the webhook would hang and the
    // event would be lost precisely when the downstream is at its least
    // reliable.
    lastStatus = null;
    lastError = 'network: ' + err.message;
    if (attempt < MAX_ATTEMPTS) {
      await sleep(BASE_DELAY_MS * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 250));
      continue;
    }
    break;
  }

  lastStatus = res.statusCode;

  if (res.statusCode >= 200 && res.statusCode < 300) {
    return [{
      json: {
        delivered: true,
        attempts,
        status_code: res.statusCode,
        contact_id: res.body?.id ?? null,
        was_duplicate_downstream: res.body?.duplicate === true,
        idempotency_key: key,
        event
      }
    }];
  }

  const retryable = res.statusCode === 429 || res.statusCode >= 500;
  lastError = 'HTTP ' + res.statusCode + ': ' + JSON.stringify(res.body);

  if (!retryable) break;

  if (attempt < MAX_ATTEMPTS) {
    const retryAfterHeader = Number(res.headers?.['retry-after']);
    const backoff = Number.isFinite(retryAfterHeader) && retryAfterHeader > 0
      ? retryAfterHeader * 1000
      : BASE_DELAY_MS * Math.pow(2, attempt - 1);
    // Jitter prevents synchronised retries across concurrent executions.
    await sleep(backoff + Math.floor(Math.random() * 250));
  }
}

return [{
  json: {
    delivered: false,
    attempts,
    status_code: lastStatus,
    error: lastError,
    idempotency_key: key,
    event
  }
}];
`.trim()

function buildIngestWorkflow() {
  const nodes = [
    node(
      'Webhook: lead received',
      'n8n-nodes-base.webhook',
      2,
      {
        httpMethod: 'POST',
        path: 'ingest',
        responseMode: 'responseNode',
        options: {}
      },
      [-660, 300],
      { webhookId: 'wf1-ingest-webhook' }
    ),

    code('Validate payload', VALIDATE_JS, [-440, 300]),

    ifTrue('Valid?', '={{ $json.valid }}', [-220, 300]),

    respond(
      'Respond 400: invalid payload',
      {
        code: 400,
        body:
          '={{ JSON.stringify({ error: "validation_failed", details: $json.errors }) }}'
      },
      [0, 460]
    ),

    postJson(
      'Claim idempotency key',
      '/store/idempotency/claim',
      '={{ JSON.stringify({ key: $json.idempotency_key }) }}',
      [0, 160]
    ),

    ifTrue('First delivery?', '={{ $json.claimed }}', [220, 160]),

    postJson(
      'Audit: duplicate suppressed',
      '/store/audit',
      `={{ JSON.stringify({ idempotency_key: $('Validate payload').item.json.idempotency_key, stage: "idempotency", outcome: "duplicate_suppressed", detail: "key already claimed; no downstream call made" }) }}`,
      [440, 320]
    ),

    respond(
      'Respond 200: duplicate',
      {
        code: 200,
        body: `={{ JSON.stringify({ status: "duplicate_ignored", event_id: $('Validate payload').item.json.idempotency_key }) }}`
      },
      [660, 320]
    ),

    code('Deliver to CRM (backoff)', DELIVER_JS, [440, 0]),

    ifTrue('Delivered?', '={{ $json.delivered }}', [660, 0]),

    postJson(
      'Mark key succeeded',
      '/store/idempotency/complete',
      '={{ JSON.stringify({ key: $json.idempotency_key, status: "succeeded", contact_id: $json.contact_id }) }}',
      [880, -120]
    ),

    postJson(
      'Audit: delivered',
      '/store/audit',
      `={{ JSON.stringify({ idempotency_key: $('Deliver to CRM (backoff)').item.json.idempotency_key, stage: "delivery", outcome: "succeeded", detail: "attempts=" + $('Deliver to CRM (backoff)').item.json.attempts }) }}`,
      [1100, -120]
    ),

    respond(
      'Respond 201: created',
      {
        code: 201,
        body: `={{ JSON.stringify({ status: "created", contact_id: $('Deliver to CRM (backoff)').item.json.contact_id, attempts: $('Deliver to CRM (backoff)').item.json.attempts }) }}`
      },
      [1320, -120]
    ),

    postJson(
      'Dead-letter the event',
      '/store/dead-letter',
      '={{ JSON.stringify({ idempotency_key: $json.idempotency_key, payload: $json.event, error: $json.error, attempts: $json.attempts }) }}',
      [880, 120]
    ),

    postJson(
      'Mark key dead-lettered',
      '/store/idempotency/complete',
      `={{ JSON.stringify({ key: $('Deliver to CRM (backoff)').item.json.idempotency_key, status: "dead_lettered" }) }}`,
      [1100, 120]
    ),

    postJson(
      'Alert on-call',
      '/alerts',
      `={{ JSON.stringify({ severity: "error", text: "Ingest dead-lettered after " + $('Deliver to CRM (backoff)').item.json.attempts + " attempts. key=" + $('Deliver to CRM (backoff)').item.json.idempotency_key + " last_error=" + $('Deliver to CRM (backoff)').item.json.error }) }}`,
      [1320, 120]
    ),

    respond(
      'Respond 202: queued for retry',
      {
        code: 202,
        body: `={{ JSON.stringify({ status: "accepted_pending_retry", event_id: $('Deliver to CRM (backoff)').item.json.idempotency_key, note: "stored in dead-letter queue; nothing was lost" }) }}`
      },
      [1540, 120]
    )
  ]

  const c = {}
  connect(c, 'Webhook: lead received', 'Validate payload')
  connect(c, 'Validate payload', 'Valid?')
  connect(c, 'Valid?', 'Claim idempotency key', 0) // true
  connect(c, 'Valid?', 'Respond 400: invalid payload', 1) // false
  connect(c, 'Claim idempotency key', 'First delivery?')
  connect(c, 'First delivery?', 'Deliver to CRM (backoff)', 0) // true
  connect(c, 'First delivery?', 'Audit: duplicate suppressed', 1) // false
  connect(c, 'Audit: duplicate suppressed', 'Respond 200: duplicate')
  connect(c, 'Deliver to CRM (backoff)', 'Delivered?')
  connect(c, 'Delivered?', 'Mark key succeeded', 0) // true
  connect(c, 'Delivered?', 'Dead-letter the event', 1) // false
  connect(c, 'Mark key succeeded', 'Audit: delivered')
  connect(c, 'Audit: delivered', 'Respond 201: created')
  connect(c, 'Dead-letter the event', 'Mark key dead-lettered')
  connect(c, 'Mark key dead-lettered', 'Alert on-call')
  connect(c, 'Alert on-call', 'Respond 202: queued for retry')

  return {
    // Fixed ids keep `n8n import:workflow` idempotent: re-importing updates the
    // same workflow instead of piling up copies.
    id: 'wf1ResilientIngst',
    name: 'WF1 - Resilient ingest (idempotent, retrying, dead-lettered)',
    active: false,
    // n8n only registers webhooks for workflows whose triggerCount it knows
    // about. The UI computes this on save; `import:workflow` does not, so an
    // imported workflow activates but never binds its webhook. Setting it here
    // is what makes a CLI-only setup actually reachable.
    triggerCount: 1,
    nodes,
    connections: c,
    settings: { executionOrder: 'v1' },
    pinData: {}
  }
}

// ---------------------------------------------------------------------------
// Workflow 2: dead-letter replay
// ---------------------------------------------------------------------------

const REPLAY_JS = `
// ---------------------------------------------------------------------------
// Replay everything sitting in the dead-letter queue.
//
// Two things make this safe to run at any time:
//
//   1. The idempotency claim is RELEASED before re-delivery. Without that the
//      key is still marked as seen and the replay would suppress itself as a
//      duplicate - a dead-letter queue you cannot drain is just a slower way
//      of losing data.
//
//   2. The CRM still enforces its own uniqueness on the same key, so even if
//      the original attempt did land downstream before failing on the way
//      back, the replay cannot create a second contact.
//
// Rows that fail again are LEFT PENDING rather than marked resolved.
// ---------------------------------------------------------------------------
const SERVICES_URL = ${JSON.stringify(SERVICES_URL)};

const pending = await this.helpers.httpRequest({
  method: 'GET',
  url: SERVICES_URL + '/store/dead-letter?status=pending',
  json: true
});

const results = [];

for (const row of pending) {
  const key = row.idempotency_key;

  await this.helpers.httpRequest({
    method: 'POST',
    url: SERVICES_URL + '/store/idempotency/release',
    body: { key },
    json: true
  });

  const res = await this.helpers.httpRequest({
    method: 'POST',
    url: SERVICES_URL + '/crm/contacts',
    headers: { 'idempotency-key': key },
    body: {
      email: row.payload.email,
      name: row.payload.name,
      source: row.payload.source
    },
    json: true,
    returnFullResponse: true,
    ignoreHttpStatusErrors: true
  });

  const ok = res.statusCode >= 200 && res.statusCode < 300;

  if (ok) {
    await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/store/idempotency/claim',
      body: { key },
      json: true
    });
    await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/store/idempotency/complete',
      body: { key, status: 'succeeded', contact_id: res.body?.id ?? null },
      json: true
    });
    await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/store/dead-letter/' + row.id + '/resolve',
      body: { note: 'replayed successfully' },
      json: true
    });
    await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/store/audit',
      body: {
        idempotency_key: key,
        stage: 'replay',
        outcome: 'succeeded',
        detail: 'dead_letter_id=' + row.id
      },
      json: true
    });
  } else {
    await this.helpers.httpRequest({
      method: 'POST',
      url: SERVICES_URL + '/store/audit',
      body: {
        idempotency_key: key,
        stage: 'replay',
        outcome: 'failed',
        detail: 'still failing: HTTP ' + res.statusCode
      },
      json: true
    });
  }

  results.push({
    dead_letter_id: row.id,
    idempotency_key: key,
    replayed: ok,
    status_code: res.statusCode
  });
}

return [{
  json: {
    scanned: pending.length,
    replayed: results.filter((r) => r.replayed).length,
    still_failing: results.filter((r) => !r.replayed).length,
    results
  }
}];
`.trim()

function buildReplayWorkflow() {
  const nodes = [
    node('Manual trigger', 'n8n-nodes-base.manualTrigger', 1, {}, [-300, 300]),
    node(
      'Every 15 minutes',
      'n8n-nodes-base.scheduleTrigger',
      1.2,
      { rule: { interval: [{ field: 'minutes', minutesInterval: 15 }] } },
      [-300, 460]
    ),
    code('Replay dead-letter queue', REPLAY_JS, [-40, 380])
  ]

  const c = {}
  connect(c, 'Manual trigger', 'Replay dead-letter queue')
  connect(c, 'Every 15 minutes', 'Replay dead-letter queue')

  return {
    id: 'wf1bDeadLetterRp',
    name: 'WF1b - Dead-letter replay',
    active: false,
    triggerCount: 1,
    nodes,
    connections: c,
    settings: { executionOrder: 'v1' },
    pinData: {}
  }
}

// ---------------------------------------------------------------------------
mkdirSync(OUT, { recursive: true })

const artifacts = [
  ['01-resilient-ingest.json', buildIngestWorkflow()],
  ['02-dead-letter-replay.json', buildReplayWorkflow()]
]

for (const [file, wf] of artifacts) {
  const path = join(OUT, file)
  writeFileSync(path, JSON.stringify(wf, null, 2) + '\n', 'utf8')
  console.log(`wrote ${file} (${wf.nodes.length} nodes)`)
}
