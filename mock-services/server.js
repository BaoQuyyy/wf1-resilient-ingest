/**
 * Downstream stand-ins for the resilient ingest pipeline.
 *
 * Three roles, deliberately kept in one small service so the demo starts with
 * a single command:
 *
 *   /crm/*    a CRM API that can be told to fail on demand, so retry and
 *             dead-letter behaviour can be demonstrated instead of described.
 *   /store/*  the pipeline's own state: idempotency ledger, dead-letter queue
 *             and audit log. In production this is Postgres; the interface the
 *             workflow talks to does not change.
 *   /alerts   stand-in for a Slack incoming webhook.
 *
 * Zero dependencies: node:http + node:sqlite.
 */
import { createServer } from 'node:http'
import * as store from './db.js'

const PORT = Number(process.env.PORT ?? 4000)

// ---------------------------------------------------------------------------
// Chaos control: lets the demo script switch the CRM between healthy, flaky
// and hard-down without restarting anything.
// ---------------------------------------------------------------------------
const chaos = {
  mode: 'healthy', // 'healthy' | 'flaky' | 'down'
  failRate: 1.0, // used when mode === 'flaky'
  latencyMs: 0
}

const json = (res, status, body, headers = {}) => {
  const payload = JSON.stringify(body, null, 2)
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    ...headers
  })
  res.end(payload)
}

const readBody = (req) =>
  new Promise((resolve, reject) => {
    let raw = ''
    req.on('data', (c) => {
      raw += c
      // Refuse unbounded bodies rather than letting the process grow until it dies.
      if (raw.length > 1_000_000) {
        reject(new Error('payload too large'))
        req.destroy()
      }
    })
    req.on('end', () => {
      if (!raw) return resolve({})
      try {
        resolve(JSON.parse(raw))
      } catch {
        reject(new Error('body is not valid JSON'))
      }
    })
    req.on('error', reject)
  })

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
const routes = {
  // --- Mock CRM ------------------------------------------------------------
  'POST /crm/contacts': async (req, res, body) => {
    if (chaos.latencyMs) await sleep(chaos.latencyMs)

    // A 4xx from a downstream is NOT retryable: the same payload will fail the
    // same way forever. The pipeline must tell these two classes apart, so the
    // mock emits both.
    if (!body?.email || !body?.name) {
      return json(res, 422, {
        error: 'unprocessable_entity',
        message: 'email and name are required',
        retryable: false
      })
    }

    const shouldFail =
      chaos.mode === 'down' || (chaos.mode === 'flaky' && Math.random() < chaos.failRate)

    if (shouldFail) {
      // Alternate between the two failure shapes a real API produces.
      const rateLimited = Math.random() < 0.5
      if (rateLimited) {
        return json(
          res,
          429,
          { error: 'rate_limited', message: 'too many requests', retryable: true },
          { 'retry-after': '2' }
        )
      }
      return json(res, 503, {
        error: 'service_unavailable',
        message: 'upstream CRM is having a bad day',
        retryable: true
      })
    }

    const idempotencyKey = req.headers['idempotency-key'] ?? body.idempotency_key
    if (!idempotencyKey) {
      return json(res, 400, {
        error: 'bad_request',
        message: 'Idempotency-Key header is required',
        retryable: false
      })
    }

    const created = store.createContact({
      idempotencyKey,
      email: body.email,
      name: body.name,
      source: body.source
    })

    return json(res, created.duplicate ? 200 : 201, {
      id: created.id,
      duplicate: created.duplicate
    })
  },

  'GET /crm/contacts': (req, res) => json(res, 200, store.listContacts()),

  // --- Pipeline state ------------------------------------------------------
  'POST /store/idempotency/claim': (req, res, body) => {
    if (!body?.key) return json(res, 400, { error: 'key is required' })
    const result = store.claimIdempotencyKey(body.key)
    return json(res, 200, result)
  },

  'POST /store/idempotency/complete': (req, res, body) => {
    if (!body?.key || !body?.status) {
      return json(res, 400, { error: 'key and status are required' })
    }
    store.completeIdempotencyKey(body.key, {
      status: body.status,
      contactId: body.contact_id ?? null
    })
    return json(res, 200, { ok: true })
  },

  'POST /store/idempotency/release': (req, res, body) => {
    if (!body?.key) return json(res, 400, { error: 'key is required' })
    store.releaseIdempotencyKey(body.key)
    return json(res, 200, { ok: true })
  },

  'POST /store/dead-letter': (req, res, body) => {
    if (!body?.idempotency_key || !body?.payload || !body?.error) {
      return json(res, 400, { error: 'idempotency_key, payload and error are required' })
    }
    const row = store.insertDeadLetter({
      idempotencyKey: body.idempotency_key,
      payload: body.payload,
      error: body.error,
      attempts: body.attempts ?? 0
    })
    return json(res, 201, row)
  },

  'GET /store/dead-letter': (req, res, _b, url) =>
    json(res, 200, store.listDeadLetter(url.searchParams.get('status') ?? undefined)),

  'POST /store/audit': (req, res, body) => {
    if (!body?.stage || !body?.outcome) {
      return json(res, 400, { error: 'stage and outcome are required' })
    }
    store.audit({
      idempotencyKey: body.idempotency_key ?? null,
      stage: body.stage,
      outcome: body.outcome,
      detail: typeof body.detail === 'string' ? body.detail : JSON.stringify(body.detail ?? null)
    })
    return json(res, 201, { ok: true })
  },

  'GET /store/audit': (req, res, _b, url) =>
    json(res, 200, store.listAudit(url.searchParams.get('key') ?? undefined)),

  // --- Alert sink ----------------------------------------------------------
  'POST /alerts': (req, res, body) => {
    store.insertAlert({ severity: body?.severity ?? 'warning', text: body?.text ?? '(no text)' })
    return json(res, 201, { ok: true })
  },

  'GET /alerts': (req, res) => json(res, 200, store.listAlerts()),

  // --- Admin / demo control ------------------------------------------------
  'GET /admin/chaos': (req, res) => json(res, 200, chaos),

  'POST /admin/chaos': (req, res, body) => {
    if (body?.mode && !['healthy', 'flaky', 'down'].includes(body.mode)) {
      return json(res, 400, { error: "mode must be 'healthy', 'flaky' or 'down'" })
    }
    Object.assign(chaos, {
      mode: body?.mode ?? chaos.mode,
      failRate: body?.failRate ?? chaos.failRate,
      latencyMs: body?.latencyMs ?? chaos.latencyMs
    })
    return json(res, 200, chaos)
  },

  'POST /admin/reset': (req, res) => {
    store.resetAll()
    chaos.mode = 'healthy'
    chaos.failRate = 1.0
    chaos.latencyMs = 0
    return json(res, 200, { ok: true, stats: store.stats() })
  },

  'GET /admin/stats': (req, res) => json(res, 200, store.stats()),

  'GET /health': (req, res) => json(res, 200, { ok: true })
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`)
  const key = `${req.method} ${url.pathname}`

  try {
    // Dead-letter resolve carries an id in the path, so it is matched separately.
    const resolveMatch = url.pathname.match(/^\/store\/dead-letter\/(\d+)\/resolve$/)
    if (req.method === 'POST' && resolveMatch) {
      const body = await readBody(req)
      const result = store.resolveDeadLetter(Number(resolveMatch[1]), body?.note)
      return json(res, result.updated ? 200 : 404, result)
    }

    const handler = routes[key]
    if (!handler) return json(res, 404, { error: 'not_found', path: key })

    const body = req.method === 'POST' ? await readBody(req) : undefined
    return await handler(req, res, body, url)
  } catch (err) {
    // Errors are surfaced, never swallowed: the caller gets a real status code
    // and the reason, and the process keeps serving.
    console.error(`[error] ${key}:`, err.message)
    return json(res, 400, { error: 'bad_request', message: err.message })
  }
})

server.listen(PORT, () => {
  console.log(`wf1 mock services listening on http://localhost:${PORT}`)
  console.log(`  CRM chaos:   POST /admin/chaos {"mode":"down"}`)
  console.log(`  stats:       GET  /admin/stats`)
})
