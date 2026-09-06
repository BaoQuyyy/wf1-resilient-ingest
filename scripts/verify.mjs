/**
 * End-to-end proof that the pipeline behaves as claimed.
 *
 * Every assertion below corresponds to a promise made in the README. If this
 * script exits 0, the screenshots in the portfolio are backed by something
 * that actually ran.
 *
 * Prerequisites:
 *   1. mock services running   ->  npm start --prefix mock-services
 *   2. n8n running with both workflows imported and WF1 active
 *
 *   node scripts/verify.mjs
 */
import { execSync } from 'node:child_process'

const SERVICES = process.env.WF1_SERVICES_URL ?? 'http://localhost:4000'
const N8N = process.env.WF1_N8N_URL ?? 'http://localhost:5678'
const WEBHOOK = `${N8N}/webhook/ingest`
const REPLAY_WORKFLOW_ID = process.env.WF1_REPLAY_ID ?? null

let failures = 0
let checks = 0

const green = (s) => `\x1b[32m${s}\x1b[0m`
const red = (s) => `\x1b[31m${s}\x1b[0m`
const dim = (s) => `\x1b[2m${s}\x1b[0m`

function check(label, actual, expected) {
  checks++
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) {
    console.log(`  ${green('PASS')} ${label}`)
  } else {
    failures++
    console.log(`  ${red('FAIL')} ${label}`)
    console.log(`       expected: ${JSON.stringify(expected)}`)
    console.log(`       actual:   ${JSON.stringify(actual)}`)
  }
}

// `connection: close` on every request. The replay step blocks the event loop
// for a couple of seconds while n8n runs, which is long enough for the server
// to close an idle keep-alive socket; the next fetch would then reuse a dead
// socket and fail with ECONNRESET.
const NO_KEEPALIVE = { connection: 'close' }

const post = async (url, body, headers = {}) => {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...NO_KEEPALIVE, ...headers },
    body: JSON.stringify(body ?? {})
  })
  let parsed = null
  const text = await res.text()
  try {
    parsed = text ? JSON.parse(text) : null
  } catch {
    parsed = text
  }
  return { status: res.status, body: parsed }
}

const get = async (url) => {
  const res = await fetch(url, { headers: NO_KEEPALIVE })
  return { status: res.status, body: await res.json() }
}

const stats = () => get(`${SERVICES}/admin/stats`).then((r) => r.body)
const setChaos = (mode) => post(`${SERVICES}/admin/chaos`, { mode })
const reset = () => post(`${SERVICES}/admin/reset`)

const section = (title) => console.log(`\n${title}`)

// ---------------------------------------------------------------------------

async function preflight() {
  try {
    await get(`${SERVICES}/health`)
  } catch {
    console.error(red(`\nMock services are not reachable at ${SERVICES}.`))
    console.error(`Start them with:  node mock-services/server.js\n`)
    process.exit(2)
  }

  try {
    const probe = await fetch(WEBHOOK, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}'
    })
    if (probe.status === 404) {
      console.error(red(`\nn8n responded 404 at ${WEBHOOK}.`))
      console.error('The workflow is imported but not ACTIVE, or the path differs.\n')
      process.exit(2)
    }
  } catch {
    console.error(red(`\nn8n is not reachable at ${N8N}.`))
    console.error('Start it with:  n8n start\n')
    process.exit(2)
  }
}

async function main() {
  await preflight()
  await reset()
  await setChaos('healthy')

  // -------------------------------------------------------------------------
  section('1. A valid event is delivered exactly once')
  const r1 = await post(WEBHOOK, {
    event_id: 'evt_alpha',
    email: 'alpha@example.com',
    name: 'Alpha Lead',
    source: 'landing-page'
  })
  check('webhook returns 201', r1.status, 201)
  check('one contact exists', (await stats()).crm_contacts, 1)

  // -------------------------------------------------------------------------
  section('2. Redelivering the same event does not create a second contact')
  const dupes = []
  for (let i = 0; i < 3; i++) {
    dupes.push(
      await post(WEBHOOK, {
        event_id: 'evt_alpha',
        email: 'alpha@example.com',
        name: 'Alpha Lead',
        source: 'landing-page'
      })
    )
  }
  check('all redeliveries return 200', [...new Set(dupes.map((d) => d.status))], [200])
  check(
    'all report duplicate_ignored',
    [...new Set(dupes.map((d) => d.body?.status))],
    ['duplicate_ignored']
  )
  check('still exactly one contact', (await stats()).crm_contacts, 1)

  // -------------------------------------------------------------------------
  section('3. A malformed payload is rejected at the front door')
  const bad = await post(WEBHOOK, { event_id: 'evt_bad', email: 'not-an-email' })
  check('webhook returns 400', bad.status, 400)
  check('error is validation_failed', bad.body?.error, 'validation_failed')
  check('no extra contact created', (await stats()).crm_contacts, 1)
  check('no key was claimed for the bad event', (await stats()).idempotency_keys, 1)

  // -------------------------------------------------------------------------
  section('4. A hard-down downstream dead-letters instead of losing the event')
  await setChaos('down')
  const started = Date.now()
  const down = await post(WEBHOOK, {
    event_id: 'evt_bravo',
    email: 'bravo@example.com',
    name: 'Bravo Lead',
    source: 'webinar'
  })
  const elapsed = Date.now() - started

  check('webhook returns 202 (accepted, queued)', down.status, 202)
  const s4 = await stats()
  check('one row in the dead-letter queue', s4.dead_letter_pending, 1)
  check('an alert was raised', s4.alerts, 1)
  check('no contact was created', s4.crm_contacts, 1)
  // 3 attempts => two waits of ~1s and ~2s. Anything under 2.5s means the
  // backoff did not actually happen.
  check('backoff actually waited (>2.5s)', elapsed > 2500, true)
  console.log(dim(`       (delivery took ${elapsed}ms across retries)`))

  // -------------------------------------------------------------------------
  section('5. Once the downstream recovers, the queue drains')
  await setChaos('healthy')

  if (REPLAY_WORKFLOW_ID) {
    // execSync (not execFileSync) so the n8n.cmd shim resolves on Windows.
    //
    // The ports are overridden because `n8n execute` starts its own task
    // broker, which collides with the already-running server on the default
    // ports. Without this it fails with "port 5679 is already in use".
    execSync(`n8n execute --id ${REPLAY_WORKFLOW_ID}`, {
      stdio: 'pipe',
      env: {
        ...process.env,
        N8N_RUNNERS_BROKER_PORT: '5690',
        N8N_PORT: '5691',
        N8N_DIAGNOSTICS_ENABLED: 'false'
      }
    })
  } else {
    console.log(dim('       WF1_REPLAY_ID not set - skipping n8n replay execution'))
  }

  const s5 = await stats()
  if (REPLAY_WORKFLOW_ID) {
    check('dead-letter queue is empty', s5.dead_letter_pending, 0)
    check('the row is marked resolved', s5.dead_letter_resolved, 1)
    check('the replayed lead now exists', s5.crm_contacts, 2)
  }

  // -------------------------------------------------------------------------
  section('Audit trail')
  const audit = await get(`${SERVICES}/store/audit`)
  console.log(dim(`       ${audit.body.length} audit rows recorded`))
  for (const row of audit.body) {
    console.log(dim(`       ${row.stage.padEnd(12)} ${row.outcome.padEnd(22)} ${row.idempotency_key ?? ''}`))
  }

  // -------------------------------------------------------------------------
  console.log(
    `\n${failures === 0 ? green('ALL CHECKS PASSED') : red(`${failures} CHECK(S) FAILED`)}  (${checks - failures}/${checks})\n`
  )
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((err) => {
  console.error(red(`\nverify.mjs crashed: ${err.message}`))
  if (err.cause) console.error(red(`  cause: ${err.cause.code ?? ''} ${err.cause.message ?? err.cause}`))
  console.error(dim(err.stack))
  process.exit(3)
})
