/**
 * One command to bring the whole stack up and prove it works.
 *
 *   node scripts/dev.mjs          start everything, then verify
 *   node scripts/dev.mjs status   what is running right now
 *   node scripts/dev.mjs stop     shut it down
 *   node scripts/dev.mjs restart  stop, then start
 *
 * Design rule: silence on success. Each step prints one line when it works and
 * everything it knows when it does not, so an unattended run only demands
 * attention when something is actually wrong.
 *
 * Both services are spawned detached with their output redirected to
 * .runtime/*.log, so they outlive this process and this terminal.
 */
import { spawn, execSync } from 'node:child_process'
import { openSync, mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const RUNTIME = join(ROOT, '.runtime')
mkdirSync(RUNTIME, { recursive: true })

const SERVICES_PORT = 4000
const N8N_PORT = 5678
const INGEST_ID = 'wf1ResilientIngst'
const REPLAY_ID = 'wf1bDeadLetterRp'

const green = (s) => `\x1b[32m${s}\x1b[0m`
const red = (s) => `\x1b[31m${s}\x1b[0m`
const yellow = (s) => `\x1b[33m${s}\x1b[0m`
const dim = (s) => `\x1b[2m${s}\x1b[0m`

const ok = (msg) => console.log(`${green('ok')}   ${msg}`)
const info = (msg) => console.log(`${dim('..')}   ${dim(msg)}`)
const warn = (msg) => console.log(`${yellow('warn')} ${msg}`)

function die(msg, detail) {
  console.error(`\n${red('PROBLEM')}  ${msg}\n`)
  if (detail) console.error(dim(String(detail).split('\n').slice(-25).join('\n')))
  console.error(`\n${dim(`logs: ${RUNTIME}`)}\n`)
  process.exit(1)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/** PIDs listening on a TCP port, via netstat (works without extra tooling). */
function pidsOnPort(port) {
  try {
    const out = execSync(`netstat -ano | findstr ":${port}"`, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      shell: 'cmd.exe'
    })
    const pids = new Set()
    for (const line of out.split('\n')) {
      if (!line.includes('LISTENING')) continue
      const pid = line.trim().split(/\s+/).pop()
      if (/^\d+$/.test(pid) && pid !== '0') pids.add(pid)
    }
    return [...pids]
  } catch {
    return [] // findstr exits non-zero when nothing matches
  }
}

function killPort(port, label) {
  const pids = pidsOnPort(port)
  for (const pid of pids) {
    try {
      execSync(`taskkill /F /PID ${pid}`, { stdio: 'ignore' })
    } catch {
      warn(`could not kill ${label} (pid ${pid}) - it may already be gone`)
    }
  }
  return pids.length
}

async function waitFor(label, probe, { timeoutMs = 90_000, intervalMs = 1500 } = {}) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await probe()) return true
    await sleep(intervalMs)
  }
  return false
}

const httpOk = (url) =>
  fetch(url, { headers: { connection: 'close' } })
    .then((r) => r.ok)
    .catch(() => false)

function spawnDetached(label, commandLine, env = {}) {
  const logPath = join(RUNTIME, `${label}.log`)
  const fd = openSync(logPath, 'a')
  // The whole command goes in as one string rather than command + args, because
  // Node deprecates passing an args array alongside shell:true (the args are
  // concatenated unescaped). shell:true is required here to resolve n8n.cmd.
  const child = spawn(commandLine, {
    cwd: ROOT,
    detached: true,
    windowsHide: true,
    shell: true,
    stdio: ['ignore', fd, fd],
    env: { ...process.env, ...env }
  })
  child.unref()
  return { pid: child.pid, logPath }
}

const readLog = (label) => {
  const p = join(RUNTIME, `${label}.log`)
  return existsSync(p) ? readFileSync(p, 'utf8') : '(no log)'
}

const saveState = (state) =>
  writeFileSync(join(RUNTIME, 'state.json'), JSON.stringify(state, null, 2))

// ---------------------------------------------------------------------------

async function startServices() {
  if (await httpOk(`http://localhost:${SERVICES_PORT}/health`)) {
    ok('mock services already running')
    return
  }
  // A stale process holding the port but not answering is worse than none.
  killPort(SERVICES_PORT, 'stale mock services')

  const { pid } = spawnDetached('mock-services', 'node mock-services/server.js')
  const live = await waitFor(
    'mock services',
    () => httpOk(`http://localhost:${SERVICES_PORT}/health`),
    { timeoutMs: 30_000 }
  )
  if (!live) die('mock services did not come up on port 4000', readLog('mock-services'))
  ok(`mock services on :${SERVICES_PORT} (pid ${pid})`)
}

function buildAndImport() {
  const run = (cmd) => {
    try {
      return execSync(cmd, {
        cwd: ROOT,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, N8N_DIAGNOSTICS_ENABLED: 'false' }
      })
    } catch (err) {
      die(`command failed: ${cmd}`, (err.stdout ?? '') + (err.stderr ?? ''))
    }
  }

  run('node workflows/build.mjs')
  ok('workflows built from source')

  // Import must happen while n8n is stopped: the CLI writes straight to the
  // database and a running instance would not pick the change up.
  for (const f of ['01-resilient-ingest.json', '02-dead-letter-replay.json']) {
    const out = run(`n8n import:workflow --input=workflows/${f}`)
    if (!/Successfully imported/.test(out)) die(`import did not confirm success for ${f}`, out)
  }
  run(`n8n publish:workflow --id=${INGEST_ID}`)
  ok('workflows imported and published')
}

async function startN8n() {
  const { pid } = spawnDetached('n8n', 'n8n start', {
    N8N_PORT: String(N8N_PORT),
    N8N_DIAGNOSTICS_ENABLED: 'false',
    N8N_SECURE_COOKIE: 'false'
  })

  const healthy = await waitFor('n8n', () => httpOk(`http://localhost:${N8N_PORT}/healthz`))
  if (!healthy) die('n8n did not become healthy on port 5678', readLog('n8n'))

  // Health comes up BEFORE webhook registration finishes, so probing /healthz
  // is not enough - the endpoint itself has to answer.
  const bound = await waitFor(
    'webhook',
    async () => {
      try {
        const res = await fetch(`http://localhost:${N8N_PORT}/webhook/ingest`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', connection: 'close' },
          body: JSON.stringify({ event_id: '_probe', email: 'probe@example.com', name: 'Probe' })
        })
        return res.status !== 404
      } catch {
        return false
      }
    },
    { timeoutMs: 60_000 }
  )
  if (!bound) {
    die(
      'n8n is up but /webhook/ingest still answers 404.\n' +
        'Usually triggerCount is 0 on the imported workflow - see docs/NOTES.md #2.',
      readLog('n8n')
    )
  }
  ok(`n8n on :${N8N_PORT} (pid ${pid}), webhook bound`)
}

function verify() {
  try {
    const out = execSync('node scripts/verify.mjs', {
      cwd: ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, WF1_REPLAY_ID: REPLAY_ID, N8N_DIAGNOSTICS_ENABLED: 'false' }
    })
    const summary = out.match(/ALL CHECKS PASSED.*/)?.[0] ?? 'passed'
    ok(summary.replace(/\x1b\[\d+m/g, ''))
    return out
  } catch (err) {
    die('verification failed', (err.stdout ?? '') + (err.stderr ?? ''))
  }
}

async function status() {
  const svc = await httpOk(`http://localhost:${SERVICES_PORT}/health`)
  const n8n = await httpOk(`http://localhost:${N8N_PORT}/healthz`)
  console.log(`mock services :${SERVICES_PORT}  ${svc ? green('up') : red('down')}`)
  console.log(`n8n           :${N8N_PORT}  ${n8n ? green('up') : red('down')}`)
  if (svc) {
    const stats = await fetch(`http://localhost:${SERVICES_PORT}/admin/stats`, {
      headers: { connection: 'close' }
    }).then((r) => r.json())
    console.log(dim(`  ${JSON.stringify(stats)}`))
  }
  return svc && n8n
}

function stop() {
  const a = killPort(N8N_PORT, 'n8n')
  const b = killPort(SERVICES_PORT, 'mock services')
  ok(`stopped ${a + b} process(es)`)
}

// ---------------------------------------------------------------------------

const command = process.argv[2] ?? 'start'

switch (command) {
  case 'status':
    process.exit((await status()) ? 0 : 1)
    break

  case 'stop':
    stop()
    break

  case 'restart':
  case 'start': {
    if (command === 'restart') stop()

    await startServices()

    // n8n must be down while the CLI rewrites the workflow rows.
    const killed = killPort(N8N_PORT, 'n8n')
    if (killed) info('stopped running n8n so the import can take effect')

    buildAndImport()
    await startN8n()
    const out = verify()

    saveState({
      startedAt: new Date().toISOString(),
      servicesPort: SERVICES_PORT,
      n8nPort: N8N_PORT,
      lastVerify: out.match(/ALL CHECKS PASSED.*/)?.[0]?.replace(/\x1b\[\d+m/g, '') ?? null
    })

    console.log(`\n${green('stack is up and verified')}`)
    console.log(dim(`  n8n editor    http://localhost:${N8N_PORT}`))
    console.log(dim(`  webhook       http://localhost:${N8N_PORT}/webhook/ingest`))
    console.log(dim(`  logs          ${RUNTIME}`))
    break
  }

  default:
    console.error(`unknown command: ${command}`)
    console.error('usage: node scripts/dev.mjs [start|stop|restart|status]')
    process.exit(2)
}
