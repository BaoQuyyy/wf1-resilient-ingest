import { DatabaseSync } from 'node:sqlite'
import { mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const DB_PATH = process.env.WF1_DB_PATH ?? join(HERE, 'data', 'ingest.db')

mkdirSync(dirname(DB_PATH), { recursive: true })

const db = new DatabaseSync(DB_PATH)

// WAL keeps readers from blocking the writer, which matters as soon as n8n
// runs several executions concurrently against this same file.
db.exec('PRAGMA journal_mode = WAL')

db.exec(`
  -- The idempotency ledger: one row per business event we have ever accepted.
  -- The PRIMARY KEY is what makes duplicate processing physically impossible.
  -- The second delivery loses the race at the storage layer, not in
  -- application logic that a concurrent request could slip past.
  CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            TEXT PRIMARY KEY,
    first_seen_at  TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('in_flight','succeeded','dead_lettered')),
    contact_id     TEXT,
    completed_at   TEXT
  );

  -- Nothing is ever silently dropped. Anything that exhausts its retries lands
  -- here with the full original payload, so it can be replayed once the
  -- downstream problem is fixed.
  CREATE TABLE IF NOT EXISTS dead_letter (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    error            TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('pending','resolved','abandoned')),
    created_at       TEXT NOT NULL,
    resolved_at      TEXT,
    resolved_note    TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_dead_letter_status ON dead_letter(status);

  -- Every decision the pipeline makes, with its reason. This is the table you
  -- read at 2am when a client asks where their order went.
  CREATE TABLE IF NOT EXISTS audit_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT NOT NULL,
    idempotency_key  TEXT,
    stage            TEXT NOT NULL,
    outcome          TEXT NOT NULL,
    detail           TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_audit_key ON audit_log(idempotency_key);

  -- Stand-in for a Slack incoming webhook.
  CREATE TABLE IF NOT EXISTS alerts (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT NOT NULL,
    severity TEXT NOT NULL,
    text     TEXT NOT NULL
  );

  -- Stand-in for the downstream CRM's own storage, so we can prove that a
  -- replayed or duplicated webhook produced exactly one contact.
  CREATE TABLE IF NOT EXISTS crm_contacts (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    email            TEXT NOT NULL,
    name             TEXT NOT NULL,
    source           TEXT,
    created_at       TEXT NOT NULL
  );
`)

const now = () => new Date().toISOString()

/**
 * Atomically claim an idempotency key.
 * Returns { claimed: true } for the first caller and
 * { claimed: false, existing } for every subsequent one.
 * Atomicity comes from the PRIMARY KEY constraint, so two concurrent webhook
 * deliveries cannot both win.
 */
export function claimIdempotencyKey(key) {
  const res = db
    .prepare(
      `INSERT OR IGNORE INTO idempotency_keys (key, first_seen_at, status)
       VALUES (?, ?, 'in_flight')`
    )
    .run(key, now())

  if (res.changes === 1) return { claimed: true }

  const existing = db.prepare('SELECT * FROM idempotency_keys WHERE key = ?').get(key)
  return { claimed: false, existing }
}

export function completeIdempotencyKey(key, { status, contactId = null }) {
  db.prepare(
    `UPDATE idempotency_keys
     SET status = ?, contact_id = ?, completed_at = ?
     WHERE key = ?`
  ).run(status, contactId, now(), key)
}

/**
 * Release a claim so the event can be processed from scratch later.
 * The replay workflow needs this: without it a dead-lettered event could never
 * be re-processed, because its key would stay claimed forever.
 */
export function releaseIdempotencyKey(key) {
  db.prepare('DELETE FROM idempotency_keys WHERE key = ?').run(key)
}

export function insertDeadLetter({ idempotencyKey, payload, error, attempts }) {
  const res = db
    .prepare(
      `INSERT INTO dead_letter
         (idempotency_key, payload_json, error, attempts, status, created_at)
       VALUES (?, ?, ?, ?, 'pending', ?)`
    )
    .run(idempotencyKey, JSON.stringify(payload), error, attempts, now())
  return { id: Number(res.lastInsertRowid) }
}

export function listDeadLetter(status) {
  const rows = status
    ? db.prepare('SELECT * FROM dead_letter WHERE status = ? ORDER BY id').all(status)
    : db.prepare('SELECT * FROM dead_letter ORDER BY id').all()
  return rows.map((r) => ({ ...r, payload: JSON.parse(r.payload_json) }))
}

export function resolveDeadLetter(id, note) {
  const res = db
    .prepare(
      `UPDATE dead_letter
       SET status = 'resolved', resolved_at = ?, resolved_note = ?
       WHERE id = ? AND status = 'pending'`
    )
    .run(now(), note ?? null, id)
  return { updated: res.changes }
}

export function audit({ idempotencyKey = null, stage, outcome, detail = null }) {
  db.prepare(
    `INSERT INTO audit_log (ts, idempotency_key, stage, outcome, detail)
     VALUES (?, ?, ?, ?, ?)`
  ).run(now(), idempotencyKey, stage, outcome, detail)
}

export function listAudit(key) {
  return key
    ? db.prepare('SELECT * FROM audit_log WHERE idempotency_key = ? ORDER BY id').all(key)
    : db.prepare('SELECT * FROM audit_log ORDER BY id').all()
}

export function insertAlert({ severity, text }) {
  db.prepare('INSERT INTO alerts (ts, severity, text) VALUES (?, ?, ?)').run(now(), severity, text)
}

export function listAlerts() {
  return db.prepare('SELECT * FROM alerts ORDER BY id').all()
}

export function createContact({ idempotencyKey, email, name, source }) {
  const id = `crm_${Math.random().toString(36).slice(2, 10)}`
  try {
    db.prepare(
      `INSERT INTO crm_contacts (id, idempotency_key, email, name, source, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    ).run(id, idempotencyKey, email, name, source ?? null, now())
    return { id, duplicate: false }
  } catch (err) {
    // The UNIQUE constraint is the CRM's own second line of defence. Even if the
    // pipeline's idempotency check were bypassed entirely, the downstream store
    // still refuses to create a second contact for the same event.
    if (String(err.message).includes('UNIQUE')) {
      const row = db
        .prepare('SELECT id FROM crm_contacts WHERE idempotency_key = ?')
        .get(idempotencyKey)
      return { id: row.id, duplicate: true }
    }
    throw err
  }
}

export function listContacts() {
  return db.prepare('SELECT * FROM crm_contacts ORDER BY created_at').all()
}

export function resetAll() {
  for (const t of ['idempotency_keys', 'dead_letter', 'audit_log', 'alerts', 'crm_contacts']) {
    db.exec(`DELETE FROM ${t}`)
  }
}

export function stats() {
  const one = (sql) => db.prepare(sql).get().n
  return {
    idempotency_keys: one('SELECT COUNT(*) n FROM idempotency_keys'),
    crm_contacts: one('SELECT COUNT(*) n FROM crm_contacts'),
    dead_letter_pending: one("SELECT COUNT(*) n FROM dead_letter WHERE status = 'pending'"),
    dead_letter_resolved: one("SELECT COUNT(*) n FROM dead_letter WHERE status = 'resolved'"),
    alerts: one('SELECT COUNT(*) n FROM alerts'),
    audit_rows: one('SELECT COUNT(*) n FROM audit_log')
  }
}
