// Seat availability cleanup worker.
// Redis holds booked seat counts per flight cabin and month (hash fs:{cabinId}:YYYY-MM, one field per departure day with
// bookings; see the API). Month keys expire by themselves two days after their month ends. Once a day, just after UTC
// midnight, this worker also removes the departure days that are already past from the current and previous month's
// keys, so Redis only holds bookings for today onwards. Each run is idempotent and catches up on missed days. Every run
// is recorded in availability_window_log (shown in the admin UI as "Availability log").
import { Pool } from 'pg';
import Redis from 'ioredis';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const AVAILABILITY_DAYS = Number(process.env.AVAILABILITY_DAYS || 365);
const RETRY_MS = 60_000;

// Same calendar (UTC YYYY-MM-DD) and key format as the API: fs:{cabinId}:YYYY-MM, field = day of month.
function todayStr() { return new Date().toISOString().slice(0, 10); }
function addDays(d: string, n: number) {
  const t = new Date(d + 'T00:00:00Z'); t.setUTCDate(t.getUTCDate() + n); return t.toISOString().slice(0, 10);
}
function log(msg: string, extra: object = {}) { console.log(JSON.stringify({ time: new Date().toISOString(), msg, ...extra })); }

type Trigger = 'startup' | 'daily' | 'retry';
type NightCount = { night: string; keys: number };

/** Groups key counts per night, oldest first, for the log. */
function perNight(counts: Map<string, number>): NightCount[] {
  return [...counts].sort(([a], [b]) => a.localeCompare(b)).map(([night, keys]) => ({ night, keys }));
}
function bump(counts: Map<string, number>, night: string) { counts.set(night, (counts.get(night) || 0) + 1); }

async function ensureLogTable() {
  await pool.query(`CREATE TABLE IF NOT EXISTS availability_window_log (
    id BIGSERIAL PRIMARY KEY, ran_at TIMESTAMPTZ NOT NULL DEFAULT now(), trigger TEXT NOT NULL, status TEXT NOT NULL,
    window_first DATE, window_last DATE, added_keys INT NOT NULL DEFAULT 0, removed_keys INT NOT NULL DEFAULT 0,
    added_nights JSONB NOT NULL DEFAULT '[]'::jsonb, removed_nights JSONB NOT NULL DEFAULT '[]'::jsonb,
    duration_ms INT, error TEXT)`);
}

async function writeLog(e: { trigger: Trigger; status: 'OK' | 'FAILED'; first: string; last: string; added: NightCount[]; removed: NightCount[]; ms: number; error?: string }) {
  const sum = (x: NightCount[]) => x.reduce((a, n) => a + n.keys, 0);
  await pool.query(
    `INSERT INTO availability_window_log(trigger,status,window_first,window_last,added_keys,removed_keys,added_nights,removed_nights,duration_ms,error)
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [e.trigger, e.status, e.first, e.last, sum(e.added), sum(e.removed), JSON.stringify(e.added), JSON.stringify(e.removed), e.ms, e.error ?? null]);
}

/** Removes booked departure days before `today`: whole keys of the previous month, past days of the current month. */
async function removePastNights(today: string) {
  const removed = new Map<string, number>();
  const thisMonth = today.slice(0, 7), lastMonth = addDays(`${thisMonth}-01`, -1).slice(0, 7);
  for (const month of [lastMonth, thisMonth]) {
    for await (const keys of redis.scanStream({ match: `fs:{*}:${month}`, count: 1000 }) as AsyncIterable<string[]>) {
      if (!keys.length) continue;
      const all = await redis.pipeline(keys.map(k => ['hkeys', k])).exec();
      const del = redis.pipeline();
      keys.forEach((k, i) => {
        const past = (all![i][1] as string[]).filter(f => month < thisMonth || Number(f) < Number(today.slice(8)));
        if (!past.length) return;
        del.hdel(k, ...past);
        for (const f of past) bump(removed, `${month}-${f.padStart(2, '0')}`);
      });
      await del.exec();
    }
  }
  return perNight(removed);
}

async function roll(trigger: Trigger) {
  const t0 = performance.now();
  const first = todayStr();
  const last = addDays(first, AVAILABILITY_DAYS - 1);
  const added: NightCount[] = []; // nothing is ever added: bookings write their own nights
  let removed: NightCount[] = [];
  try {
    await ensureLogTable();
    removed = await removePastNights(first);
  } catch (err) {
    // Best effort: PostgreSQL itself may be what failed.
    await writeLog({ trigger, status: 'FAILED', first, last, added, removed, ms: Math.round(performance.now() - t0), error: String(err) }).catch(() => {});
    throw err;
  }
  const ms = Math.round(performance.now() - t0);
  await writeLog({ trigger, status: 'OK', first, last, added, removed, ms });
  log('availability window rolled', { trigger, first, last, addedKeys: added.reduce((a, n) => a + n.keys, 0), removedKeys: removed.reduce((a, n) => a + n.keys, 0), ms });
}

function msUntilNextUtcMidnight() {
  const now = new Date();
  // +1s so the API's todayStr() has already moved to the new day.
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1) - now.getTime() + 1000;
}

async function loop(trigger: Trigger) {
  try {
    await roll(trigger);
    setTimeout(() => loop('daily'), msUntilNextUtcMidnight());
  } catch (err) {
    log('roll failed, retrying', { error: String(err), retryInMs: RETRY_MS });
    setTimeout(() => loop('retry'), RETRY_MS);
  }
}

async function shutdown() {
  await Promise.allSettled([pool.end(), redis.quit()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

log('availability worker started', { days: AVAILABILITY_DAYS });
loop('startup');
