import Fastify from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import { Pool } from 'pg';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import crypto from 'crypto';
import fs from 'fs/promises';
import path from 'path';

const app = Fastify({ logger: true });
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const kafka = new Kafka({ clientId: 'flight-booking-lab-api', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer();

app.register(cors, { origin: process.env.CORS_ORIGIN || true });
app.register(jwt, { secret: process.env.JWT_SECRET || 'dev-secret' });

type Role = 'CUSTOMER'|'SELLER'|'ADMIN';

declare module 'fastify' {
  interface FastifyRequest {
    userCtx?: { id: string; role: Role; impersonatedBy?: string };
  }
}

const PAYMENT_WINDOW_SECONDS = Number(process.env.PAYMENT_WINDOW_SECONDS || 60);
// How many days ahead (from today) departures can be booked.
const AVAILABILITY_DAYS = Number(process.env.AVAILABILITY_DAYS || 365);
const MAX_PASSENGERS = 9;
const MAX_LEGS = 4;                 // round trip with one connection each way
const MIN_CONNECTION_MIN = 90;      // shortest layover search offers and booking accepts
const MAX_CONNECTION_MIN = 12 * 60;
const BOOKING_CUTOFF_MIN = 120;     // a flight can be booked until 2 hours before departure

function id() { return crypto.randomUUID(); }

async function auth(req: any, roles?: Role[]) {
  try {
    const p: any = await req.jwtVerify();
    req.userCtx = { id: p.id, role: p.role, impersonatedBy: p.impersonatedBy };
    if (roles && !roles.includes(p.role)) throw new Error('FORBIDDEN');
  } catch {
    throw { statusCode: 401, message: 'Unauthorized' };
  }
}

async function tryAuth(req: any) {
  if (!req.headers?.authorization) return;
  try {
    const p: any = await req.jwtVerify();
    req.userCtx = { id: p.id, role: p.role, impersonatedBy: p.impersonatedBy };
  } catch { /* anonymous */ }
}

async function addOutbox(client: any, topic: string, key: string, payload: any) {
  await client.query(
    `INSERT INTO outbox_events(id,topic,event_key,payload) VALUES($1,$2,$3,$4)`,
    [id(), topic, key, JSON.stringify(payload)]
  );
}

// ---- Date and timezone helpers ------------------------------------------------------------------------
// A departure date is the local calendar day at the departure airport (YYYY-MM-DD). Instants are UTC milliseconds,
// computed from local wall-clock times with the airport's IANA timezone (Intl, no library).
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
function todayStr() { return new Date().toISOString().slice(0, 10); }
function addDays(d: string, n: number) {
  const t = new Date(d + 'T00:00:00Z'); t.setUTCDate(t.getUTCDate() + n); return t.toISOString().slice(0, 10);
}
const daysBetween = (a: string, b: string) => Math.round((Date.parse(b + 'T00:00:00Z') - Date.parse(a + 'T00:00:00Z')) / 86400000);
const weekdayOf = (d: string) => new Date(d + 'T00:00:00Z').getUTCDay();
const tzFormat = new Map<string, Intl.DateTimeFormat>();
/** Minutes the timezone is ahead of UTC at that instant (DST aware). */
function tzOffsetMin(tz: string, utcMs: number) {
  if (!tzFormat.has(tz)) tzFormat.set(tz, new Intl.DateTimeFormat('en-US', { timeZone: tz, hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }));
  const p: any = Object.fromEntries(tzFormat.get(tz)!.formatToParts(new Date(utcMs)).map(x => [x.type, x.value]));
  return (Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second) - Math.floor(utcMs / 1000) * 1000) / 60000;
}
/** UTC instant of a local date + HH:MM at a timezone. */
function localToUtc(date: string, time: string, tz: string) {
  const guess = Date.parse(`${date}T${time.slice(0, 5)}:00Z`);
  const first = guess - tzOffsetMin(tz, guess) * 60000;
  return guess - tzOffsetMin(tz, first) * 60000;
}

// ---- Airports ------------------------------------------------------------------------------------------
// The market is Tel Aviv <-> Bangkok. aviationstack flight rows carry airport names and timezones but not cities or
// countries, so the airports this lab can meet are listed here. HUB_CANDIDATES are airports with service to both ends;
// the import picks the busiest of them from real TLV departures.
const HOME = 'TLV', AWAY = 'BKK';
const AIRPORT_INFO: Record<string, [city: string, country: string, tz: string]> = {
  TLV: ['Tel Aviv', 'Israel', 'Asia/Jerusalem'], ETM: ['Eilat', 'Israel', 'Asia/Jerusalem'],
  BKK: ['Bangkok', 'Thailand', 'Asia/Bangkok'], DMK: ['Bangkok', 'Thailand', 'Asia/Bangkok'], HKT: ['Phuket', 'Thailand', 'Asia/Bangkok'],
  DXB: ['Dubai', 'United Arab Emirates', 'Asia/Dubai'], AUH: ['Abu Dhabi', 'United Arab Emirates', 'Asia/Dubai'], SHJ: ['Sharjah', 'United Arab Emirates', 'Asia/Dubai'],
  DEL: ['Delhi', 'India', 'Asia/Kolkata'], BOM: ['Mumbai', 'India', 'Asia/Kolkata'], ADD: ['Addis Ababa', 'Ethiopia', 'Africa/Addis_Ababa'],
  AMM: ['Amman', 'Jordan', 'Asia/Amman'], IST: ['Istanbul', 'Türkiye', 'Europe/Istanbul'], ATH: ['Athens', 'Greece', 'Europe/Athens'],
  LCA: ['Larnaca', 'Cyprus', 'Asia/Nicosia'], BAH: ['Manama', 'Bahrain', 'Asia/Bahrain'], CAI: ['Cairo', 'Egypt', 'Africa/Cairo'],
  TAS: ['Tashkent', 'Uzbekistan', 'Asia/Tashkent'], ALA: ['Almaty', 'Kazakhstan', 'Asia/Almaty'], CMB: ['Colombo', 'Sri Lanka', 'Asia/Colombo'],
  DOH: ['Doha', 'Qatar', 'Asia/Qatar'], KWI: ['Kuwait City', 'Kuwait', 'Asia/Kuwait'], MCT: ['Muscat', 'Oman', 'Asia/Muscat'],
};
const HUB_CANDIDATES = ['DXB', 'AUH', 'DEL', 'BOM', 'ADD', 'AMM', 'IST', 'ATH', 'LCA', 'BAH', 'CAI', 'TAS', 'ALA', 'SHJ', 'CMB', 'MCT'];
const DEFAULT_HUBS = ['DXB', 'AUH'];
const MAX_HUBS = 2;

// ---- aviationstack: never the same request twice ----------------------------------------------------------
// Free plan: 100 requests a month. Every request is identified by endpoint + sorted params (never the access key).
// 1. api_requests (UNIQUE request_key) caches every response forever; a key that exists is never called again.
// 2. Snapshot files (data/aviationstack/*.json, committed) hold the same responses, so a new database or a fresh
//    clone rebuilds the catalogue without a single call.
// 3. The row is claimed (status PENDING) and the monthly budget checked in one transaction, under an advisory lock,
//    before the HTTP call: concurrent identical requests cannot both go out, and the budget cannot be overrun.
// 4. A failed request stays FAILED; only an explicit admin retry spends another call on it.
const AS_KEY = process.env.AVIATIONSTACK_KEY || '';
const AS_URL = (process.env.AVIATIONSTACK_URL || 'https://api.aviationstack.com/v1').replace(/\/$/, '');
const AS_BUDGET = Number(process.env.AVIATIONSTACK_MONTHLY_BUDGET || 30);
const AS_PLAN_MONTHLY = 100;
const SNAPSHOT_DIR = process.env.SNAPSHOT_DIR || path.join(process.cwd(), 'data', 'aviationstack');
const BUDGET_LOCK = 7_110_001;
type AsParams = Record<string, string | number>;

function requestKey(endpoint: string, params: AsParams) {
  return `${endpoint}?${Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&')}`;
}
const snapshotFile = (key: string) => path.join(SNAPSHOT_DIR, `${crypto.createHash('sha1').update(key).digest('hex').slice(0, 16)}.json`);
const redact = (s: string) => AS_KEY ? s.split(AS_KEY).join('***') : s;

async function budgetStatus() {
  const r = await pool.query(`SELECT count(*)::int AS used, count(*) FILTER (WHERE ok IS NOT TRUE)::int AS failed
    FROM api_calls WHERE called_at >= date_trunc('month', now())`);
  const total = await pool.query(`SELECT count(*)::int AS n FROM api_calls`);
  return { used: r.rows[0].used, failed: r.rows[0].failed, budget: AS_BUDGET, left: Math.max(0, AS_BUDGET - r.rows[0].used),
    planMonthly: AS_PLAN_MONTHLY, allTime: total.rows[0].n, keyConfigured: !!AS_KEY, baseUrl: AS_URL };
}

/** Loads every snapshot file into api_requests (as SNAPSHOT) that is not there yet. Costs no API calls. */
async function loadSnapshots() {
  let files: string[] = [];
  try { files = (await fs.readdir(SNAPSHOT_DIR)).filter(f => f.endsWith('.json')); } catch { return 0; }
  let loaded = 0;
  for (const f of files) {
    try {
      const s = JSON.parse(await fs.readFile(path.join(SNAPSHOT_DIR, f), 'utf8'));
      if (!s.requestKey || !s.response) continue;
      const r = await pool.query(`INSERT INTO api_requests(request_key,endpoint,params,status,source,http_status,response,rows,created_at,finished_at)
        VALUES($1,$2,$3,'OK','SNAPSHOT',200,$4,$5,$6,$6) ON CONFLICT (request_key) DO NOTHING RETURNING id`,
        [s.requestKey, s.endpoint, JSON.stringify(s.params), JSON.stringify(s.response), s.response?.data?.length ?? null, s.fetchedAt || new Date().toISOString()]);
      loaded += r.rowCount || 0;
    } catch (e) { app.log.warn({ file: f, err: String(e) }, 'unreadable aviationstack snapshot skipped'); }
  }
  return loaded;
}

/** Cached aviationstack GET. Returns the stored response when this exact request was ever made (or snapshotted). */
async function aviationstack(endpoint: string, params: AsParams, opts: { retryFailed?: boolean } = {}) {
  const key = requestKey(endpoint, params);
  let cached = (await pool.query(`SELECT status, response, error FROM api_requests WHERE request_key=$1`, [key])).rows[0];
  if (!cached && await loadSnapshots()) cached = (await pool.query(`SELECT status, response, error FROM api_requests WHERE request_key=$1`, [key])).rows[0];
  if (cached?.status === 'OK') return { key, response: cached.response, called: false };
  if (cached?.status === 'PENDING') throw { statusCode: 409, message: `${key} is in flight or was interrupted; it is never repeated automatically` };
  if (cached?.status === 'FAILED' && !opts.retryFailed) throw { statusCode: 409, message: `${key} failed before (${cached.error}); press Retry to spend one more request on it` };
  if (!AS_KEY) throw { statusCode: 400, message: `AVIATIONSTACK_KEY is not set and there is no snapshot for ${key}` };

  const client = await pool.connect();
  let callId: number;
  try {
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock($1)`, [BUDGET_LOCK]);
    const used = (await client.query(`SELECT count(*)::int AS n FROM api_calls WHERE called_at >= date_trunc('month', now())`)).rows[0].n;
    if (used >= AS_BUDGET) throw { statusCode: 429, message: `Monthly aviationstack budget used up (${used}/${AS_BUDGET}); nothing was called` };
    const claim = cached
      ? await client.query(`UPDATE api_requests SET status='PENDING', source='API', error=NULL, finished_at=NULL WHERE request_key=$1 AND status='FAILED' RETURNING id`, [key])
      : await client.query(`INSERT INTO api_requests(request_key,endpoint,params,status,source) VALUES($1,$2,$3,'PENDING','API')
          ON CONFLICT (request_key) DO NOTHING RETURNING id`, [key, endpoint, JSON.stringify(params)]);
    if (!claim.rowCount) throw { statusCode: 409, message: `${key} was just claimed by another request` };
    callId = (await client.query(`INSERT INTO api_calls(request_key) VALUES($1) RETURNING id`, [key])).rows[0].id;
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }

  const t0 = performance.now();
  let httpStatus: number | null = null;
  try {
    const qs = new URLSearchParams({ access_key: AS_KEY, ...Object.fromEntries(Object.entries(params).map(([k, v]) => [k, String(v)])) });
    const res = await fetch(`${AS_URL}/${endpoint}?${qs}`, { signal: AbortSignal.timeout(30000) });
    httpStatus = res.status;
    const body: any = await res.json().catch(() => null);
    if (!res.ok || !body || body.error) throw new Error(body?.error ? `${body.error.code}: ${body.error.message}` : `HTTP ${res.status}`);
    await pool.query(`UPDATE api_requests SET status='OK', http_status=$2, response=$3, rows=$4, finished_at=now() WHERE request_key=$1`,
      [key, httpStatus, JSON.stringify(body), body.data?.length ?? null]);
    await pool.query(`UPDATE api_calls SET http_status=$2, ok=true, duration_ms=$3 WHERE id=$1`, [callId, httpStatus, Math.round(performance.now() - t0)]);
    await fs.mkdir(SNAPSHOT_DIR, { recursive: true });
    await fs.writeFile(snapshotFile(key), JSON.stringify({ requestKey: key, endpoint, params, fetchedAt: new Date().toISOString(), response: body }, null, 1));
    app.log.info({ key, rows: body.data?.length, ms: Math.round(performance.now() - t0) }, 'aviationstack request made and cached');
    return { key, response: body, called: true };
  } catch (e: any) {
    const msg = redact(String(e?.message || e));
    await pool.query(`UPDATE api_requests SET status='FAILED', http_status=$2, error=$3, finished_at=now() WHERE request_key=$1`, [key, httpStatus, msg]);
    await pool.query(`UPDATE api_calls SET http_status=$2, ok=false, duration_ms=$3 WHERE id=$1`, [callId, httpStatus, Math.round(performance.now() - t0)]);
    throw { statusCode: 502, message: `aviationstack ${key}: ${msg}` };
  }
}

// ---- Import plan: TLV <-> BKK, direct and via the two busiest hubs -------------------------------------------
type PlanStep = { step: string; endpoint: string; params: AsParams; key: string };
const flightsStep = (step: string, dep?: string, arr?: string): PlanStep => {
  const params: AsParams = { limit: 100, ...(dep ? { dep_iata: dep } : {}), ...(arr ? { arr_iata: arr } : {}) };
  return { step, endpoint: 'flights', params, key: requestKey('flights', params) };
};
const BASE_STEPS = [
  flightsStep(`Direct ${HOME} → ${AWAY}`, HOME, AWAY),
  flightsStep(`Direct ${AWAY} → ${HOME}`, AWAY, HOME),
  flightsStep(`All ${HOME} departures (to find the hubs)`, HOME),
];
const hubSteps = (hubs: string[]) => hubs.flatMap(h => [
  flightsStep(`${HOME} → ${h}`, HOME, h), flightsStep(`${h} → ${AWAY}`, h, AWAY), flightsStep(`${AWAY} → ${h}`, AWAY, h), flightsStep(`${h} → ${HOME}`, h, HOME)]);

/** The busiest hub candidates among real TLV departures (operating flights only, codeshares ignored). */
function pickHubs(rows: any[]) {
  const count = new Map<string, number>();
  for (const r of rows || []) {
    const to = r?.arrival?.iata;
    if (r?.departure?.iata === HOME && HUB_CANDIDATES.includes(to) && !r?.flight?.codeshared) count.set(to, (count.get(to) || 0) + 1);
  }
  const found = [...count].sort((a, b) => b[1] - a[1] || HUB_CANDIDATES.indexOf(a[0]) - HUB_CANDIDATES.indexOf(b[0])).map(([h]) => h);
  return [...found, ...DEFAULT_HUBS.filter(h => !found.includes(h))].slice(0, MAX_HUBS);
}

async function importPlan() {
  await loadSnapshots();
  const discovery = (await pool.query(`SELECT response FROM api_requests WHERE request_key=$1 AND status='OK'`, [BASE_STEPS[2].key])).rows[0];
  const hubs = discovery ? pickHubs(discovery.response?.data) : null;
  const steps = [...BASE_STEPS, ...(hubs ? hubSteps(hubs) : [])];
  const rows = new Map((await pool.query(`SELECT request_key, status, source, rows, error, finished_at AS "finishedAt" FROM api_requests WHERE request_key = ANY($1)`,
    [steps.map(s => s.key)])).rows.map((r: any) => [r.request_key, r]));
  const out = steps.map(s => ({ ...s, cached: rows.get(s.key) || null }));
  const uncached = out.filter(s => s.cached?.status !== 'OK').length + (hubs ? 0 : MAX_HUBS * 4);
  return { hubs, hubsPending: !hubs, steps: out, requestsNeeded: uncached, budget: await budgetStatus() };
}

// ---- Turning real flight rows into recurring schedules -----------------------------------------------------
// aviationstack /flights is real-time: it shows today's flights, not future schedules (a paid feature). Every
// operating flight seen becomes a daily schedule for the next AVAILABILITY_DAYS days, with synthetic cabins.
// `scheduled` holds the local wall-clock time at the airport (despite its +00:00 suffix); the airport's timezone
// turns it into an instant. If that gives an impossible duration, the raw value is read as UTC instead.
type ParsedFlight = { airline: { iata: string; name: string }; flightNumber: string; dep: { iata: string; name: string; tz: string };
  arr: { iata: string; name: string; tz: string }; depTime: string; arrTime: string; offset: number; durationMin: number; aircraft: string | null };
function parseFlight(r: any): ParsedFlight | null {
  const dep = r?.departure, arr = r?.arrival;
  if (r?.flight?.codeshared) return null; // a marketing copy of another airline's flight
  if (!dep?.iata || !arr?.iata || !dep.scheduled || !arr.scheduled || !r.airline?.iata || !r.flight?.iata) return null;
  const depTz = dep.timezone || AIRPORT_INFO[dep.iata]?.[2], arrTz = arr.timezone || AIRPORT_INFO[arr.iata]?.[2];
  if (!depTz || !arrTz) return null;
  let depDate = dep.scheduled.slice(0, 10), depTime = dep.scheduled.slice(11, 16), arrDate = arr.scheduled.slice(0, 10), arrTime = arr.scheduled.slice(11, 16);
  let durationMin = (localToUtc(arrDate, arrTime, arrTz) - localToUtc(depDate, depTime, depTz)) / 60000;
  if (!(durationMin >= 30 && durationMin <= 20 * 60)) {
    const d = Date.parse(dep.scheduled), a = Date.parse(arr.scheduled);
    durationMin = (a - d) / 60000;
    if (!(durationMin >= 30 && durationMin <= 20 * 60)) return null;
    const local = (ms: number, tz: string) => new Date(ms + tzOffsetMin(tz, ms) * 60000).toISOString();
    const dl = local(d, depTz), al = local(a, arrTz);
    depDate = dl.slice(0, 10); depTime = dl.slice(11, 16); arrDate = al.slice(0, 10); arrTime = al.slice(11, 16);
  }
  const offset = daysBetween(depDate, arrDate);
  if (offset < 0 || offset > 2) return null;
  return { airline: { iata: String(r.airline.iata).toUpperCase(), name: r.airline.name || r.airline.iata },
    flightNumber: String(r.flight.iata).toUpperCase(), dep: { iata: dep.iata, name: dep.airport || dep.iata, tz: depTz },
    arr: { iata: arr.iata, name: arr.airport || arr.iata, tz: arrTz }, depTime, arrTime, offset, durationMin: Math.round(durationMin),
    aircraft: r.aircraft?.iata || r.aircraft?.icao || null };
}

// Economy / business seats by aircraft type (ICAO or IATA code); unknown types by flight length.
const AIRCRAFT_SEATS: Record<string, [number, number]> = {
  B789: [260, 30], B788: [220, 28], '789': [260, 30], '788': [220, 28], B77W: [310, 42], '77W': [310, 42], B773: [300, 40], B772: [280, 35],
  A359: [280, 32], '359': [280, 32], A35K: [320, 40], A333: [260, 30], '333': [260, 30], A332: [240, 28], '332': [240, 28], A388: [430, 60], '388': [430, 60],
  A321: [180, 12], '321': [180, 12], A21N: [180, 12], '32Q': [180, 12], A320: [150, 12], '320': [150, 12], A20N: [150, 12], '32N': [150, 12],
  B738: [160, 12], '738': [160, 12], B38M: [160, 12], '7M8': [160, 12], B739: [170, 16], B763: [210, 24], '763': [210, 24],
};
function cabinsFor(aircraft: string | null, durationMin: number) {
  const [eco, biz] = (aircraft && AIRCRAFT_SEATS[aircraft]) || (durationMin > 360 ? [250, 30] : [160, 12]);
  const ecoPrice = Math.round((1500 + durationMin * 38) / 100) * 100; // baht, like the hotel lab: ~฿26,600 for 11 hours
  return [{ cabin: 'ECONOMY', seats: eco, price: ecoPrice }, ...(biz ? [{ cabin: 'BUSINESS', seats: biz, price: Math.round(ecoPrice * 3.2 / 100) * 100 }] : [])];
}

async function upsertAirport(client: any, iata: string, name: string, tz: string) {
  const [city, country, knownTz] = AIRPORT_INFO[iata] || [name.replace(/ International| Airport/g, ''), 'Unknown', tz];
  await client.query(`INSERT INTO airports(iata,name,city,country,timezone) VALUES($1,$2,$3,$4,$5)
    ON CONFLICT (iata) DO UPDATE SET name=CASE WHEN airports.name=airports.iata THEN EXCLUDED.name ELSE airports.name END`, [iata, name, city, country, knownTz || tz]);
}
/** Inserts a schedule and its cabins; an existing (flight number, origin) keeps its cabins, prices and bookings. */
async function upsertSchedule(client: any, f: ParsedFlight, source: 'AVIATIONSTACK'|'SAMPLE', sourceKey: string | null) {
  await upsertAirport(client, f.dep.iata, f.dep.name, f.dep.tz);
  await upsertAirport(client, f.arr.iata, f.arr.name, f.arr.tz);
  await client.query(`INSERT INTO airlines(iata,name) VALUES($1,$2) ON CONFLICT (iata) DO NOTHING`, [f.airline.iata, f.airline.name]);
  const s = await client.query(`INSERT INTO flight_schedules(id,airline_iata,flight_number,dep_iata,arr_iata,dep_time,arr_time,arr_day_offset,duration_min,aircraft,source,source_request_key,is_sample)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    ON CONFLICT (flight_number, dep_iata) DO UPDATE SET source_request_key=COALESCE(flight_schedules.source_request_key, EXCLUDED.source_request_key)
    RETURNING id, (xmax = 0) AS inserted`,
    [id(), f.airline.iata, f.flightNumber, f.dep.iata, f.arr.iata, f.depTime, f.arrTime, f.offset, f.durationMin, f.aircraft, source, sourceKey, source === 'SAMPLE']);
  const scheduleId = s.rows[0].id;
  for (const c of cabinsFor(f.aircraft, f.durationMin))
    await client.query(`INSERT INTO flight_cabins(id,schedule_id,cabin,total_seats,price) VALUES($1,$2,$3,$4,$5) ON CONFLICT (schedule_id, cabin) DO NOTHING`,
      [id(), scheduleId, c.cabin, c.seats, c.price]);
  return s.rows[0].inserted as boolean;
}

/** Builds schedules from every cached response of the import plan. Free: reads PostgreSQL only. */
async function buildCatalogueFromLedger() {
  const plan = await importPlan();
  const responses = (await pool.query(`SELECT request_key, response FROM api_requests WHERE status='OK' AND request_key = ANY($1)`,
    [plan.steps.map(s => s.key)])).rows;
  const hubs = plan.hubs || [];
  const wanted = (f: ParsedFlight) => (f.dep.iata === HOME && f.arr.iata === AWAY) || (f.dep.iata === AWAY && f.arr.iata === HOME)
    || hubs.some(h => [`${HOME}-${h}`, `${h}-${AWAY}`, `${AWAY}-${h}`, `${h}-${HOME}`].includes(`${f.dep.iata}-${f.arr.iata}`));
  const seen = new Set<string>(); let created = 0, skipped = 0, rows = 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const r of responses) for (const row of r.response?.data || []) {
      rows++;
      const f = parseFlight(row);
      if (!f || !wanted(f)) { skipped++; continue; }
      const k = `${f.flightNumber}|${f.dep.iata}`;
      if (seen.has(k)) continue;
      seen.add(k);
      if (await upsertSchedule(client, f, 'AVIATIONSTACK', r.request_key)) created++;
    }
    if (seen.size) {
      // Synthetic sample schedules give way to real ones, unless someone booked them.
      await client.query(`DELETE FROM flight_schedules s WHERE s.source='SAMPLE'
        AND NOT EXISTS (SELECT 1 FROM flight_booking_legs l WHERE l.schedule_id=s.id)`);
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  await assignAirlinesToSellers();
  return { responses: responses.length, rows, schedules: seen.size, created, skipped, hubs };
}

// Synthetic TLV <-> BKK flights for when there is neither an API key nor a snapshot. Realistic, but made up.
const SAMPLE_FLIGHTS: [string, string, string, string, string, string, string, number, string][] = [
  ['LY', 'El Al', 'LY81', 'TLV', 'BKK', '21:00', '10:05', 1, 'B789'], ['LY', 'El Al', 'LY82', 'BKK', 'TLV', '12:40', '18:25', 0, 'B789'],
  ['6H', 'Israir', '6H921', 'TLV', 'BKK', '23:30', '12:30', 1, 'A332'], ['6H', 'Israir', '6H922', 'BKK', 'TLV', '14:30', '20:45', 0, 'A332'],
  ['FZ', 'flydubai', 'FZ1082', 'TLV', 'DXB', '15:05', '19:40', 0, 'B38M'], ['EK', 'Emirates', 'EK384', 'DXB', 'BKK', '03:05', '12:30', 0, 'B77W'],
  ['EK', 'Emirates', 'EK385', 'BKK', 'DXB', '14:25', '17:50', 0, 'B77W'], ['FZ', 'flydubai', 'FZ1081', 'DXB', 'TLV', '21:25', '23:55', 0, 'B38M'],
  ['EY', 'Etihad Airways', 'EY597', 'TLV', 'AUH', '17:20', '21:55', 0, 'A321'], ['EY', 'Etihad Airways', 'EY406', 'AUH', 'BKK', '02:40', '12:10', 0, 'B789'],
  ['EY', 'Etihad Airways', 'EY407', 'BKK', 'AUH', '14:05', '17:40', 0, 'B789'], ['EY', 'Etihad Airways', 'EY598', 'AUH', 'TLV', '19:50', '22:25', 0, 'A321'],
];
async function insertSampleSchedules() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const t = todayStr();
    for (const [al, alName, fn, dep, arr, dt, at, off, ac] of SAMPLE_FLIGHTS) {
      const depTz = AIRPORT_INFO[dep][2], arrTz = AIRPORT_INFO[arr][2];
      const durationMin = Math.round((localToUtc(addDays(t, off), at, arrTz) - localToUtc(t, dt, depTz)) / 60000);
      await upsertSchedule(client, { airline: { iata: al, name: alName }, flightNumber: fn, dep: { iata: dep, name: `${AIRPORT_INFO[dep][0]} (${dep})`, tz: depTz },
        arr: { iata: arr, name: `${AIRPORT_INFO[arr][0]} (${arr})`, tz: arrTz }, depTime: dt, arrTime: at, offset: off, durationMin, aircraft: ac }, 'SAMPLE', null);
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  return SAMPLE_FLIGHTS.length;
}

/** Airlines nobody owns go to the sample sellers, alternating (like sample hotels). */
async function assignAirlinesToSellers() {
  const sellers = (await pool.query(`SELECT id FROM users WHERE email IN ('seller@example.com','seller2@example.com') ORDER BY email`)).rows;
  if (!sellers.length) return 0;
  const free = (await pool.query(`SELECT iata FROM airlines WHERE seller_id IS NULL ORDER BY iata`)).rows;
  for (const [i, a] of free.entries()) await pool.query(`UPDATE airlines SET seller_id=$1 WHERE iata=$2`, [sellers[i % sellers.length].id, a.iata]);
  return free.length;
}

// ---- Catalogue in memory ----------------------------------------------------------------------------------------
// A cabin row joined with its flight, airline and airports: the unit of inventory (like a room type).
type Cabin = { id: string; schedule_id: string; cabin: 'ECONOMY'|'BUSINESS'; total_seats: number; price: number; airline_iata: string; airline_name: string;
  flight_number: string; dep_iata: string; arr_iata: string; dep_time: string; arr_time: string; arr_day_offset: number; duration_min: number;
  days_of_week: number[]; aircraft: string | null; source: string; country: string; dep_city: string; arr_city: string; dep_tz: string; arr_tz: string;
  dep_name: string; arr_name: string; seller_id: string | null };
const CABIN_SQL = `SELECT c.id, c.schedule_id, c.cabin, c.total_seats, c.price::float AS price, s.airline_iata, al.name AS airline_name, s.flight_number,
  s.dep_iata, s.arr_iata, to_char(s.dep_time,'HH24:MI') AS dep_time, to_char(s.arr_time,'HH24:MI') AS arr_time, s.arr_day_offset, s.duration_min,
  s.days_of_week, s.aircraft, s.source, da.country, da.city AS dep_city, aa.city AS arr_city, da.timezone AS dep_tz, aa.timezone AS arr_tz,
  da.name AS dep_name, aa.name AS arr_name, al.seller_id
  FROM flight_cabins c JOIN flight_schedules s ON s.id=c.schedule_id JOIN airlines al ON al.iata=s.airline_iata
  JOIN airports da ON da.iata=s.dep_iata JOIN airports aa ON aa.iata=s.arr_iata`;
async function loadCabins(where = '', values: any[] = []): Promise<Cabin[]> {
  return (await pool.query(`${CABIN_SQL} ${where}`, values)).rows;
}
const runsOn = (c: Cabin, date: string) => c.days_of_week.includes(weekdayOf(date));
function legTimes(c: Cabin, date: string) {
  const depAt = localToUtc(date, c.dep_time, c.dep_tz);
  return { depAt, arrAt: localToUtc(addDays(date, c.arr_day_offset), c.arr_time, c.arr_tz) };
}
/** "" when the departure date can be booked, else why not. */
function dateProblem(date: string) {
  if (!DATE_RE.test(String(date)) || isNaN(Date.parse(date))) return 'Dates must be YYYY-MM-DD';
  if (date < todayStr()) return 'The departure date is in the past';
  if (date >= addDays(todayStr(), AVAILABILITY_DAYS)) return `Flights are open up to ${AVAILABILITY_DAYS} days ahead`;
  return '';
}

// ---- Pricing -----------------------------------------------------------------------------------------------------
// The price of one seat on one departure, in layers (whole baht, never below 0), per passenger:
//   1. base: flight_cabins.price
//   2. HOLIDAY rule, if any: fixed price or base +/- %; skips steps 3-4
//   3. SEASON rule: fixed price or base +/- %
//   4. weekday %: the airline's value for that day of the week, else the departure country's, else 0
//   5. DEMAND: seats already sold on this departure (>= 50% +15%, >= 80% +40%)
//   6. ADVANCE: days before departure (< 7 days +30%, >= 60 days -10%)
//   7. DISCOUNT: minus the single largest active discount %
// Inside one layer a cabin rule beats a flight rule beats an airline rule beats a country rule, then the newest wins.
// Same engine as the hotel lab (room > hotel > country); layers 5 and 6 are flight specific.
type PriceRule = { id: string; country: string | null; airline_iata: string | null; schedule_id: string | null; cabin_id: string | null;
  kind: 'SEASON'|'HOLIDAY'|'DISCOUNT'; name: string; start_date: string; end_date: string; adjust_type: 'PERCENT'|'FIXED'; adjust_value: number; created_at: Date };
type Pricing = { airlinePct: Map<string, (number|null)[]>; countryPct: Map<string, number[]>; rules: PriceRule[];
  byAirline: Map<string, PriceRule[]>; byCountry: Map<string, PriceRule[]> };
const WEEKDAY_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const toPct = (a: any) => Array.from({ length: 7 }, (_, i) => a?.[i] == null ? null : Number(a[i]));
const RULE_COLUMNS = `id, country, airline_iata, schedule_id, cabin_id, kind, name, to_char(start_date,'YYYY-MM-DD') AS start_date,
  to_char(end_date,'YYYY-MM-DD') AS end_date, adjust_type, adjust_value::float AS adjust_value, created_at`;
const DEMAND_TIERS: [number, number, string][] = [[0.8, 40, 'Last seats'], [0.5, 15, 'Filling up']];
const ADVANCE_TIERS: [(days: number) => boolean, number, string][] = [[d => d < 7, 30, 'Last minute'], [d => d >= 60, -10, 'Early bird']];

async function loadPricing(cabins: Cabin[]): Promise<Pricing> {
  const airlines = [...new Set(cabins.map(c => c.airline_iata))], countries = [...new Set(cabins.map(c => c.country))];
  const [al, cp, rules] = await Promise.all([
    pool.query(`SELECT iata, weekday_pct FROM airlines WHERE iata = ANY($1)`, [airlines]),
    pool.query(`SELECT country, weekday_pct FROM country_pricing WHERE country = ANY($1)`, [countries]),
    pool.query(`SELECT ${RULE_COLUMNS} FROM flight_price_rules WHERE airline_iata = ANY($1) OR country = ANY($2)`, [airlines, countries])]);
  const group = (key: (r: PriceRule) => string | null) => rules.rows.reduce((m: Map<string, PriceRule[]>, r: PriceRule) => {
    const k = key(r); if (k) m.set(k, [...(m.get(k) || []), r]); return m; }, new Map());
  return { airlinePct: new Map(al.rows.map((a: any) => [a.iata, toPct(a.weekday_pct)])),
    countryPct: new Map(cp.rows.map((c: any) => [c.country, toPct(c.weekday_pct).map(x => x ?? 0)])), rules: rules.rows,
    byAirline: group(r => r.airline_iata), byCountry: group(r => r.country) };
}
const ruleSource = (r: PriceRule) => r.cabin_id ? 'cabin' : r.schedule_id ? 'flight' : r.airline_iata ? 'airline' : 'country';
const SOURCE_RANK = { cabin: 4, flight: 3, airline: 2, country: 1 } as const;
function pickRule(rules: PriceRule[]) {
  let win: PriceRule | null = null;
  for (const r of rules) if (!win || (SOURCE_RANK[ruleSource(r)] - SOURCE_RANK[ruleSource(win)] || r.created_at.getTime() - win.created_at.getTime()) > 0) win = r;
  return win;
}
const signed = (n: number) => `${n > 0 ? '+' : n < 0 ? '−' : ''}${Math.abs(n)}%`;

/** Price of one seat on a departure. `booked` = seats already sold or held on it (for the demand layer). */
function priceSeat(c: Cabin, date: string, booked: number, P: Pricing, today = todayStr()) {
  const base = Number(c.price);
  const candidates = [...(P.byAirline.get(c.airline_iata) || []), ...(P.byCountry.get(c.country) || [])];
  const active = candidates.filter(r => date >= r.start_date && date <= r.end_date &&
    (r.cabin_id ? r.cabin_id === c.id : r.schedule_id ? r.schedule_id === c.schedule_id : r.airline_iata ? r.airline_iata === c.airline_iata : r.country === c.country));
  const of = (kind: PriceRule['kind']) => active.filter(r => r.kind === kind);
  const apply = (r: PriceRule) => r.adjust_type === 'FIXED' ? r.adjust_value : base * (1 + r.adjust_value / 100);
  const describe = (r: PriceRule) => r.adjust_type === 'FIXED' ? `฿${r.adjust_value.toLocaleString('en')}` : signed(r.adjust_value);
  const parts: { layer: string; source: string; name: string; change: string }[] = [];
  let price = base;
  const holiday = pickRule(of('HOLIDAY'));
  if (holiday) { price = apply(holiday); parts.push({ layer: 'HOLIDAY', source: ruleSource(holiday), name: holiday.name, change: describe(holiday) }); }
  else {
    const season = pickRule(of('SEASON'));
    if (season) { price = apply(season); parts.push({ layer: 'SEASON', source: ruleSource(season), name: season.name, change: describe(season) }); }
    const dow = weekdayOf(date);
    const own = P.airlinePct.get(c.airline_iata)?.[dow] ?? null, pct = own ?? P.countryPct.get(c.country)?.[dow] ?? 0;
    if (pct) { price *= 1 + pct / 100; parts.push({ layer: 'WEEKDAY', source: own != null ? 'airline' : 'country', name: WEEKDAY_SHORT[dow], change: signed(pct) }); }
  }
  const load = c.total_seats ? booked / c.total_seats : 0;
  const demand = DEMAND_TIERS.find(([min]) => load >= min);
  if (demand) { price *= 1 + demand[1] / 100; parts.push({ layer: 'DEMAND', source: 'seats', name: demand[2], change: signed(demand[1]) }); }
  const ahead = daysBetween(today, date), adv = ADVANCE_TIERS.find(([when]) => when(ahead));
  if (adv) { price *= 1 + adv[1] / 100; parts.push({ layer: 'ADVANCE', source: 'date', name: adv[2], change: signed(adv[1]) }); }
  const discount = of('DISCOUNT').sort((a, b) => b.adjust_value - a.adjust_value)[0];
  if (discount) { price *= 1 - discount.adjust_value / 100; parts.push({ layer: 'DISCOUNT', source: ruleSource(discount), name: discount.name, change: signed(-discount.adjust_value) }); }
  const label = parts.map(p => `${p.name} ${p.change}`).join(' · ');
  return { date, price: Math.max(0, Math.round(price)), parts, label, rule: parts[0] ? { kind: parts[0].layer, name: label } : null };
}

/** Validates a SEASON / HOLIDAY / DISCOUNT rule body; returns the normalised fields or an error message. */
function parseRule(b: any, allowDiscount: boolean) {
  const kinds = allowDiscount ? ['SEASON', 'HOLIDAY', 'DISCOUNT'] : ['SEASON', 'HOLIDAY'];
  if (!kinds.includes(b.kind)) return { error: `kind must be ${kinds.join(', ')}` };
  const name = String(b.name || '').trim().slice(0, 80) || ({ SEASON: 'Season', HOLIDAY: 'Holiday', DISCOUNT: 'Discount' } as any)[b.kind];
  const start = b.startDate, end = b.endDate || b.startDate;
  if (!DATE_RE.test(String(start)) || !DATE_RE.test(String(end))) return { error: 'Pick a start and an end date' };
  if (end < start) return { error: 'The end date cannot be before the start date' };
  const type = b.kind === 'DISCOUNT' ? 'PERCENT' : b.adjustType, value = Number(b.adjustValue);
  if (b.kind === 'DISCOUNT' ? !(value >= 1 && value <= 90) : type === 'PERCENT' ? !(value >= -90 && value <= 500) : type === 'FIXED' ? !(value > 0 && value <= 10_000_000) : true)
    return { error: b.kind === 'DISCOUNT' ? 'A discount must be between 1 and 90%' : 'Use a percentage between -90 and 500, or a fixed price above 0' };
  return { kind: b.kind as string, name, start, end, type: type as string, value };
}
/** Validates 7 weekday percentages (Sun..Sat); null = inherit (airline level only). */
function parseWeekdays(pct: any, allowNull: boolean) {
  if (!Array.isArray(pct) || pct.length !== 7) return { error: 'Send 7 values, Sunday to Saturday' };
  const out = pct.map((v: any) => v === null || v === '' ? null : Number(v));
  if (out.some((v: any) => v === null ? !allowNull : !(v >= -90 && v <= 500))) return { error: 'Each day must be between -90% and +500%' };
  return { pct: out as (number|null)[] };
}

// ---- Redis seats: booked counts only ---------------------------------------------------------------------------
// Same model as the hotel lab's rooms: per cabin and month one small hash, fs:{cabinId}:YYYY-MM, with a field only for
// departure days that have bookings or payment holds (field = day of month, value = seats booked). No field = nothing
// booked. Capacity (flight_cabins.total_seats) stays in PostgreSQL and is passed to the scripts. Month keys expire two
// days after their month ends. fs:loaded is written by the rebuild; without it the scripts refuse to run.
const FS_LOADED = 'fs:loaded';
const monthKey = (cabinId: string, month: string) => `fs:{${cabinId}}:${month}`;
const dayField = (date: string) => String(Number(date.slice(8)));
const monthExpireAt = (month: string) => { const [y, m] = month.split('-').map(Number); return String(Math.floor(Date.UTC(y, m, 1) / 1000) + 2 * 86400); };
const NOT_LOADED = { statusCode: 409, message: 'Seat availability is not loaded in Redis yet (rebuild running); try again in a moment' };
type SeatRef = { cabinId: string; date: string };

/** Seats booked per departure (in input order), or null when Redis is not loaded. */
async function bookedCounts(refs: SeatRef[]) {
  const pipe = redis.pipeline().exists(FS_LOADED);
  for (const r of refs) pipe.hget(monthKey(r.cabinId, r.date.slice(0, 7)), dayField(r.date));
  const res = await execOrThrow(pipe);
  if (!Number(res[0][1])) return null;
  return refs.map((_, i) => Number(res[1 + i][1] || 0));
}

// Both scripts: KEYS = [fs:loaded, month keys...]; ARGV = seats per leg (passengers), number of legs, then per leg
// (index into KEYS, day field, total seats), then per month key its EXPIREAT. Returns {code, value}:
// {0, fewest seats left on any leg}, {-1, 1-based leg that is full}, {-2, 0} = Redis not loaded.
function legsScript(legs: (SeatRef & { total: number })[], qty: number) {
  const months = [...new Set(legs.map(l => monthKey(l.cabinId, l.date.slice(0, 7))))];
  const keys = [FS_LOADED, ...months];
  const argv = [String(qty), String(legs.length),
    ...legs.flatMap(l => [String(months.indexOf(monthKey(l.cabinId, l.date.slice(0, 7))) + 2), dayField(l.date), String(l.total)]),
    ...months.map(k => monthExpireAt(k.slice(-7)))];
  return [keys.length, ...keys, ...argv] as const;
}
// All-or-nothing over every leg of the trip: if any departure has fewer free seats than passengers, nothing changes.
const RESERVE_LUA = `
  if redis.call('EXISTS', KEYS[1]) == 0 then return {-2, 0} end
  local qty, n, least = tonumber(ARGV[1]), tonumber(ARGV[2]), nil
  for i = 1, n do
    local b = tonumber(redis.call('HGET', KEYS[tonumber(ARGV[3*i])], ARGV[3*i + 1]) or '0')
    local left = tonumber(ARGV[3*i + 2]) - b - qty
    if left < 0 then return {-1, i} end
    if least == nil or left < least then least = left end
  end
  for i = 1, n do redis.call('HINCRBY', KEYS[tonumber(ARGV[3*i])], ARGV[3*i + 1], qty) end
  for j = 2, #KEYS do redis.call('EXPIREAT', KEYS[j], ARGV[3*n + 1 + j]) end
  return {0, least}
`;
// Gives seats back: -qty per leg, and a day that reaches 0 is removed (keeps the hash sparse).
const RELEASE_LUA = `
  if redis.call('EXISTS', KEYS[1]) == 0 then return {-2, 0} end
  local qty, n, least = tonumber(ARGV[1]), tonumber(ARGV[2]), nil
  for i = 1, n do
    local k, f = KEYS[tonumber(ARGV[3*i])], ARGV[3*i + 1]
    local b = redis.call('HINCRBY', k, f, -qty)
    if b <= 0 then redis.call('HDEL', k, f) b = 0 end
    local left = tonumber(ARGV[3*i + 2]) - b
    if least == nil or left < least then least = left end
  end
  return {0, least}
`;
/** Releases the legs that have not departed yet. Returns the fewest seats now free on them, or null if none. */
async function releaseLegs(legs: (SeatRef & { total: number })[], qty: number) {
  const open = legs.filter(l => l.date >= todayStr());
  if (!open.length) return null;
  const [code, least] = (await redis.eval(RELEASE_LUA, ...legsScript(open, qty))) as number[];
  if (code === -2) throw NOT_LOADED;
  return least;
}
/** Every month key a cabin can have: last month .. the end of the booking window. */
function cabinMonthKeys(cabinId: string) {
  const first = addDays(todayStr(), -31);
  return [...new Set(Array.from({ length: AVAILABILITY_DAYS + 33 }, (_, i) => addDays(first, i).slice(0, 7)))].map(m => monthKey(cabinId, m));
}

/** Runs a pipeline and throws its first command error (ioredis reports those per command instead of rejecting). */
async function execOrThrow(pipe: ReturnType<typeof redis.pipeline>) {
  const res = await pipe.exec();
  const failed = res?.find(([err]) => err);
  if (failed) throw failed[0];
  return res!;
}

// ---- Search --------------------------------------------------------------------------------------------------------
type Leg = { cabin: Cabin; date: string; depAt: number; arrAt: number };
/** Direct and 1-stop itineraries from -> to departing on `date` (local at the origin), in one cabin class. */
function findItineraries(cabins: Cabin[], from: string, to: string, date: string) {
  const out: Leg[][] = [];
  const leg = (c: Cabin, d: string): Leg => ({ cabin: c, date: d, ...legTimes(c, d) });
  const byOrigin = new Map<string, Cabin[]>();
  for (const c of cabins) byOrigin.set(c.dep_iata, [...(byOrigin.get(c.dep_iata) || []), c]);
  for (const first of byOrigin.get(from) || []) {
    if (!runsOn(first, date)) continue;
    const l1 = leg(first, date);
    if (first.arr_iata === to) { out.push([l1]); continue; }
    if (first.arr_iata === from) continue;
    for (const second of byOrigin.get(first.arr_iata) || []) {
      if (second.arr_iata !== to) continue;
      for (const extra of [0, 1]) {
        const d2 = addDays(date, first.arr_day_offset + extra);
        if (!runsOn(second, d2)) continue;
        const l2 = leg(second, d2), gap = (l2.depAt - l1.arrAt) / 60000;
        if (gap >= MIN_CONNECTION_MIN && gap <= MAX_CONNECTION_MIN) out.push([l1, l2]);
      }
    }
  }
  const earliest = Date.now() + BOOKING_CUTOFF_MIN * 60000, last = addDays(todayStr(), AVAILABILITY_DAYS);
  // Of several feeders into the same onward flight, only the one with the shortest layover is worth showing.
  const best = new Map<string, Leg[]>();
  for (const legs of out.filter(l => l[0].depAt >= earliest && l.every(x => x.date < last))) {
    const k = legs.length === 1 ? `direct:${legs[0].cabin.id}` : `${legs[1].cabin.id}@${legs[1].date}`, cur = best.get(k);
    if (!cur || legs[0].depAt > cur[0].depAt) best.set(k, legs);
  }
  return [...best.values()];
}

/** Seats left and a priced quote for each itinerary (booked counts from Redis, one pipeline). */
async function quoteItineraries(itins: Leg[][], passengers: number) {
  const refs = itins.flatMap(legs => legs.map(l => ({ cabinId: l.cabin.id, date: l.date })));
  const booked = refs.length ? await bookedCounts(refs) : [];
  if (!booked) throw NOT_LOADED;
  const P = await loadPricing([...new Map(itins.flat().map(l => [l.cabin.id, l.cabin])).values()]);
  let i = 0;
  return itins.map(legs => {
    const quoted = legs.map(l => { const b = booked[i++]; const q = priceSeat(l.cabin, l.date, b, P);
      return { ...l, booked: b, seatsLeft: Math.max(0, l.cabin.total_seats - b), quote: q }; });
    const perPassenger = quoted.reduce((a, l) => a + l.quote.price, 0);
    const seatsLeft = Math.min(...quoted.map(l => l.seatsLeft));
    return { legs: quoted, perPassenger, total: perPassenger * passengers, seatsLeft, soldOut: seatsLeft < passengers,
      depAt: legs[0].depAt, arrAt: legs[legs.length - 1].arrAt };
  });
}
const iso = (ms: number) => new Date(ms).toISOString();
function itineraryJson(x: Awaited<ReturnType<typeof quoteItineraries>>[number]) {
  const legs = x.legs;
  return {
    id: legs.map(l => `${l.cabin.id}@${l.date}`).join('+'),
    stops: legs.length - 1, via: legs.slice(0, -1).map(l => l.cabin.arr_iata),
    from: legs[0].cabin.dep_iata, to: legs[legs.length - 1].cabin.arr_iata,
    depAt: iso(x.depAt), arrAt: iso(x.arrAt), durationMin: Math.round((x.arrAt - x.depAt) / 60000),
    layovers: legs.slice(1).map((l, i) => ({ airport: l.cabin.dep_iata, city: l.cabin.dep_city, minutes: Math.round((l.depAt - legs[i].arrAt) / 60000) })),
    pricePerPassenger: x.perPassenger, totalPrice: x.total, seatsLeft: x.seatsLeft, soldOut: x.soldOut,
    airlines: [...new Set(legs.map(l => l.cabin.airline_name))],
    source: legs.every(l => l.cabin.source === 'AVIATIONSTACK') ? 'AVIATIONSTACK' : 'SAMPLE',
    legs: legs.map(l => ({
      cabinId: l.cabin.id, scheduleId: l.cabin.schedule_id, cabin: l.cabin.cabin, flightNumber: l.cabin.flight_number,
      airline: l.cabin.airline_name, airlineIata: l.cabin.airline_iata, aircraft: l.cabin.aircraft,
      from: l.cabin.dep_iata, fromCity: l.cabin.dep_city, fromName: l.cabin.dep_name, to: l.cabin.arr_iata, toCity: l.cabin.arr_city, toName: l.cabin.arr_name,
      date: l.date, depLocal: l.cabin.dep_time, arrLocal: l.cabin.arr_time, arrDayOffset: l.cabin.arr_day_offset,
      depAt: iso(l.depAt), arrAt: iso(l.arrAt), durationMin: Math.round((l.arrAt - l.depAt) / 60000),
      seatsLeft: l.seatsLeft, totalSeats: l.cabin.total_seats, price: l.quote.price, label: l.quote.label, parts: l.quote.parts,
    })),
  };
}

// ---- Routes --------------------------------------------------------------------------------------------------------
app.get('/api/health', async () => ({ ok: true, service: 'flight-booking-lab-api' }));

// Learning project only: passwords are stored in plain text and listed on the login page so anyone can try every role.
app.get('/api/auth/demo-accounts', async () => {
  const counts = await pool.query(`SELECT role, count(*)::int AS n FROM users GROUP BY role`);
  const r = await pool.query(`
    SELECT name, email, password, role, "isSample" FROM (
      SELECT name, email, password_hash AS password, role, is_sample AS "isSample",
        row_number() OVER (PARTITION BY role ORDER BY created_at, length(email), email) AS rn
      FROM users) u
    WHERE rn <= 2000 ORDER BY CASE role WHEN 'ADMIN' THEN 0 WHEN 'SELLER' THEN 1 ELSE 2 END, rn`);
  return { counts: Object.fromEntries(counts.rows.map((x:any) => [x.role, x.n])), accounts: r.rows };
});

app.post('/api/auth/login', async (req: any, reply) => {
  const { email, password } = req.body || {};
  const r = await pool.query(`SELECT id,email,name,role,password_hash FROM users WHERE email=$1`, [email]);
  if (!r.rows[0] || r.rows[0].password_hash !== password) return reply.code(401).send({ error: 'Invalid credentials' });
  const u = r.rows[0];
  return { token: app.jwt.sign({ id: u.id, role: u.role }), user: { id:u.id,email:u.email,name:u.name,role:u.role } };
});

// Catalogue totals for the login page. Seat-departures = every cabin's seats x every day it flies in the window,
// minus seats held by active bookings. Computed in PostgreSQL, the source of truth.
app.get('/api/stats', async () => {
  const first = todayStr(), end = addDays(first, AVAILABILITY_DAYS);
  const r = await pool.query(`SELECT
    (SELECT count(*)::int FROM airports) AS airports,
    (SELECT count(*)::int FROM airlines) AS airlines,
    (SELECT count(*)::int FROM flight_schedules) AS flights,
    (SELECT count(*)::int FROM flight_schedules WHERE source='AVIATIONSTACK') AS "realFlights",
    (SELECT COALESCE(sum(c.total_seats * (SELECT count(*) FROM generate_series($1::date, $2::date - 1, '1 day') d
       WHERE extract(dow FROM d)::int = ANY(s.days_of_week))), 0)::bigint FROM flight_cabins c JOIN flight_schedules s ON s.id=c.schedule_id) AS "totalSeats",
    (SELECT COALESCE(sum(b.passengers), 0)::bigint FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
       WHERE b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= $1::date AND l.dep_date < $2::date) AS "bookedSeats"`, [first, end]);
  const x = r.rows[0], total = Number(x.totalSeats), booked = Number(x.bookedSeats);
  return { ...x, totalSeats: total, bookedSeats: booked, availableSeats: Math.max(0, total - booked), windowDays: AVAILABILITY_DAYS,
    firstDay: first, lastDay: addDays(end, -1), market: `${HOME} ⇄ ${AWAY}` };
});

// Airports that have departures (sellers: of their own airlines), for the From / To pickers.
app.get('/api/airports', async () => {
  const r = await pool.query(`SELECT a.iata, a.name, a.city, a.country,
      (SELECT count(*)::int FROM flight_schedules s WHERE s.dep_iata=a.iata) AS departures,
      (SELECT count(*)::int FROM flight_schedules s WHERE s.arr_iata=a.iata) AS arrivals
    FROM airports a ORDER BY CASE a.iata WHEN $1 THEN 0 WHEN $2 THEN 1 ELSE 2 END, a.city`, [HOME, AWAY]);
  return r.rows;
});

// One-way search. Round trips are two searches (outbound, then return) whose chosen itineraries are booked together.
// ?from=TLV&to=BKK&date=YYYY-MM-DD&passengers=1&cabin=ECONOMY&stops=0|1&sort=price|duration|departure&page=&limit=
app.get('/api/flights/search', async (req: any, reply) => {
  await tryAuth(req);
  const { from, to, date } = req.query;
  const passengers = Math.min(MAX_PASSENGERS, Math.max(1, Number(req.query.passengers) || 1));
  const cabin = req.query.cabin === 'BUSINESS' ? 'BUSINESS' : 'ECONOMY';
  const maxStops = req.query.stops === '0' ? 0 : 1;
  const problem = dateProblem(date);
  if (problem) return reply.code(400).send({ error: problem });
  if (!from || !to || from === to) return reply.code(400).send({ error: 'Pick two different airports' });
  const seller = req.userCtx?.role === 'SELLER';
  const cabins = await loadCabins(`WHERE c.cabin=$1 ${seller ? 'AND al.seller_id=$2' : ''}`, seller ? [cabin, req.userCtx.id] : [cabin]);
  const itins = findItineraries(cabins, from, to, date).filter(l => l.length - 1 <= maxStops);
  const quoted = await quoteItineraries(itins, passengers);
  const sort = ['duration', 'departure'].includes(req.query.sort) ? req.query.sort : 'price';
  const key = (x: typeof quoted[number]) => sort === 'duration' ? x.arrAt - x.depAt : sort === 'departure' ? x.depAt : x.total;
  quoted.sort((a, b) => Number(a.soldOut) - Number(b.soldOut) || key(a) - key(b) || a.legs.length - b.legs.length);
  const page = Math.max(1, Number(req.query.page) || 1), limit = Math.min(100, Math.max(1, Number(req.query.limit) || 20));
  return { from, to, date, passengers, cabin, sort, page, limit, total: quoted.length,
    cheapest: quoted.some(x => !x.soldOut) ? Math.min(...quoted.filter(x => !x.soldOut).map(x => x.perPassenger)) : null,
    items: quoted.slice((page - 1) * limit, page * limit).map(itineraryJson) };
});

// Cheapest price per day for a route, for the date strip above the results (N days from `date`).
app.get('/api/flights/calendar', async (req: any, reply) => {
  const { from, to } = req.query;
  const start = DATE_RE.test(String(req.query.date)) && req.query.date >= todayStr() ? String(req.query.date) : todayStr();
  const days = Math.min(14, Math.max(1, Number(req.query.days) || 7));
  const cabin = req.query.cabin === 'BUSINESS' ? 'BUSINESS' : 'ECONOMY';
  const passengers = Math.min(MAX_PASSENGERS, Math.max(1, Number(req.query.passengers) || 1));
  if (!from || !to) return reply.code(400).send({ error: 'from and to are required' });
  const cabins = await loadCabins(`WHERE c.cabin=$1`, [cabin]);
  const out = [];
  for (let i = 0; i < days; i++) {
    const d = addDays(start, i);
    if (dateProblem(d)) break;
    const q = (await quoteItineraries(findItineraries(cabins, from, to, d), passengers)).filter(x => !x.soldOut);
    out.push({ date: d, cheapest: q.length ? Math.min(...q.map(x => x.perPassenger)) : null, options: q.length });
  }
  return out;
});

// One departure: every cabin with seats left and today's price (seller: only their own airlines).
app.get('/api/flights/:scheduleId', async (req: any, reply) => {
  await tryAuth(req);
  const date = req.query.date || todayStr();
  const problem = dateProblem(date);
  if (problem) return reply.code(400).send({ error: problem });
  const cabins = await loadCabins(`WHERE c.schedule_id=$1 ORDER BY c.cabin DESC`, [req.params.scheduleId]);
  if (!cabins.length) return reply.code(404).send({ error: 'Flight not found' });
  if (req.userCtx?.role === 'SELLER' && cabins[0].seller_id !== req.userCtx.id) return reply.code(403).send({ error: 'This flight belongs to another airline' });
  const c0 = cabins[0];
  if (!runsOn(c0, date)) return reply.code(400).send({ error: `${c0.flight_number} does not fly on ${WEEKDAY_SHORT[weekdayOf(date)]}` });
  const q = await quoteItineraries(cabins.map(c => [{ cabin: c, date, ...legTimes(c, date) }]), 1);
  return { scheduleId: c0.schedule_id, flightNumber: c0.flight_number, airline: c0.airline_name, from: c0.dep_iata, to: c0.arr_iata, date,
    source: c0.source, cabins: q.map(x => { const l = x.legs[0];
      return { id: l.cabin.id, cabin: l.cabin.cabin, totalSeats: l.cabin.total_seats, booked: l.booked, seatsLeft: l.seatsLeft, basePrice: l.cabin.price, price: l.quote.price, label: l.quote.label, parts: l.quote.parts }; }) };
});

// ---- Recent searches ---------------------------------------------------------------------------------------------------
// The last 10 searches of each user, newest first, in one Redis list sh:{userId} (about 2 KB per user; expires after 90
// days without a search). A repeated search moves to the top instead of appearing twice: the Lua script removes the
// entry with the same key, pushes the new one and trims, atomically. Each entry keeps the cheapest fare seen (baht).
const HISTORY_SIZE = 10, HISTORY_TTL_SECONDS = 90 * 86400;
const historyKey = (userId: string) => `sh:{${userId}}`;
const HISTORY_PUSH_LUA = `
  for _, item in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
    if cjson.decode(item).key == ARGV[1] then redis.call('LREM', KEYS[1], 0, item) end
  end
  redis.call('LPUSH', KEYS[1], ARGV[2])
  redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[3]) - 1)
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  return redis.call('LLEN', KEYS[1])
`;
function parseSearch(b: any) {
  const iata = (x: any) => /^[A-Z]{3}$/.test(String(x)) ? String(x) : null;
  const e = { from: iata(b?.from), to: iata(b?.to), tripType: b?.tripType === 'ROUND_TRIP' ? 'ROUND_TRIP' : 'ONE_WAY',
    date: String(b?.date || ''), returnDate: b?.tripType === 'ROUND_TRIP' ? String(b?.returnDate || '') : null,
    passengers: Math.min(MAX_PASSENGERS, Math.max(1, Number(b?.passengers) || 1)), cabin: b?.cabin === 'BUSINESS' ? 'BUSINESS' : 'ECONOMY',
    stops: b?.stops === '0' || b?.stops === 0 ? '0' : '1' };
  if (!e.from || !e.to || e.from === e.to) return { error: 'Pick two different airports' };
  if (!DATE_RE.test(e.date) || (e.returnDate !== null && (!DATE_RE.test(e.returnDate) || e.returnDate < e.date))) return { error: 'Invalid dates' };
  return { entry: e, key: [e.from, e.to, e.tripType, e.date, e.returnDate, e.passengers, e.cabin, e.stops].join('|') };
}
app.get('/api/me/search-history', async (req: any) => {
  await auth(req);
  return (await redis.lrange(historyKey(req.userCtx!.id), 0, HISTORY_SIZE - 1)).map(x => JSON.parse(x));
});
app.post('/api/me/search-history', async (req: any, reply) => {
  await auth(req);
  const p = parseSearch(req.body);
  if ('error' in p) return reply.code(400).send({ error: p.error });
  const cheapest = Number(req.body?.cheapest) > 0 ? Math.round(Number(req.body.cheapest)) : null;
  const item = JSON.stringify({ key: p.key, ...p.entry, cheapest, results: Number(req.body?.results) || 0, searchedAt: new Date().toISOString() });
  const size = await redis.eval(HISTORY_PUSH_LUA, 1, historyKey(req.userCtx!.id), p.key, item, String(HISTORY_SIZE), String(HISTORY_TTL_SECONDS));
  return { ok: true, size };
});
app.delete('/api/me/search-history', async (req: any) => {
  await auth(req);
  await redis.del(historyKey(req.userCtx!.id));
  return { ok: true };
});

// ---- Booking --------------------------------------------------------------------------------------------------------
type BookLeg = Leg & { direction: 'OUTBOUND'|'RETURN' };
/** Validates the requested legs against the catalogue; returns them ready to book, or an error message. */
async function resolveLegs(input: any[]): Promise<{ legs: BookLeg[] } | { error: string }> {
  if (!Array.isArray(input) || input.length < 1 || input.length > MAX_LEGS) return { error: `Send 1 to ${MAX_LEGS} flights` };
  const cabins = new Map((await loadCabins(`WHERE c.id = ANY($1)`, [input.map(l => String(l?.cabinId))])).map(c => [c.id, c]));
  const legs: BookLeg[] = [];
  for (const l of input) {
    const c = cabins.get(String(l?.cabinId));
    if (!c) return { error: 'Flight not found' };
    const problem = dateProblem(l.date);
    if (problem) return { error: problem };
    if (!runsOn(c, l.date)) return { error: `${c.flight_number} does not fly on ${l.date}` };
    legs.push({ cabin: c, date: l.date, ...legTimes(c, l.date), direction: l.direction === 'RETURN' ? 'RETURN' : 'OUTBOUND' });
  }
  if (new Set(legs.map(l => `${l.cabin.id}@${l.date}`)).size !== legs.length) return { error: 'The same flight appears twice' };
  if (legs[0].depAt < Date.now() + BOOKING_CUTOFF_MIN * 60000) return { error: `Flights can be booked until ${BOOKING_CUTOFF_MIN / 60} hours before departure` };
  if (legs.some((l, i) => i && l.direction === 'OUTBOUND' && legs[i - 1].direction === 'RETURN')) return { error: 'Outbound flights come before return flights' };
  for (let i = 1; i < legs.length; i++) {
    const prev = legs[i - 1], cur = legs[i], gap = (cur.depAt - prev.arrAt) / 60000;
    if (cur.direction === prev.direction) {
      if (cur.cabin.dep_iata !== prev.cabin.arr_iata) return { error: `${cur.cabin.flight_number} does not leave from where ${prev.cabin.flight_number} lands` };
      if (gap < MIN_CONNECTION_MIN || gap > MAX_CONNECTION_MIN) return { error: `Connection in ${cur.cabin.dep_iata} must be ${MIN_CONNECTION_MIN} min to ${MAX_CONNECTION_MIN / 60} h (it is ${Math.round(gap)} min)` };
    } else if (gap < 0) return { error: 'The return flight leaves before the outbound trip arrives' };
  }
  const out = legs.filter(l => l.direction === 'OUTBOUND'), ret = legs.filter(l => l.direction === 'RETURN');
  if (ret.length && (ret[0].cabin.dep_iata !== out[out.length - 1].cabin.arr_iata || ret[ret.length - 1].cabin.arr_iata !== out[0].cabin.dep_iata))
    return { error: 'The return trip must go back from the destination to the origin' };
  return { legs };
}
function parsePassengers(input: any): { passengers: { firstName: string; lastName: string; passport: string | null }[] } | { error: string } {
  if (!Array.isArray(input) || input.length < 1 || input.length > MAX_PASSENGERS) return { error: `Send 1 to ${MAX_PASSENGERS} passengers` };
  const out = input.map((p: any) => ({ firstName: String(p?.firstName || '').trim().slice(0, 60), lastName: String(p?.lastName || '').trim().slice(0, 60),
    passport: p?.passport ? String(p.passport).trim().slice(0, 20) : null }));
  if (out.some(p => !p.firstName || !p.lastName)) return { error: 'Every passenger needs a first and a last name' };
  return { passengers: out };
}

/** Prices the trip, holds every leg's seats in Redis at once, then persists the booking + outbox events. */
async function createFlightBooking(userId: string, legs: BookLeg[], passengers: { firstName: string; lastName: string; passport: string | null }[],
  status: 'PENDING'|'CONFIRMED' = 'PENDING', windowSeconds = PAYMENT_WINDOW_SECONDS) {
  const pax = passengers.length;
  // Priced before the Redis hold, so a pricing failure never leaves a hold behind. The booking keeps this price.
  const [quote] = await quoteItineraries([legs], pax);
  const seats = legs.map(l => ({ cabinId: l.cabin.id, date: l.date, total: l.cabin.total_seats }));
  const t0 = performance.now();
  const [code, value] = (await redis.eval(RESERVE_LUA, ...legsScript(seats, pax))) as number[];
  const redisMs = performance.now() - t0;
  if (code === -2) throw { ...NOT_LOADED, redisMs };
  if (code === -1) { const l = legs[value - 1]; throw { statusCode: 409, message: `Not enough seats left on ${l.cabin.flight_number} (${l.cabin.cabin.toLowerCase()}) on ${l.date}`, redisMs }; }
  const t1 = performance.now();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Final guard, PostgreSQL decides: lock the cabins (in id order, so two trips sharing flights cannot deadlock) and
    // count active passengers on each departure. If Redis is ever wrong this turns an overbooking into "sold out".
    await client.query(`SELECT 1 FROM flight_cabins WHERE id = ANY($1) ORDER BY id FOR UPDATE`, [seats.map(s => s.cabinId)]);
    for (const [i, s] of seats.entries()) {
      const used = (await client.query(`SELECT COALESCE(sum(b.passengers),0)::int AS n FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
        WHERE l.cabin_id=$1 AND l.dep_date=$2 AND b.status IN ('CONFIRMED','PENDING')`, [s.cabinId, s.date])).rows[0].n;
      if (used + pax > s.total) throw { statusCode: 409, message: `Not enough seats left on ${legs[i].cabin.flight_number} on ${s.date}`, guard: true, redisMs };
    }
    const bookingId = id();
    const tripType = legs.some(l => l.direction === 'RETURN') ? 'ROUND_TRIP' : 'ONE_WAY';
    const breakdown = quote.legs.map((l, i) => ({ seq: i + 1, direction: legs[i].direction, flightNumber: l.cabin.flight_number, from: l.cabin.dep_iata, to: l.cabin.arr_iata,
      date: l.date, cabin: l.cabin.cabin, price: l.quote.price, label: l.quote.label, parts: l.quote.parts }));
    // PENDING = seats held while the customer pays; the expiry worker releases them if they don't.
    const ins = await client.query(
      `INSERT INTO flight_bookings(id,user_id,status,trip_type,passengers,price,expires_at,paid_at,price_breakdown)
       VALUES($1,$2,$3,$4,$5,$6, CASE WHEN $3='PENDING' THEN now() + ($7 * interval '1 second') END, CASE WHEN $3='CONFIRMED' THEN now() END, $8)
       RETURNING expires_at AS "expiresAt"`,
      [bookingId, userId, status, tripType, pax, quote.total, windowSeconds, JSON.stringify(breakdown)]);
    for (const [i, l] of quote.legs.entries())
      await client.query(`INSERT INTO flight_booking_legs(booking_id,seq,direction,cabin_id,schedule_id,dep_date,dep_at,arr_at,price) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [bookingId, i + 1, legs[i].direction, l.cabin.id, l.cabin.schedule_id, l.date, iso(l.depAt), iso(l.arrAt), l.quote.price]);
    for (const p of passengers)
      await client.query(`INSERT INTO passengers(id,booking_id,first_name,last_name,passport) VALUES($1,$2,$3,$4,$5)`, [id(), bookingId, p.firstName, p.lastName, p.passport]);
    const expiresAt = ins.rows[0].expiresAt;
    const legsEvent = breakdown.map(b => ({ flightNumber: b.flightNumber, from: b.from, to: b.to, date: b.date, cabin: b.cabin }));
    await addOutbox(client, 'flight.booking.created', bookingId, { bookingId, userId, status, tripType, passengers: pax, totalPrice: quote.total, legs: legsEvent, seatsLeft: value, expiresAt });
    for (const [i, s] of seats.entries())
      await addOutbox(client, 'flight.seats.changed', s.cabinId, { cabinId: s.cabinId, flightNumber: legs[i].cabin.flight_number, date: s.date, delta: -pax });
    await client.query('COMMIT');
    return { bookingId, status, tripType, passengers: pax, totalPrice: quote.total, priceBreakdown: breakdown, seatsLeft: value, expiresAt,
      paymentWindowSeconds: status === 'PENDING' ? windowSeconds : 0, timings: { redisMs, pgMs: performance.now() - t1 } };
  } catch (e) {
    await client.query('ROLLBACK');
    await releaseLegs(seats, pax);
    throw e;
  } finally { client.release(); }
}

app.post('/api/flight-bookings', async (req: any, reply) => {
  await auth(req, ['CUSTOMER']);
  const legs = await resolveLegs(req.body?.legs);
  if ('error' in legs) return reply.code(400).send({ error: legs.error });
  const pax = parsePassengers(req.body?.passengers);
  if ('error' in pax) return reply.code(400).send({ error: pax.error });
  try { return await createFlightBooking(req.userCtx!.id, legs.legs, pax.passengers); }
  catch (e: any) { if (e?.statusCode === 409) return reply.code(409).send({ error: e.message }); throw e; }
});

const BOOKING_SQL = `
  SELECT b.id, b.status, b.trip_type AS "tripType", b.passengers, b.price, b.created_at AS "createdAt", b.expires_at AS "expiresAt", b.paid_at AS "paidAt",
    b.price_breakdown AS "priceBreakdown", u.name AS "customerName", u.email AS "customerEmail",
    (SELECT json_agg(json_build_object('seq', l.seq, 'direction', l.direction, 'cabinId', l.cabin_id, 'scheduleId', l.schedule_id, 'cabin', c.cabin,
        'flightNumber', s.flight_number, 'airline', al.name, 'airlineIata', s.airline_iata, 'from', s.dep_iata, 'to', s.arr_iata, 'fromCity', da.city, 'toCity', aa.city,
        'date', to_char(l.dep_date,'YYYY-MM-DD'), 'depAt', l.dep_at, 'arrAt', l.arr_at, 'depLocal', to_char(s.dep_time,'HH24:MI'), 'arrLocal', to_char(s.arr_time,'HH24:MI'),
        'arrDayOffset', s.arr_day_offset, 'price', l.price) ORDER BY l.seq)
     FROM flight_booking_legs l JOIN flight_cabins c ON c.id=l.cabin_id JOIN flight_schedules s ON s.id=l.schedule_id JOIN airlines al ON al.iata=s.airline_iata
     JOIN airports da ON da.iata=s.dep_iata JOIN airports aa ON aa.iata=s.arr_iata WHERE l.booking_id=b.id) AS legs,
    (SELECT json_agg(json_build_object('firstName', p.first_name, 'lastName', p.last_name, 'passport', p.passport) ORDER BY p.last_name, p.first_name)
     FROM passengers p WHERE p.booking_id=b.id) AS "passengerList"
  FROM flight_bookings b JOIN users u ON u.id=b.user_id`;

app.get('/api/flight-bookings/me', async (req: any) => {
  await auth(req, ['CUSTOMER']);
  const rows = (await pool.query(`${BOOKING_SQL} WHERE b.user_id=$1 ORDER BY (SELECT min(dep_at) FROM flight_booking_legs WHERE booking_id=b.id) DESC, b.created_at DESC`,
    [req.userCtx!.id])).rows;
  const active = (b: any) => b.status === 'CONFIRMED' || b.status === 'PENDING';
  const span = (b: any) => [b.legs[0].depAt, b.legs[b.legs.length - 1].arrAt];
  return rows.map((b: any) => ({ ...b,
    // Other active trips in the air at the same time as this one (a "double booking").
    overlaps: !active(b) ? [] : rows.filter((o: any) => o.id !== b.id && active(o) && span(o)[0] < span(b)[1] && span(b)[0] < span(o)[1])
      .map((o: any) => ({ id: o.id, route: `${o.legs[0].from} → ${o.legs[o.legs.length - 1].to}`, depAt: o.legs[0].depAt })) }));
});

type LegRow = { cabin_id: string; dep_date: string; total_seats: number; flight_number: string };
const legRows = async (client: any, bookingId: string): Promise<LegRow[]> => (await client.query(
  `SELECT l.cabin_id, to_char(l.dep_date,'YYYY-MM-DD') AS dep_date, c.total_seats, s.flight_number
   FROM flight_booking_legs l JOIN flight_cabins c ON c.id=l.cabin_id JOIN flight_schedules s ON s.id=l.schedule_id WHERE l.booking_id=$1 ORDER BY l.seq`, [bookingId])).rows;
const seatRefs = (rows: LegRow[]) => rows.map(r => ({ cabinId: r.cabin_id, date: r.dep_date, total: r.total_seats }));

/** PENDING -> CONFIRMED inside the payment window. Throws {statusCode:404|409}. No real payment is processed. */
async function payBooking(bookingId: string, userId: string) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const upd = await client.query(
      `UPDATE flight_bookings SET status='CONFIRMED', paid_at=now()
       WHERE id=$1 AND user_id=$2 AND status='PENDING' AND expires_at > now() RETURNING *`, [bookingId, userId]);
    const row = upd.rows[0];
    if (!row) {
      await client.query('ROLLBACK');
      const b = await pool.query(`SELECT status FROM flight_bookings WHERE id=$1 AND user_id=$2`, [bookingId, userId]);
      if (!b.rows[0]) throw { statusCode: 404, message: 'Booking not found' };
      if (b.rows[0].status === 'PENDING') throw { statusCode: 409, message: 'Payment window has passed; the seats were released' };
      throw { statusCode: 409, message: `Booking is ${b.rows[0].status.replace('_',' ').toLowerCase()}` };
    }
    await addOutbox(client, 'flight.booking.confirmed', row.id, { bookingId: row.id, userId: row.user_id, paidAt: row.paid_at, totalPrice: Number(row.price), passengers: row.passengers });
    await client.query('COMMIT');
    return { bookingId: row.id, status: 'CONFIRMED' as const, paidAt: row.paid_at };
  } catch (e) { await client.query('ROLLBACK').catch(() => {}); throw e; } finally { client.release(); }
}

/** Marks a PENDING/CONFIRMED booking CANCELLED and releases its seats. `force` skips the owner and departed checks (admin). */
async function cancelBooking(bookingId: string, opts: { userId?: string; force?: boolean }) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const b = await client.query(`SELECT b.*, (SELECT min(dep_at) FROM flight_booking_legs WHERE booking_id=b.id) AS first_dep
      FROM flight_bookings b WHERE b.id=$1 ${opts.userId ? 'AND b.user_id=$2' : ''} FOR UPDATE`, opts.userId ? [bookingId, opts.userId] : [bookingId]);
    const row = b.rows[0];
    if (!row) throw { statusCode: 404, message: 'Booking not found' };
    if (row.status !== 'CONFIRMED' && row.status !== 'PENDING') throw { statusCode: 409, message: `Booking is already ${row.status.replace('_',' ').toLowerCase()}` };
    if (!opts.force && new Date(row.first_dep).getTime() <= Date.now()) throw { statusCode: 409, message: 'The trip has already started' };
    await client.query(`UPDATE flight_bookings SET status='CANCELLED' WHERE id=$1`, [row.id]);
    const legs = await legRows(client, row.id);
    const seatsLeft = await releaseLegs(seatRefs(legs), row.passengers);
    await addOutbox(client, 'flight.booking.cancelled', row.id, { bookingId: row.id, userId: row.user_id, passengers: row.passengers, forced: !!opts.force,
      legs: legs.map(l => ({ flightNumber: l.flight_number, date: l.dep_date })) });
    for (const l of legs) await addOutbox(client, 'flight.seats.changed', l.cabin_id, { cabinId: l.cabin_id, flightNumber: l.flight_number, date: l.dep_date, delta: row.passengers });
    await client.query('COMMIT');
    return { bookingId: row.id, status: 'CANCELLED' as const, seatsLeft };
  } catch (e) { await client.query('ROLLBACK').catch(() => {}); throw e; } finally { client.release(); }
}

app.post('/api/flight-bookings/:id/pay', async (req: any, reply) => {
  await auth(req, ['CUSTOMER']);
  try { return await payBooking(req.params.id, req.userCtx!.id); }
  catch (e: any) { if (e?.statusCode) return reply.code(e.statusCode).send({ error: e.message }); throw e; }
});

app.post('/api/flight-bookings/:id/cancel', async (req: any, reply) => {
  await auth(req, ['CUSTOMER']);
  try { return await cancelBooking(req.params.id, { userId: req.userCtx!.id }); }
  catch (e: any) { if (e?.statusCode) return reply.code(e.statusCode).send({ error: e.message }); throw e; }
});

// ---- Seller (airline): only their own airlines --------------------------------------------------------------------
app.get('/api/seller/flights', async (req: any) => {
  await auth(req, ['SELLER']);
  const r = await pool.query(`
    SELECT s.id, s.flight_number AS "flightNumber", s.airline_iata AS "airlineIata", al.name AS airline, s.dep_iata AS "from", s.arr_iata AS "to",
      da.city AS "fromCity", aa.city AS "toCity", to_char(s.dep_time,'HH24:MI') AS "depLocal", to_char(s.arr_time,'HH24:MI') AS "arrLocal",
      s.arr_day_offset AS "arrDayOffset", s.duration_min AS "durationMin", s.aircraft, s.source, s.days_of_week AS "daysOfWeek",
      (SELECT json_agg(json_build_object('id', c.id, 'cabin', c.cabin, 'totalSeats', c.total_seats, 'price', c.price::float) ORDER BY c.cabin DESC)
        FROM flight_cabins c WHERE c.schedule_id=s.id) AS cabins,
      (SELECT count(DISTINCT l.booking_id)::int FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
        WHERE l.schedule_id=s.id AND b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= CURRENT_DATE) AS "upcomingBookings"
    FROM flight_schedules s JOIN airlines al ON al.iata=s.airline_iata JOIN airports da ON da.iata=s.dep_iata JOIN airports aa ON aa.iata=s.arr_iata
    WHERE al.seller_id=$1 ORDER BY al.name, s.dep_iata, s.dep_time`, [req.userCtx!.id]);
  return r.rows;
});

// Stat tiles + per-day and per-flight series for the next N days (default 7, max 31). Source: PostgreSQL only.
app.get('/api/seller/dashboard', async (req: any) => {
  await auth(req, ['SELLER']);
  const days = Math.min(31, Math.max(1, Number(req.query.days || 7)));
  const sid = req.userCtx!.id;
  const perDay = await pool.query(`
    WITH d AS (SELECT generate_series(CURRENT_DATE, CURRENT_DATE + ($2::int - 1), '1 day')::date AS day),
      mine AS (SELECT s.id, s.days_of_week FROM flight_schedules s JOIN airlines al ON al.iata=s.airline_iata WHERE al.seller_id=$1)
    SELECT to_char(d.day,'YYYY-MM-DD') AS day,
      COALESCE((SELECT sum(c.total_seats) FROM mine m JOIN flight_cabins c ON c.schedule_id=m.id WHERE extract(dow FROM d.day)::int = ANY(m.days_of_week)),0)::int AS inventory,
      COALESCE((SELECT sum(b.passengers) FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id JOIN mine m ON m.id=l.schedule_id
                WHERE l.dep_date=d.day AND b.status IN ('CONFIRMED','PENDING')),0)::int AS booked,
      COALESCE((SELECT sum(b.passengers) FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id JOIN mine m ON m.id=l.schedule_id
                WHERE l.dep_date=d.day AND b.status='PENDING'),0)::int AS locked,
      COALESCE((SELECT count(*) FROM mine m WHERE extract(dow FROM d.day)::int = ANY(m.days_of_week)),0)::int AS departures
    FROM d ORDER BY d.day`, [sid, days]);
  const perFlight = await pool.query(`
    SELECT s.id, s.flight_number AS name, s.dep_iata || ' → ' || s.arr_iata AS route,
      (SELECT sum(c.total_seats) FROM flight_cabins c WHERE c.schedule_id=s.id)::int AS seats,
      (SELECT count(*) FROM generate_series(CURRENT_DATE, CURRENT_DATE + ($2::int - 1), '1 day') d WHERE extract(dow FROM d)::int = ANY(s.days_of_week))::int AS departures,
      COALESCE((SELECT sum(b.passengers) FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
                WHERE l.schedule_id=s.id AND b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= CURRENT_DATE AND l.dep_date < CURRENT_DATE + $2::int),0)::int AS "bookedSeats",
      COALESCE((SELECT count(DISTINCT b.id) FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
                WHERE l.schedule_id=s.id AND b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= CURRENT_DATE AND l.dep_date < CURRENT_DATE + $2::int),0)::int AS bookings,
      COALESCE((SELECT sum(l.price * b.passengers) FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
                WHERE l.schedule_id=s.id AND b.status='CONFIRMED' AND l.dep_date >= CURRENT_DATE AND l.dep_date < CURRENT_DATE + $2::int),0)::numeric AS revenue
    FROM flight_schedules s JOIN airlines al ON al.iata=s.airline_iata WHERE al.seller_id=$1 ORDER BY s.flight_number`, [sid, days]);
  const flights = perFlight.rows.map((f: any) => {
    const cap = f.seats * f.departures;
    return { ...f, revenue: Number(f.revenue), capacitySeats: cap, freeSeats: cap - f.bookedSeats, loadFactorPct: cap ? Math.round(100 * f.bookedSeats / cap) : 0 };
  });
  const capacity = perDay.rows.reduce((a: number, d: any) => a + d.inventory, 0), booked = perDay.rows.reduce((a: number, d: any) => a + d.booked, 0);
  const airlines = (await pool.query(`SELECT iata, name FROM airlines WHERE seller_id=$1 ORDER BY name`, [sid])).rows;
  return { days, from: perDay.rows[0]?.day, to: perDay.rows[perDay.rows.length - 1]?.day, airlines,
    tiles: { airlines: airlines.length, flights: flights.length, departures: perDay.rows.reduce((a: number, d: any) => a + d.departures, 0),
      capacitySeats: capacity, bookedSeats: booked, freeSeats: capacity - booked, loadFactorPct: capacity ? Math.round(100 * booked / capacity) : 0,
      bookings: flights.reduce((a, f) => a + f.bookings, 0), revenue: flights.reduce((a, f) => a + f.revenue, 0),
      lockedNow: perDay.rows.reduce((a: number, d: any) => a + d.locked, 0) },
    perDay: perDay.rows.map((d: any) => ({ ...d, free: Math.max(0, d.inventory - d.booked) })), flights };
});

app.get('/api/seller/flights/:id/bookings', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  const s = await pool.query(`SELECT s.id FROM flight_schedules s JOIN airlines al ON al.iata=s.airline_iata WHERE s.id=$1 AND al.seller_id=$2`, [req.params.id, req.userCtx!.id]);
  if (!s.rows[0]) return reply.code(404).send({ error: 'Flight not found' });
  return (await pool.query(`${BOOKING_SQL} WHERE EXISTS (SELECT 1 FROM flight_booking_legs l WHERE l.booking_id=b.id AND l.schedule_id=$1)
    ORDER BY b.created_at DESC LIMIT 500`, [req.params.id])).rows;
});

// ---- Seller price management (per airline) ---------------------------------------------------------------------------
async function ownAirline(req: any, iata: string) {
  return (await pool.query(`SELECT iata, name FROM airlines WHERE iata=$1 AND seller_id=$2`, [iata, req.userCtx!.id])).rows[0];
}
// Base fares, weekday % (airline and inherited country values), rules (airline + inherited country) and the resulting
// price of every cabin for the next `days` departure days (with today's seat counts for the demand layer).
app.get('/api/seller/airlines/:iata/prices', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  const airline = await ownAirline(req, req.params.iata);
  if (!airline) return reply.code(404).send({ error: 'Airline not found' });
  const from = DATE_RE.test(String(req.query.from)) ? String(req.query.from) : todayStr();
  const days = Math.min(120, Math.max(1, Number(req.query.days) || 60));
  const cabins = await loadCabins(`WHERE s.airline_iata=$1 ORDER BY s.dep_iata, s.dep_time, c.cabin DESC`, [airline.iata]);
  const P = await loadPricing(cabins);
  const dates = Array.from({ length: days }, (_, i) => addDays(from, i));
  const booked = await bookedCounts(cabins.flatMap(c => dates.map(d => ({ cabinId: c.id, date: d })))) || [];
  const cabinLabel = new Map(cabins.map(c => [c.id, `${c.flight_number} ${c.cabin.toLowerCase()}`]));
  const flightLabel = new Map(cabins.map(c => [c.schedule_id, `${c.flight_number} ${c.dep_iata} → ${c.arr_iata}`]));
  const countries = [...new Set(cabins.map(c => c.country))];
  const order = { HOLIDAY: 0, SEASON: 1, DISCOUNT: 2 };
  return { airline, from, days, countries,
    cabins: cabins.map(c => ({ id: c.id, scheduleId: c.schedule_id, flightNumber: c.flight_number, from: c.dep_iata, to: c.arr_iata, cabin: c.cabin,
      totalSeats: c.total_seats, price: c.price, country: c.country, flies: c.days_of_week })),
    countryPct: Object.fromEntries(countries.map(c => [c, P.countryPct.get(c) ?? Array(7).fill(0)])), airlinePct: P.airlinePct.get(airline.iata) ?? Array(7).fill(null),
    rules: P.rules.filter(r => r.end_date >= todayStr() && (r.airline_iata === airline.iata || countries.includes(r.country || '')))
      .sort((a, b) => order[a.kind] - order[b.kind] || a.start_date.localeCompare(b.start_date))
      .map(r => ({ ...r, source: ruleSource(r), target: r.cabin_id ? cabinLabel.get(r.cabin_id) : r.schedule_id ? flightLabel.get(r.schedule_id) : null })),
    calendar: cabins.map((c, ci) => ({ cabinId: c.id,
      daily: dates.map((d, di) => runsOn(c, d) ? priceSeat(c, d, booked[ci * dates.length + di] || 0, P) : { date: d, price: null, parts: [], label: 'no flight', rule: null }) })) };
});

app.patch('/api/seller/cabins/:id', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  const price = Number(req.body?.price);
  if (!(price > 0 && price <= 10_000_000)) return reply.code(400).send({ error: 'Price must be between 1 and 10,000,000' });
  const r = await pool.query(`UPDATE flight_cabins c SET price=$1 FROM flight_schedules s, airlines al
    WHERE c.id=$2 AND s.id=c.schedule_id AND al.iata=s.airline_iata AND al.seller_id=$3 RETURNING c.id`, [price, req.params.id, req.userCtx!.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Cabin not found' });
  return { ok: true };
});

// Airline weekday %, Sunday..Saturday; null = use the departure country's value for that day.
app.put('/api/seller/airlines/:iata/weekdays', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  const airline = await ownAirline(req, req.params.iata);
  if (!airline) return reply.code(404).send({ error: 'Airline not found' });
  const w = parseWeekdays(req.body?.pct, true);
  if ('error' in w) return reply.code(400).send({ error: w.error });
  await pool.query(`UPDATE airlines SET weekday_pct=$1 WHERE iata=$2`, [w.pct.every(v => v === null) ? null : w.pct, airline.iata]);
  return { ok: true };
});

app.post('/api/seller/airlines/:iata/price-rules', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  const airline = await ownAirline(req, req.params.iata);
  if (!airline) return reply.code(404).send({ error: 'Airline not found' });
  const b = req.body || {}, r = parseRule(b, true);
  if ('error' in r) return reply.code(400).send({ error: r.error });
  let scheduleId: string | null = b.scheduleId || null, cabinId: string | null = b.cabinId || null;
  if (cabinId) {
    const c = (await pool.query(`SELECT c.schedule_id FROM flight_cabins c JOIN flight_schedules s ON s.id=c.schedule_id WHERE c.id=$1 AND s.airline_iata=$2`, [cabinId, airline.iata])).rows[0];
    if (!c) return reply.code(400).send({ error: 'That cabin is not on this airline' });
    scheduleId = c.schedule_id;
  } else if (scheduleId && !(await pool.query(`SELECT 1 FROM flight_schedules WHERE id=$1 AND airline_iata=$2`, [scheduleId, airline.iata])).rows[0])
    return reply.code(400).send({ error: 'That flight is not on this airline' });
  const ins = await pool.query(
    `INSERT INTO flight_price_rules(id,airline_iata,schedule_id,cabin_id,kind,name,start_date,end_date,adjust_type,adjust_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
    [id(), airline.iata, scheduleId, cabinId, r.kind, r.name, r.start, r.end, r.type, r.value]);
  return { ok: true, id: ins.rows[0].id };
});

app.delete('/api/seller/price-rules/:id', async (req: any, reply) => {
  await auth(req, ['SELLER']);
  // Country rules have no airline, so a seller can never delete them.
  const r = await pool.query(`DELETE FROM flight_price_rules pr USING airlines al WHERE pr.id=$1 AND al.iata=pr.airline_iata AND al.seller_id=$2 RETURNING pr.id`,
    [req.params.id, req.userCtx!.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Price rule not found' });
  return { ok: true };
});

// ---- Admin: country pricing defaults (by departure country) ----------------------------------------------------------
app.get('/api/admin/country-pricing', async (req: any) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`
    SELECT c.country, cp.weekday_pct,
      (SELECT count(*)::int FROM flight_schedules s JOIN airports a ON a.iata=s.dep_iata WHERE a.country=c.country) AS flights
    FROM (SELECT DISTINCT country FROM airports UNION SELECT country FROM country_pricing) c
    LEFT JOIN country_pricing cp ON cp.country=c.country ORDER BY flights DESC, c.country`);
  const rules = (await pool.query(`SELECT ${RULE_COLUMNS} FROM flight_price_rules WHERE country IS NOT NULL AND end_date >= $1 ORDER BY start_date`, [todayStr()])).rows;
  return r.rows.map((c: any) => ({ country: c.country, flights: c.flights, weekdayPct: toPct(c.weekday_pct).map(x => x ?? 0),
    rules: rules.filter((x: any) => x.country === c.country) }));
});
app.put('/api/admin/country-pricing/:country/weekdays', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const w = parseWeekdays(req.body?.pct, false);
  if ('error' in w) return reply.code(400).send({ error: w.error });
  await pool.query(`INSERT INTO country_pricing(country, weekday_pct) VALUES($1,$2) ON CONFLICT (country) DO UPDATE SET weekday_pct=EXCLUDED.weekday_pct`,
    [req.params.country, w.pct]);
  return { ok: true };
});
app.post('/api/admin/country-pricing/:country/rules', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const r = parseRule(req.body || {}, false);
  if ('error' in r) return reply.code(400).send({ error: r.error });
  const ins = await pool.query(
    `INSERT INTO flight_price_rules(id,country,kind,name,start_date,end_date,adjust_type,adjust_value) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
    [id(), req.params.country, r.kind, r.name, r.start, r.end, r.type, r.value]);
  return { ok: true, id: ins.rows[0].id };
});
app.delete('/api/admin/price-rules/:id', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`DELETE FROM flight_price_rules WHERE id=$1 AND country IS NOT NULL RETURNING id`, [req.params.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Country rule not found' });
  return { ok: true };
});

// ---- Display currencies ---------------------------------------------------------------------------------------------
// Prices are computed, stored and charged in baht. Other currencies are display only, converted in the browser with
// fixed rates from currency_rates (edited by the admin), so no exchange-rate API is ever called.
const CURRENCY_DEFAULTS: [string, string, string, number][] = [['THB', 'Thai baht', '฿', 1], ['USD', 'US dollar', '$', 33], ['ILS', 'Israeli new shekel', '₪', 8.9]];
app.get('/api/currencies', async () =>
  (await pool.query(`SELECT code, name, symbol, thb_per_unit::float AS "thbPerUnit", updated_at AS "updatedAt" FROM currency_rates
    ORDER BY CASE code WHEN 'THB' THEN 0 ELSE 1 END, code`)).rows);
app.put('/api/admin/currencies/:code', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const rate = Number(req.body?.thbPerUnit);
  if (req.params.code === 'THB') return reply.code(400).send({ error: 'THB is the base currency (always 1)' });
  if (!(rate > 0 && rate <= 100000)) return reply.code(400).send({ error: 'Rate must be a positive number of baht per unit' });
  const r = await pool.query(`UPDATE currency_rates SET thb_per_unit=$2, updated_at=now() WHERE code=$1 RETURNING code`, [req.params.code, rate]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Unknown currency' });
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'CURRENCY_RATE_SET',$3)`,
    [id(), req.userCtx!.id, JSON.stringify({ code: req.params.code, thbPerUnit: rate })]);
  return { ok: true };
});

app.get('/api/admin/users', async (req: any) => {
  await auth(req, ['ADMIN']);
  return (await pool.query(`SELECT id,email,name,role FROM users ORDER BY created_at DESC`)).rows;
});

// Every flight with its cabins, for the admin concurrency demo and simulation pickers.
app.get('/api/admin/flights', async (req: any) => {
  await auth(req, ['ADMIN']);
  return (await pool.query(`SELECT s.id, s.flight_number AS "flightNumber", s.dep_iata AS "from", s.arr_iata AS "to", to_char(s.dep_time,'HH24:MI') AS "depLocal",
      (SELECT json_agg(json_build_object('id', c.id, 'cabin', c.cabin, 'totalSeats', c.total_seats) ORDER BY c.cabin DESC) FROM flight_cabins c WHERE c.schedule_id=s.id) AS cabins
    FROM flight_schedules s ORDER BY CASE WHEN s.dep_iata IN ($1,$2) AND s.arr_iata IN ($1,$2) THEN 0 ELSE 1 END, s.dep_iata, s.dep_time`, [HOME, AWAY])).rows;
});

app.post('/api/admin/impersonate/:userId', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`SELECT id,email,name,role FROM users WHERE id=$1`, [req.params.userId]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'User not found' });
  const u = r.rows[0];
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,target_user_id) VALUES($1,$2,'IMPERSONATION_STARTED',$3)`, [id(), req.userCtx!.id, u.id]);
  return { token: app.jwt.sign({ id: u.id, role: u.role, impersonatedBy: req.userCtx!.id }), user: u };
});

// ---- Admin: flight data from aviationstack ----------------------------------------------------------------------------
app.get('/api/admin/aviationstack', async (req: any) => {
  await auth(req, ['ADMIN']);
  const ledger = (await pool.query(`SELECT request_key AS "key", endpoint, status, source, http_status AS "httpStatus", error, rows,
      created_at AS "createdAt", finished_at AS "finishedAt",
      (SELECT count(*)::int FROM api_calls c WHERE c.request_key=r.request_key) AS calls,
      (SELECT count(*)::int FROM flight_schedules s WHERE s.source_request_key=r.request_key) AS schedules
    FROM api_requests r ORDER BY created_at DESC LIMIT 200`)).rows;
  const calls = (await pool.query(`SELECT id, request_key AS "key", called_at AS "calledAt", http_status AS "httpStatus", ok, duration_ms AS "durationMs"
    FROM api_calls ORDER BY called_at DESC LIMIT 100`)).rows;
  return { ...(await importPlan()), ledger, calls, snapshotDir: SNAPSHOT_DIR };
});

let importing = false;
// Runs the plan: cached steps cost nothing, uncached steps are called once each. Stops at the first budget refusal or
// failed call (a bad key or a refused HTTPS call would fail the same way on every remaining step).
app.post('/api/admin/aviationstack/import', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  if (!req.body?.confirm) return reply.code(400).send({ error: 'Send {confirm:true} after reviewing the plan' });
  if (importing) return reply.code(409).send({ error: 'An import is already running' });
  importing = true;
  const results: any[] = [];
  try {
    const run = async (s: PlanStep) => {
      try { const r = await aviationstack(s.endpoint, s.params); results.push({ step: s.step, key: s.key, ok: true, called: r.called, rows: r.response?.data?.length ?? 0 }); return true; }
      catch (e: any) { results.push({ step: s.step, key: s.key, ok: false, called: e?.statusCode === 502, error: e?.message || String(e) }); return ![429, 502].includes(e?.statusCode); }
    };
    let go = true;
    for (const s of BASE_STEPS) if (go) go = await run(s);
    const plan = await importPlan();
    if (go && plan.hubs) for (const s of hubSteps(plan.hubs)) if (go) go = await run(s);
    const built = await buildCatalogueFromLedger();
    const called = results.filter(r => r.called).length;
    await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'AVIATIONSTACK_IMPORT',$3)`,
      [id(), req.userCtx!.id, JSON.stringify({ called, results: results.length, schedules: built.schedules })]);
    await addOutbox(pool, 'flight.data.imported', 'aviationstack', { called, fromCache: results.filter(r => r.ok && !r.called).length, ...built });
    return { ok: true, results, built, budget: await budgetStatus(),
      message: `Import done: ${called} request(s) made, ${results.filter(r => r.ok && !r.called).length} from cache, ${results.filter(r => !r.ok).length} failed. `
        + `${built.schedules} real flights (${built.created} new) via ${built.hubs.join(', ') || 'no hubs'}.` };
  } finally { importing = false; }
});

app.post('/api/admin/aviationstack/retry', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const key = String(req.body?.key || '');
  const row = (await pool.query(`SELECT endpoint, params, status FROM api_requests WHERE request_key=$1`, [key])).rows[0];
  if (!row) return reply.code(404).send({ error: 'Unknown request' });
  if (row.status !== 'FAILED') return reply.code(409).send({ error: `Only failed requests can be retried (this one is ${row.status})` });
  try {
    const r = await aviationstack(row.endpoint, row.params, { retryFailed: true });
    const built = await buildCatalogueFromLedger();
    return { ok: true, message: `Retried ${key}: ${r.response?.data?.length ?? 0} rows. ${built.schedules} real flights in the catalogue.`, budget: await budgetStatus() };
  } catch (e: any) { if (e?.statusCode) return reply.code(e.statusCode).send({ error: e.message }); throw e; }
});

// ---- Admin: sample data ------------------------------------------------------------------------------------------------
// Example rules so layered prices show right away: an autumn discount on the first sample airline, New Year's Eve at a
// fixed economy price on TLV departures of the second. Weekday % and seasons come from the country defaults.
async function ensureSamplePriceRules() {
  if ((await pool.query(`SELECT 1 FROM flight_price_rules WHERE airline_iata IS NOT NULL LIMIT 1`)).rows.length) return 0;
  const al = (await pool.query(`SELECT iata FROM airlines WHERE seller_id IS NOT NULL ORDER BY iata LIMIT 2`)).rows;
  const t = todayStr(), y = Number(t.slice(0, 4)), sy = t > `${y}-01-15` ? y : y - 1;
  if (al[0]) await pool.query(`INSERT INTO flight_price_rules(id,airline_iata,kind,name,start_date,end_date,adjust_type,adjust_value)
    VALUES($1,$2,'DISCOUNT','Autumn deal',$3,$4,'PERCENT',10)`, [id(), al[0].iata, `${sy}-10-01`, `${sy}-11-30`]);
  if (al[1]) await pool.query(`INSERT INTO flight_price_rules(id,airline_iata,schedule_id,cabin_id,kind,name,start_date,end_date,adjust_type,adjust_value)
    SELECT gen_random_uuid(), s.airline_iata, s.id, c.id, 'HOLIDAY', 'New Year''s Eve', $2, $2, 'FIXED', round(c.price * 1.8 / 100) * 100
    FROM flight_cabins c JOIN flight_schedules s ON s.id=c.schedule_id WHERE s.airline_iata=$1 AND c.cabin='ECONOMY' AND s.dep_iata=$3`, [al[1].iata, `${sy}-12-31`, HOME]);
  return al.length;
}

// Idempotent: ensures the sample users exist; builds the catalogue from cached aviationstack responses, or from
// synthetic TLV <-> BKK flights when there are none. Never calls the API.
app.post('/api/admin/sample-data', async (req: any) => {
  await auth(req, ['ADMIN']);
  const sampleUsers = [
    ['seller@example.com', 'seller123', 'Sample Airline Seller', 'SELLER'],
    ['seller2@example.com', 'seller123', 'Sample Airline Seller 2', 'SELLER'],
    ['customer@example.com', 'customer123', 'Sample Customer', 'CUSTOMER'],
    ['customer2@example.com', 'customer123', 'Sample Customer 2', 'CUSTOMER'],
  ];
  for (const [email, pw, name, role] of sampleUsers)
    await pool.query(`INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES($1,$2,$3,$4,$5,true) ON CONFLICT (email) DO UPDATE SET is_sample=true`,
      [id(), email, pw, name, role]);
  let catalogue = 'kept';
  if (!(await pool.query(`SELECT 1 FROM flight_schedules LIMIT 1`)).rows.length) {
    const built = await buildCatalogueFromLedger();
    if (built.schedules) catalogue = `${built.schedules} real flights from ${built.responses} cached aviationstack responses`;
    else catalogue = `${await insertSampleSchedules()} synthetic flights (no aviationstack data yet)`;
  }
  await assignAirlinesToSellers();
  await ensureSamplePriceRules();
  return { ok: true, message: `Sample users ensured; catalogue: ${catalogue}`,
    login: { customer: 'customer@example.com / customer123', customer2: 'customer2@example.com / customer123', seller: 'seller@example.com / seller123', seller2: 'seller2@example.com / seller123' } };
});

const PAX_NAMES = [['Noa', 'Levi'], ['Ariel', 'Cohen'], ['Somchai', 'Srisuk'], ['Maya', 'Mizrahi'], ['Niran', 'Chaiyaporn'], ['Tal', 'Peretz']];
const samplePassengers = (n: number, seed = 0) => Array.from({ length: n }, (_, i) => { const [f, l] = PAX_NAMES[(seed + i) % PAX_NAMES.length];
  return { firstName: f, lastName: l, passport: null }; });
/** The cheapest bookable itinerary from -> to on a date, as bookable legs (or null). */
async function pickItinerary(from: string, to: string, date: string, pax: number, cabin = 'ECONOMY', stops?: number, direction: 'OUTBOUND'|'RETURN' = 'OUTBOUND') {
  const cabins = await loadCabins(`WHERE c.cabin=$1`, [cabin]);
  let itins = findItineraries(cabins, from, to, date);
  if (stops !== undefined) itins = itins.filter(l => l.length - 1 === stops);
  const q = (await quoteItineraries(itins, pax)).filter(x => !x.soldOut).sort((a, b) => a.total - b.total);
  return q[0] ? q[0].legs.map(l => ({ cabin: l.cabin, date: l.date, depAt: l.depAt, arrAt: l.arrAt, direction })) as BookLeg[] : null;
}

// A few trips for ONE customer: a direct round trip, a one-stop one-way, and two trips on the same days so the
// "double booking" notice shows. Same Redis hold path as real bookings.
app.post('/api/admin/sample-bookings', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const { userId } = req.body || {};
  const u = await pool.query(`SELECT id,role,email FROM users WHERE id=$1`, [userId]);
  if (!u.rows[0]) return reply.code(404).send({ error: 'User not found' });
  if (u.rows[0].role !== 'CUSTOMER') return reply.code(400).send({ error: 'Sample bookings can only be created for CUSTOMER users' });
  const t = todayStr(), created: any[] = [], skipped: string[] = [];
  const plans: [string, () => Promise<BookLeg[] | null>, number][] = [
    ['Round trip, direct, 2 passengers', async () => { const o = await pickItinerary(HOME, AWAY, addDays(t, 10), 2, 'ECONOMY', 0);
      const r = o && await pickItinerary(AWAY, HOME, addDays(t, 24), 2, 'ECONOMY', 0, 'RETURN'); return o && r ? [...o, ...r] : null; }, 2],
    ['One-way with a connection', () => pickItinerary(HOME, AWAY, addDays(t, 30), 1, 'ECONOMY', 1), 1],
    ['Business class, one way', () => pickItinerary(AWAY, HOME, addDays(t, 5), 1, 'BUSINESS'), 1],
    ['Same day as the business trip (double booking)', () => pickItinerary(AWAY, HOME, addDays(t, 5), 1, 'ECONOMY'), 1],
  ];
  for (const [what, pick, pax] of plans) {
    try {
      const legs = await pick();
      if (!legs) { skipped.push(`${what}: no flight found`); continue; }
      const b = await createFlightBooking(userId, legs, samplePassengers(pax, created.length), 'CONFIRMED');
      created.push({ what, ...b });
    } catch (e: any) { if (e?.statusCode === 409) skipped.push(`${what}: ${e.message}`); else throw e; }
  }
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,target_user_id,metadata) VALUES($1,$2,'SAMPLE_BOOKINGS_CREATED',$3,$4)`,
    [id(), req.userCtx!.id, userId, JSON.stringify({ created: created.length, skipped: skipped.length })]);
  return { ok: true, created, skipped,
    message: `Created ${created.length} sample trip(s) for ${u.rows[0].email}${skipped.length ? `; skipped: ${skipped.join('; ')}` : ''}` };
});

app.delete('/api/admin/sample-data', async (req: any) => {
  await auth(req, ['ADMIN']);
  // Bookings of sample customers and on synthetic flights go; real (aviationstack) flights stay: rebuilding them is free.
  const bookings = (await pool.query(`SELECT b.id FROM flight_bookings b WHERE b.user_id IN (SELECT id FROM users WHERE is_sample)
    OR EXISTS (SELECT 1 FROM flight_booking_legs l JOIN flight_schedules s ON s.id=l.schedule_id WHERE l.booking_id=b.id AND s.is_sample)`)).rows.map((r: any) => r.id);
  for (const bid of bookings) await cancelBooking(bid, { force: true }).catch(() => {});
  await pool.query(`DELETE FROM flight_bookings WHERE id = ANY($1)`, [bookings]);
  const cabins = (await pool.query(`SELECT c.id FROM flight_cabins c JOIN flight_schedules s ON s.id=c.schedule_id WHERE s.is_sample`)).rows;
  for (const c of cabins) await redis.unlink(...cabinMonthKeys(c.id));
  const flights = (await pool.query(`DELETE FROM flight_schedules WHERE is_sample`)).rowCount;
  await pool.query(`DELETE FROM flight_price_rules WHERE airline_iata IS NOT NULL`);
  await pool.query(`UPDATE airlines SET seller_id=NULL, weekday_pct=NULL WHERE seller_id IN (SELECT id FROM users WHERE is_sample)`);
  await pool.query(`DELETE FROM airlines a WHERE NOT EXISTS (SELECT 1 FROM flight_schedules s WHERE s.airline_iata=a.iata)`);
  await pool.query(`DELETE FROM simulation_runs`);
  await pool.query(`DELETE FROM audit_logs WHERE actor_user_id IN (SELECT id FROM users WHERE is_sample) OR target_user_id IN (SELECT id FROM users WHERE is_sample)`);
  await pool.query(`DELETE FROM users WHERE is_sample`);
  return { ok: true, message: `Sample data deleted (${bookings.length} bookings, ${flights} synthetic flights, simulation customers and runs). Real flights kept.` };
});

// ---- Load simulation ----------------------------------------------------------------------------------------------------
// N throw-away customers book at the same instant. Mode 'random': random TLV <-> BKK trips in the next 14 days; a share
// pays, a share lets the hold expire and rebooks, the rest abandon. Mode 'same-flight': everyone wants one departure;
// rejected customers cascade to another itinerary on the same day, then the next day. Every step is timed.
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const timed = async <T,>(fn: () => Promise<T>) => {
  const t = performance.now();
  try { const value = await fn(); return { ok: true as const, ms: performance.now() - t, value }; }
  catch (e: any) { return { ok: false as const, ms: performance.now() - t, error: e }; }
};
function stats(ms: number[]) {
  if (!ms.length) return { count: 0, avgMs: 0, p50Ms: 0, p95Ms: 0, maxMs: 0 };
  const a = [...ms].sort((x, y) => x - y); const q = (p: number) => a[Math.min(a.length - 1, Math.floor(p * a.length))];
  return { count: a.length, avgMs: +(a.reduce((x, y) => x + y, 0) / a.length).toFixed(1), p50Ms: +q(0.5).toFixed(1), p95Ms: +q(0.95).toFixed(1), maxMs: +a[a.length - 1].toFixed(1) };
}
const running = new Set<string>();
type SimParams = { mode: 'random'|'same-flight'; customers: number; payRatio: number; lateRatio: number; windowSeconds: number; passengers: number;
  cabinId?: string; date?: string };
function simContext(runId: string) {
  const started = Date.now(); const log: string[] = [];
  const note = (m: string) => { log.push(`${((Date.now() - started) / 1000).toFixed(1)}s ${m}`); app.log.info({ runId }, m); };
  const save = async (status: string, report: any) =>
    pool.query(`UPDATE simulation_runs SET status=$2, report=$3, finished_at=CASE WHEN $2 IN ('DONE','FAILED') THEN now() END WHERE id=$1`, [runId, status, JSON.stringify({ ...report, log })]);
  return { started, log, note, save };
}
async function simCreateCustomers(runId: string, n: number) {
  const short = runId.slice(0, 8);
  const values: any[] = []; const rowsSql: string[] = [];
  for (let i = 1; i <= n; i++) { values.push(id(), `sim-${short}-${i}@example.com`, 'sim123', `Sim ${short} #${i}`); rowsSql.push(`($${values.length - 3},$${values.length - 2},$${values.length - 1},$${values.length},'CUSTOMER',true)`); }
  const users = await pool.query(`INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES ${rowsSql.join(',')} RETURNING id`, values);
  const customerIds: string[] = users.rows.map((u: any) => u.id);
  await pool.query(`UPDATE simulation_runs SET customer_ids=$2 WHERE id=$1`, [runId, customerIds]);
  return customerIds;
}
/** Waits (max 30s) for this run's outbox events to reach Kafka and returns lag stats. */
async function simOutbox(started: number, note: (m: string) => void) {
  const drainStart = Date.now(); let pendingEvents = 1;
  while (pendingEvents > 0 && Date.now() - drainStart < 30000) {
    await sleep(500);
    pendingEvents = (await pool.query(`SELECT count(*)::int AS n FROM outbox_events WHERE created_at >= to_timestamp($1/1000.0) AND published_at IS NULL`, [started])).rows[0].n;
  }
  const drainMs = Date.now() - drainStart;
  note(`outbox drained in ${(drainMs / 1000).toFixed(1)}s (${pendingEvents} events still unpublished)`);
  const lag = await pool.query(`
    SELECT count(*)::int AS events, count(published_at)::int AS published,
      COALESCE(avg(EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "avgMs",
      COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "p95Ms",
      COALESCE(max(EXTRACT(EPOCH FROM (published_at-created_at))*1000),0)::float AS "maxMs"
    FROM outbox_events WHERE created_at >= to_timestamp($1/1000.0)`, [started]);
  return { ...lag.rows[0], avgMs: +lag.rows[0].avgMs.toFixed(0), p95Ms: +lag.rows[0].p95Ms.toFixed(0), maxMs: +lag.rows[0].maxMs.toFixed(0), drainMs, unpublished: pendingEvents };
}
async function simFinalStatuses(customerIds: string[]) {
  const final = await pool.query(`SELECT status, count(*)::int FROM flight_bookings WHERE user_id = ANY($1) GROUP BY status`, [customerIds]);
  return Object.fromEntries(final.rows.map((r: any) => [r.status, r.count]));
}
const shuffle = <T,>(a: T[]) => { const b = [...a]; for (let i = b.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [b[i], b[j]] = [b[j], b[i]]; } return b; };

async function runSameFlightSimulation(runId: string, params: SimParams) {
  const { started, note, save } = simContext(runId);
  try {
    const customerIds = await simCreateCustomers(runId, params.customers);
    note(`created ${customerIds.length} customers`);
    const [target] = await loadCabins(`WHERE c.id=$1`, [params.cabinId]);
    if (!target) throw new Error('Target flight not found');
    const date = params.date!, pax = params.passengers;
    const cabins = await loadCabins(`WHERE c.cabin=$1`, [target.cabin]);
    const leg = (c: Cabin, d: string): BookLeg => ({ cabin: c, date: d, ...legTimes(c, d), direction: 'OUTBOUND' });
    const sameDay = findItineraries(cabins, target.dep_iata, target.arr_iata, date).filter(l => !(l.length === 1 && l[0].cabin.id === target.id));
    const nextDay = findItineraries(cabins, target.dep_iata, target.arr_iata, addDays(date, 1));
    const label = (legs: Leg[]) => legs.map(l => l.cabin.flight_number).join(' + ') + ` (${legs[0].date})`;
    const timings: Record<string, number[]> = { target: [], sameDay: [], nextDay: [], pay: [] };
    const tiers: Record<string, number> = { target: 0, sameDay: 0, nextDay: 0, none: 0 };
    const attemptsPerTier: Record<string, number> = { target: 0, sameDay: 0, nextDay: 0 };
    const won: Record<string, number> = {}; let paid = 0, payFailed = 0, errors = 0;
    const attempt = async (userId: string, legs: Leg[], tier: string, n: number) => {
      attemptsPerTier[tier]++;
      const r = await timed(() => createFlightBooking(userId, legs.map(l => ({ ...l, direction: 'OUTBOUND' as const })), samplePassengers(pax, n), 'PENDING', params.windowSeconds));
      timings[tier].push(r.ms);
      if (!r.ok && r.error?.statusCode !== 409) { errors++; app.log.error(r.error); }
      return r.ok ? r.value : null;
    };
    const flow = async (userId: string, n: number) => {
      let attempts = 1, tier = 'target', trip = label([leg(target, date)]);
      let b = await attempt(userId, [leg(target, date)], 'target', n);
      if (!b) { tier = 'sameDay'; for (const l of shuffle(sameDay).slice(0, 3)) { attempts++; b = await attempt(userId, l, tier, n); if (b) { trip = label(l); break; } } }
      if (!b) { tier = 'nextDay'; for (const l of shuffle(nextDay).slice(0, 3)) { attempts++; b = await attempt(userId, l, tier, n); if (b) { trip = label(l); break; } } }
      if (!b) return { tier: 'none', attempts };
      const p = await timed(() => payBooking(b.bookingId, userId)); timings.pay.push(p.ms); if (p.ok) paid++; else payFailed++;
      return { tier, attempts, trip };
    };
    const burstStart = performance.now();
    const outcomes = await Promise.all(customerIds.map((u, i) => flow(u, i)));
    const burstMs = performance.now() - burstStart;
    for (const o of outcomes) { tiers[o.tier]++; if (o.trip) won[o.trip] = (won[o.trip] || 0) + 1; }
    const totalAttempts = outcomes.reduce((a, o) => a + o.attempts, 0);
    note(`cascade done in ${burstMs.toFixed(0)}ms: ${tiers.target} got the target flight, ${tiers.sameDay} another option that day, ${tiers.nextDay} the next day, ${tiers.none} nothing`);
    const outbox = await simOutbox(started, note);
    const ops: Record<string, any> = { 'Target flight attempt': stats(timings.target), 'Same-day alternative attempt': stats(timings.sameDay),
      'Next-day alternative attempt': stats(timings.nextDay), 'Pay request': stats(timings.pay) };
    const bottlenecks = [
      ...Object.entries(ops).filter(([, v]) => v.count).map(([name, v]) => ({ step: name, p95Ms: v.p95Ms, note: '' })),
      { step: 'Outbox -> Kafka publish lag (insert to publish)', p95Ms: outbox.p95Ms, note: `publisher polls every 1.5s, one Kafka send per event; ${outbox.events} events drained in ${(outbox.drainMs / 1000).toFixed(1)}s` },
    ].sort((a, b) => b.p95Ms - a.p95Ms);
    await save('DONE', {
      params, mode: params.mode, customers: customerIds.length, durationMs: Date.now() - started, burstMs: +burstMs.toFixed(0),
      throughputPerSec: +(totalAttempts / (burstMs / 1000)).toFixed(1),
      target: { flight: target.flight_number, from: target.dep_iata, to: target.arr_iata, cabin: target.cabin, totalSeats: target.total_seats, date, passengers: pax,
        sameDayOptions: sameDay.length, nextDayOptions: nextDay.length },
      tiers, attemptsPerTier, totalAttempts, avgAttemptsPerCustomer: +(totalAttempts / customerIds.length).toFixed(2),
      counts: { bookAttempts: totalAttempts, locked: customerIds.length - tiers.none, soldOut: tiers.none, paid, payFailed, errors },
      tripsWon: Object.entries(won).map(([trip, count]) => ({ trip, count })).sort((a: any, b: any) => b.count - a.count),
      outcomes: [
        { outcome: `got the target flight (${target.flight_number} ${date})`, count: tiers.target },
        { outcome: 'target full -> another option the same day', count: tiers.sameDay },
        { outcome: 'that day full -> the next day', count: tiers.nextDay },
        { outcome: 'nothing found', count: tiers.none },
      ],
      finalStatuses: await simFinalStatuses(customerIds), timings: ops, outbox, bottlenecks, progressPct: 100,
    });
    note('done');
  } catch (e: any) { app.log.error(e); await save('FAILED', { error: e?.message || String(e), progressPct: 100 }); }
  finally { running.delete(runId); }
}

async function runSimulation(runId: string, params: SimParams) {
  const { started, note, save } = simContext(runId);
  try {
    const customerIds = await simCreateCustomers(runId, params.customers);
    note(`created ${customerIds.length} customers`);
    const cabins = await loadCabins(`WHERE c.cabin='ECONOMY'`);
    const t = todayStr();
    // Every itinerary in both directions over the next 14 days, picked at random per customer.
    const pool14: Leg[][] = [];
    for (let i = 1; i <= 14; i++) for (const [a, b] of [[HOME, AWAY], [AWAY, HOME]]) pool14.push(...findItineraries(cabins, a, b, addDays(t, i)));
    if (!pool14.length) throw new Error('No flights exist. Generate sample data first.');
    const pick = () => pool14[Math.floor(Math.random() * pool14.length)];
    type Actor = { userId: string; legs: Leg[]; behavior: 'pay'|'late'|'abandon'|'none'; bookingId?: string; outcome: string };
    const actors: Actor[] = customerIds.map(uid => ({ userId: uid, legs: pick(), behavior: 'none', outcome: '' }));
    const timings: Record<string, number[]> = { book: [], bookRedis: [], bookPg: [], bookRejected: [], pay: [], rebook: [], rebookPay: [], retryOther: [] };
    const counts: Record<string, number> = { bookAttempts: 0, locked: 0, soldOut: 0, retryOther: 0, retrySucceeded: 0, paid: 0, payFailed: 0, late: 0, timedOut: 0, rebookAttempts: 0, rebooked: 0, rebookSoldOut: 0, abandoned: 0, errors: 0 };
    const book = async (a: Actor, bucket = 'book') => {
      const r = await timed(() => createFlightBooking(a.userId, a.legs.map(l => ({ ...l, direction: 'OUTBOUND' as const })), samplePassengers(params.passengers), 'PENDING', params.windowSeconds));
      timings[bucket].push(r.ms);
      if (r.ok) { a.bookingId = r.value.bookingId; if (bucket === 'book') { timings.bookRedis.push(r.value.timings.redisMs); timings.bookPg.push(r.value.timings.pgMs); } return true; }
      if (r.error?.statusCode !== 409) { counts.errors++; app.log.error(r.error); } else if (bucket === 'book') timings.bookRejected.push(r.ms);
      return false;
    };
    const burstStart = performance.now();
    const results = await Promise.all(actors.map(a => book(a)));
    const burstMs = performance.now() - burstStart;
    counts.bookAttempts = actors.length; counts.locked = results.filter(Boolean).length; counts.soldOut = actors.length - counts.locked;
    note(`burst: ${counts.locked} held, ${counts.soldOut} sold out in ${burstMs.toFixed(0)}ms`);
    await Promise.all(actors.filter(a => !a.bookingId).map(async a => { counts.retryOther++; a.legs = pick(); if (await book(a, 'retryOther')) counts.retrySucceeded++; else a.outcome = 'sold out twice'; }));
    const holders = shuffle(actors.filter(a => a.bookingId));
    const nPay = Math.round(holders.length * params.payRatio), nLate = Math.round(holders.length * params.lateRatio);
    holders.forEach((a, i) => a.behavior = i < nPay ? 'pay' : i < nPay + nLate ? 'late' : 'abandon');
    await Promise.all(holders.filter(a => a.behavior === 'pay').map(async a => { const r = await timed(() => payBooking(a.bookingId!, a.userId)); timings.pay.push(r.ms);
      if (r.ok) { counts.paid++; a.outcome = 'paid'; } else { counts.payFailed++; a.outcome = 'pay failed: ' + (r.error?.message || 'error'); } }));
    note(`${counts.paid} paid immediately`);
    const waiting = holders.filter(a => a.behavior !== 'pay'); counts.late = holders.filter(a => a.behavior === 'late').length; counts.abandoned = holders.filter(a => a.behavior === 'abandon').length;
    await save('RUNNING', { phase: `waiting ${params.windowSeconds}s for ${waiting.length} holds to expire`, counts, progressPct: 50 });
    const expiryLatency: number[] = []; const deadline = Date.now() + (params.windowSeconds + 15) * 1000;
    const pendingIds = new Set(waiting.map(a => a.bookingId!));
    while (pendingIds.size && Date.now() < deadline) {
      await sleep(500);
      const r = await pool.query(`SELECT id, status, expires_at FROM flight_bookings WHERE id = ANY($1) AND status <> 'PENDING'`, [[...pendingIds]]);
      for (const row of r.rows) { pendingIds.delete(row.id); if (row.status === 'PAYMENT_TIMEOUT') { counts.timedOut++; expiryLatency.push(Date.now() - new Date(row.expires_at).getTime()); } }
    }
    note(`${counts.timedOut} holds timed out (${pendingIds.size} still pending)`);
    await Promise.all(holders.filter(a => a.behavior === 'late').map(async a => {
      counts.rebookAttempts++;
      if (await book(a, 'rebook')) { counts.rebooked++; const r = await timed(() => payBooking(a.bookingId!, a.userId)); timings.rebookPay.push(r.ms); a.outcome = r.ok ? 'late, rebooked and paid' : 'late, rebooked, pay failed'; if (r.ok) counts.paid++; }
      else { counts.rebookSoldOut++; a.outcome = 'late, seats gone on rebook'; }
    }));
    holders.filter(a => a.behavior === 'abandon').forEach(a => a.outcome = 'abandoned (timed out)');
    note(`late customers: ${counts.rebooked} rebooked, ${counts.rebookSoldOut} lost the seats`);
    const outbox = await simOutbox(started, note);
    const ops: Record<string, any> = {
      'Redis hold (Lua reserve, all legs)': stats(timings.bookRedis), 'PostgreSQL booking write (tx + legs + outbox rows)': stats(timings.bookPg),
      'Book request end-to-end': stats(timings.book), 'Rejected (sold out) request': stats(timings.bookRejected), 'Pay request': stats(timings.pay),
      'Retry on another trip': stats(timings.retryOther), 'Rebook after timeout': stats(timings.rebook), 'Pay after rebook': stats(timings.rebookPay),
    };
    const expiry = stats(expiryLatency);
    const bottlenecks = [
      ...Object.entries(ops).filter(([, v]) => v.count).map(([name, v]) => ({ step: name, p95Ms: v.p95Ms, note: '' })),
      { step: 'Outbox -> Kafka publish lag (insert to publish)', p95Ms: outbox.p95Ms, note: `publisher polls every 1.5s, 50 rows per poll, one Kafka send per event; ${outbox.events} events drained in ${(outbox.drainMs / 1000).toFixed(1)}s` },
      { step: 'Payment-timeout worker delay after expiry', p95Ms: expiry.p95Ms, note: 'worker polls every 2s (measured with 0.5s sampling)' },
    ].sort((a, b) => b.p95Ms - a.p95Ms);
    await save('DONE', {
      params, mode: params.mode, customers: customerIds.length, durationMs: Date.now() - started, burstMs: +burstMs.toFixed(0),
      throughputPerSec: +(counts.bookAttempts / (burstMs / 1000)).toFixed(1),
      counts, finalStatuses: await simFinalStatuses(customerIds), timings: ops, outbox, expiryWorker: expiry, bottlenecks,
      redisShareOfBookPct: timings.book.length ? Math.round(100 * timings.bookRedis.reduce((a, b) => a + b, 0) / timings.book.reduce((a, b) => a + b, 0)) : 0,
      pgShareOfBookPct: timings.book.length ? Math.round(100 * timings.bookPg.reduce((a, b) => a + b, 0) / timings.book.reduce((a, b) => a + b, 0)) : 0,
      outcomes: Object.entries(actors.reduce((m: Record<string, number>, a) => { const k = a.outcome || (a.bookingId ? 'holding seats' : 'no booking'); m[k] = (m[k] || 0) + 1; return m; }, {}))
        .map(([outcome, count]) => ({ outcome, count })).sort((a: any, b: any) => b.count - a.count),
      progressPct: 100,
    });
    note('done');
  } catch (e: any) { app.log.error(e); await save('FAILED', { error: e?.message || String(e), progressPct: 100 }); }
  finally { running.delete(runId); }
}

app.post('/api/admin/simulation', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  if (running.size) return reply.code(409).send({ error: 'A simulation is already running' });
  const b = req.body || {};
  const params: SimParams = {
    mode: b.mode === 'same-flight' ? 'same-flight' : 'random',
    customers: Math.min(500, Math.max(1, Number(b.customers ?? 100))),
    payRatio: Math.min(1, Math.max(0, Number(b.payRatio ?? 0.5))),
    lateRatio: Math.min(1, Math.max(0, Number(b.lateRatio ?? 0.3))),
    windowSeconds: Math.min(60, Math.max(3, Number(b.windowSeconds ?? 5))),
    passengers: Math.min(MAX_PASSENGERS, Math.max(1, Number(b.passengers ?? 2))),
    cabinId: b.cabinId || undefined, date: b.date || undefined,
  };
  if (params.payRatio + params.lateRatio > 1) return reply.code(400).send({ error: 'payRatio + lateRatio must be <= 1' });
  if (params.mode === 'same-flight') {
    if (!params.cabinId) return reply.code(400).send({ error: 'Pick the target flight' });
    const problem = dateProblem(params.date!);
    if (problem) return reply.code(400).send({ error: problem });
  }
  const runId = id();
  await pool.query(`INSERT INTO simulation_runs(id, status, params, report, created_by) VALUES($1,'RUNNING',$2,'{}',$3)`, [runId, JSON.stringify(params), req.userCtx!.id]);
  running.add(runId);
  (params.mode === 'same-flight' ? runSameFlightSimulation : runSimulation)(runId, params); // fire and forget; poll GET /api/admin/simulation/:id
  return { runId, status: 'RUNNING', params };
});
const RUN_COLUMNS = `id, status, params, created_at AS "createdAt", finished_at AS "finishedAt", cardinality(customer_ids) AS customers,
  (SELECT count(*)::int FROM flight_bookings WHERE user_id = ANY(customer_ids) AND status IN ('CONFIRMED','PENDING')) AS "activeBookings"`;
app.get('/api/admin/simulation', async (req: any) => {
  await auth(req, ['ADMIN']);
  return (await pool.query(`SELECT ${RUN_COLUMNS} FROM simulation_runs ORDER BY created_at DESC LIMIT 10`)).rows;
});
app.get('/api/admin/simulation/:id', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const r = await pool.query(`SELECT ${RUN_COLUMNS}, report FROM simulation_runs WHERE id=$1`, [req.params.id]);
  if (!r.rows[0]) return reply.code(404).send({ error: 'Run not found' });
  return r.rows[0];
});
// Cancels every active booking made by this run's customers (admin force: departed rule does not apply).
app.post('/api/admin/simulation/:id/cancel-all', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const run = await pool.query(`SELECT customer_ids FROM simulation_runs WHERE id=$1`, [req.params.id]);
  if (!run.rows[0]) return reply.code(404).send({ error: 'Run not found' });
  const ids = (await pool.query(`SELECT id FROM flight_bookings WHERE user_id = ANY($1) AND status IN ('CONFIRMED','PENDING')`, [run.rows[0].customer_ids || []])).rows.map((r: any) => r.id);
  let cancelled = 0, failed = 0; const t0 = performance.now();
  for (const bid of ids) { try { await cancelBooking(bid, { force: true }); cancelled++; } catch (e: any) { failed++; if (!e?.statusCode) app.log.error(e); } }
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'SIMULATION_CANCEL_ALL',$3)`, [id(), req.userCtx!.id, JSON.stringify({ runId: req.params.id, cancelled, failed })]);
  return { ok: true, cancelled, failed, durationMs: Math.round(performance.now() - t0), message: `Cancelled ${cancelled} booking(s) of this run's customers${failed ? `, ${failed} failed` : ''}` };
});

// Concurrency demo: every customer books N seats on the same departure at the same instant.
// The Redis Lua hold decides who gets seats; the rest get "Not enough seats left".
app.post('/api/admin/concurrent-booking', async (req: any, reply) => {
  await auth(req, ['ADMIN']);
  const { cabinId, date, confirm } = req.body || {};
  const passengers = Math.min(MAX_PASSENGERS, Math.max(1, Number(req.body?.passengers) || 1));
  const legs = await resolveLegs([{ cabinId, date }]);
  if ('error' in legs) return reply.code(400).send({ error: legs.error });
  const c = legs.legs[0].cabin;
  const customers = await pool.query(`SELECT id,name,email FROM users WHERE role='CUSTOMER' ORDER BY created_at`);
  if (!customers.rows.length) return reply.code(409).send({ error: 'No customers exist yet' });
  const seatsFree = async () => { const b = await bookedCounts([{ cabinId: c.id, date }]); if (!b) throw NOT_LOADED; return c.total_seats - b[0]; };
  const before = await seatsFree();
  const startedAt = Date.now();
  const settled = await Promise.allSettled(customers.rows.map((u: any, i: number) =>
    createFlightBooking(u.id, legs.legs, samplePassengers(passengers, i), confirm ? 'CONFIRMED' : 'PENDING')));
  const durationMs = Date.now() - startedAt;
  const results = settled.map((x, i) => {
    const u = customers.rows[i];
    if (x.status === 'fulfilled') return { customer: u.name, email: u.email, ok: true, bookingId: x.value.bookingId, status: x.value.status, seatsLeft: x.value.seatsLeft };
    const e: any = x.reason;
    if (e?.statusCode !== 409) app.log.error(e);
    return { customer: u.name, email: u.email, ok: false, error: e?.statusCode === 409 ? e.message : 'Internal error' };
  });
  const after = await seatsFree();
  const won = results.filter(x => x.ok).length;
  await pool.query(`INSERT INTO audit_logs(id,actor_user_id,action,metadata) VALUES($1,$2,'CONCURRENT_BOOKING_DEMO',$3)`,
    [id(), req.userCtx!.id, JSON.stringify({ cabinId, date, passengers, customers: customers.rows.length, won, before, after, confirm: !!confirm })]);
  return { ok: true,
    message: `${customers.rows.length} customers booked ${passengers} seat(s) each on ${c.flight_number} (${c.cabin.toLowerCase()}) ${date} at once: ${won} got seats, ${results.length - won} were rejected. Seats free ${before} -> ${after}.`,
    flight: c.flight_number, cabin: c.cabin, date, passengers, seatsBefore: before, seatsAfter: after, durationMs, results };
});

// ---- Workers ----------------------------------------------------------------------------------------------------------
// Kafka being down never stops bookings: events wait in outbox_events and go out once the producer (re)connects.
async function outboxLoop() {
  let connected = false;
  while (true) {
    try {
      if (!connected) { await producer.connect(); connected = true; app.log.info('Kafka producer connected'); }
      const r = await pool.query(`SELECT * FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 50`);
      for (const e of r.rows) {
        await producer.send({ topic: e.topic, messages: [{ key: e.event_key, value: JSON.stringify(e.payload) }] });
        await pool.query(`UPDATE outbox_events SET published_at=NOW() WHERE id=$1`, [e.id]);
      }
    } catch (err) { connected = false; app.log.error(err); await producer.disconnect().catch(() => {}); }
    await sleep(1500);
  }
}

// Releases seats whose payment window passed: PENDING -> PAYMENT_TIMEOUT, every leg back to Redis, events out.
async function expirePendingBookings() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(`
      WITH due AS (SELECT id FROM flight_bookings WHERE status='PENDING' AND expires_at <= now() FOR UPDATE SKIP LOCKED LIMIT 100)
      UPDATE flight_bookings b SET status='PAYMENT_TIMEOUT' FROM due WHERE b.id=due.id RETURNING b.id, b.user_id, b.passengers`);
    for (const row of r.rows) {
      const legs = await legRows(client, row.id);
      const seatsLeft = await releaseLegs(seatRefs(legs), row.passengers);
      await addOutbox(client, 'flight.booking.payment_timeout', row.id, { bookingId: row.id, userId: row.user_id, passengers: row.passengers, seatsLeft,
        legs: legs.map(l => ({ flightNumber: l.flight_number, date: l.dep_date })) });
      for (const l of legs) await addOutbox(client, 'flight.seats.changed', l.cabin_id, { cabinId: l.cabin_id, flightNumber: l.flight_number, date: l.dep_date, delta: row.passengers });
      app.log.info({ bookingId: row.id, seatsLeft }, 'flight booking payment timed out, seats released');
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
}
async function expiryLoop() {
  while (true) {
    try { await expirePendingBookings(); } catch (err) { app.log.error(err); }
    await sleep(2000);
  }
}

// Redis is a cache of PostgreSQL truth: write the seats booked on every departure that has active bookings, drop what
// no booking backs any more, then mark Redis as loaded. Runs at startup and on demand from the admin panel.
async function rebuildAvailability() {
  const t0 = performance.now();
  const first = todayStr(), end = addDays(first, AVAILABILITY_DAYS);
  const r = await pool.query(`
    SELECT l.cabin_id, to_char(l.dep_date,'YYYY-MM-DD') AS date, sum(b.passengers)::int AS booked
    FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
    WHERE b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= $1::date AND l.dep_date < $2::date GROUP BY 1, 2`, [first, end]);
  const wanted = new Map<string, Record<string, string>>();
  for (const row of r.rows) {
    const k = monthKey(row.cabin_id, row.date.slice(0, 7));
    wanted.set(k, { ...wanted.get(k), [dayField(row.date)]: String(row.booked) });
  }
  // HSET only overwrites, so a booking running right now never sees one of its days disappear.
  let pipe = redis.pipeline(), queued = 0;
  for (const [k, fields] of wanted) {
    pipe.hset(k, fields).expireat(k, monthExpireAt(k.slice(-7)));
    if (++queued % 2000 === 0) { await execOrThrow(pipe); pipe = redis.pipeline(); }
  }
  await execOrThrow(pipe);
  let staleFieldsDropped = 0, staleKeysDropped = 0;
  const cabinOf = (k: string) => k.slice(4, 40);
  for await (const keys of redis.scanStream({ match: 'fs:{*', count: 1000 }) as AsyncIterable<string[]>) {
    if (!keys.length) continue;
    const known = new Set((await pool.query(`SELECT id FROM flight_cabins WHERE id = ANY($1)`, [[...new Set(keys.map(cabinOf))]])).rows.map((x: any) => x.id));
    const gone = keys.filter(k => !known.has(cabinOf(k)));
    if (gone.length) { await redis.unlink(...gone); staleKeysDropped += gone.length; }
    const check = keys.filter(k => known.has(cabinOf(k)));
    if (!check.length) continue;
    const fields = await execOrThrow(check.reduce((p, k) => p.hkeys(k), redis.pipeline()));
    const del = redis.pipeline();
    check.forEach((k, i) => {
      const extra = (fields[i][1] as string[]).filter(f => !wanted.get(k)?.[f]);
      if (extra.length) { del.hdel(k, ...extra); staleFieldsDropped += extra.length; }
    });
    await execOrThrow(del);
  }
  await redis.set(FS_LOADED, new Date().toISOString());
  const summary = { bookedSeats: r.rows.reduce((a: number, x: any) => a + x.booked, 0), departures: r.rows.length, keys: wanted.size,
    staleFieldsDropped, staleKeysDropped, ms: Math.round(performance.now() - t0) };
  app.log.info(summary, 'seat availability rebuilt from PostgreSQL');
  return summary;
}
app.post('/api/admin/rebuild-availability', async (req: any) => {
  await auth(req, ['ADMIN']);
  const s = await rebuildAvailability();
  return { ok: true, ...s, message: `Redis rebuilt from PostgreSQL in ${s.ms} ms: ${s.bookedSeats} booked seats on ${s.departures} departures in ${s.keys} keys; ${s.staleFieldsDropped} stale days and ${s.staleKeysDropped} stale keys dropped` };
});

async function redisMemory() {
  const info = Object.fromEntries((await redis.info('memory')).split('\r\n').filter(l => l.includes(':')).map(l => l.split(':') as [string, string]));
  return { usedBytes: Number(info.used_memory), peakBytes: Number(info.used_memory_peak), datasetBytes: Number(info.used_memory_dataset), maxBytes: Number(info.maxmemory) || 0 };
}

// Every departure (cabin x day it flies) of the booking window with its booked count in Redis (filter by flight and/or
// date; paged), plus a consistency check: Redis vs active bookings in PostgreSQL for every booked departure.
app.get('/api/admin/redis-records', async (req: any) => {
  await auth(req, ['ADMIN']);
  const { scheduleId, date } = req.query;
  const page = Math.max(1, Number(req.query.page) || 1), limit = Math.min(500, Math.max(1, Number(req.query.limit) || 50));
  const first = todayStr(), end = addDays(first, AVAILABILITY_DAYS);
  const all = await loadCabins(`ORDER BY s.flight_number, c.cabin DESC`);
  const pg = (await pool.query(`
    SELECT l.cabin_id, to_char(l.dep_date,'YYYY-MM-DD') AS date, sum(b.passengers)::int AS booked
    FROM flight_booking_legs l JOIN flight_bookings b ON b.id=l.booking_id
    WHERE b.status IN ('CONFIRMED','PENDING') AND l.dep_date >= $1::date AND l.dep_date < $2::date GROUP BY 1, 2 LIMIT 50000`, [first, end])).rows;
  const redisVals = pg.length ? await execOrThrow(pg.reduce((p, x: any) => p.hget(monthKey(x.cabin_id, x.date.slice(0, 7)), dayField(x.date)), redis.pipeline())) : [];
  const summary = { totalKeys: await redis.dbsize(), cabins: all.length, windowDays: AVAILABILITY_DAYS, memory: await redisMemory(), loadedAt: await redis.get(FS_LOADED),
    checkedDepartures: pg.length, bookedSeats: pg.reduce((a: number, x: any) => a + x.booked, 0),
    mismatches: pg.filter((x: any, i) => Number(redisVals[i][1] || 0) !== x.booked).length };
  const cabins = scheduleId ? all.filter(c => c.schedule_id === scheduleId) : all;
  const dates = date ? [String(date)] : Array.from({ length: AVAILABILITY_DAYS }, (_, i) => addDays(first, i));
  // Only days the flight flies. Row order: date, then flight, then cabin.
  const rows: { cabin: Cabin; date: string }[] = [];
  for (const d of dates) for (const c of cabins) if (runsOn(c, d)) rows.push({ cabin: c, date: d });
  const slice = rows.slice((page - 1) * limit, page * limit);
  const vals = slice.length ? await execOrThrow(slice.reduce((p, x) => p.hget(monthKey(x.cabin.id, x.date.slice(0, 7)), dayField(x.date)), redis.pipeline())) : [];
  return { summary, page, limit, total: rows.length, flights: [...new Map(all.map(c => [c.schedule_id, { id: c.schedule_id, name: `${c.flight_number} ${c.dep_iata} → ${c.arr_iata}` }])).values()],
    items: slice.map((x, i) => { const booked = Number(vals[i][1] || 0);
      return { key: `${monthKey(x.cabin.id, x.date.slice(0, 7))} · ${dayField(x.date)}`, stored: vals[i][1] !== null, date: x.date, booked,
        available: Math.max(0, x.cabin.total_seats - booked), flight: x.cabin.flight_number, route: `${x.cabin.dep_iata} → ${x.cabin.arr_iata}`,
        cabin: x.cabin.cabin, totalSeats: x.cabin.total_seats }; }) };
});

app.get('/api/admin/availability-log', async (req: any) => {
  await auth(req, ['ADMIN']);
  const limit = Math.min(500, Math.max(1, Number(req.query.limit) || 100));
  try {
    return (await pool.query(`
      SELECT id, ran_at AS "ranAt", trigger, status, to_char(window_first,'YYYY-MM-DD') AS "windowFirst", to_char(window_last,'YYYY-MM-DD') AS "windowLast",
        added_keys AS "addedKeys", removed_keys AS "removedKeys", added_nights AS "addedNights", removed_nights AS "removedNights", duration_ms AS "durationMs", error
      FROM availability_window_log ORDER BY ran_at DESC, id DESC LIMIT $1`, [limit])).rows;
  } catch (e: any) {
    if (e?.code === '42P01') return []; // table not created yet: the worker has never run
    throw e;
  }
});

// ---- Startup --------------------------------------------------------------------------------------------------------
// Country defaults (Sunday..Saturday %), identical to the hotel lab where both have them, and seasons for departures
// from Israel (winter holidays, summer) and Thailand (high season going home). Only where nothing is set yet.
async function seedCountryPricing() {
  const defaults: [string, number[]][] = [['Israel', [0, -15, 0, 0, 30, 40, 10]], ['Thailand', [0, 0, 0, -10, 0, 20, 30]]];
  for (const [c, pct] of defaults) await pool.query(`INSERT INTO country_pricing(country, weekday_pct) VALUES($1,$2) ON CONFLICT DO NOTHING`, [c, pct]);
  const t = todayStr(), y = Number(t.slice(0, 4));
  const winter = t > `${y}-01-15` ? y : y - 1, summer = t > `${y}-08-31` ? y + 1 : y;
  const seasons: [string, string, string, string, number][] = [
    ['Israel', 'Winter holidays', `${winter}-12-15`, `${winter + 1}-01-10`, 35], ['Israel', 'Summer', `${summer}-07-01`, `${summer}-08-31`, 25],
    ['Thailand', 'High season', `${winter}-12-15`, `${winter + 1}-01-15`, 20]];
  for (const [c, name, start, end, v] of seasons) await pool.query(`INSERT INTO flight_price_rules(id,country,kind,name,start_date,end_date,adjust_type,adjust_value)
    SELECT $1,$2,'SEASON',$3,$4,$5,'PERCENT',$6 WHERE NOT EXISTS (SELECT 1 FROM flight_price_rules WHERE country=$2 AND name=$3)`, [id(), c, name, start, end, v]);
}

async function seedCurrencies() {
  await pool.query(`CREATE TABLE IF NOT EXISTS currency_rates (code TEXT PRIMARY KEY, name TEXT NOT NULL, symbol TEXT NOT NULL,
    thb_per_unit NUMERIC(12,4) NOT NULL CHECK (thb_per_unit > 0), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`);
  for (const [code, name, symbol, rate] of CURRENCY_DEFAULTS)
    await pool.query(`INSERT INTO currency_rates(code,name,symbol,thb_per_unit) VALUES($1,$2,$3,$4) ON CONFLICT (code) DO NOTHING`, [code, name, symbol, rate]);
}

async function start() {
  await seedCurrencies();
  await seedCountryPricing();
  const snaps = await loadSnapshots();
  if (snaps) app.log.info({ snapshots: snaps }, 'aviationstack snapshots loaded into the request ledger (no API calls)');
  // Pending rows left by a crash never completed; they stay visible as FAILED and are not repeated automatically.
  await pool.query(`UPDATE api_requests SET status='FAILED', error='API restarted during the call' WHERE status='PENDING'`);
  await pool.query(`UPDATE simulation_runs SET status='FAILED', report = report || '{"error":"API restarted while running"}' WHERE status='RUNNING'`);
  try { await rebuildAvailability(); } catch (err) { app.log.error(err, 'startup availability rebuild failed'); }
  await app.listen({ port: Number(process.env.PORT || 3020), host: '0.0.0.0' });
  outboxLoop();
  expiryLoop();
}
start();
