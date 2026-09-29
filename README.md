# Flight Booking Lab

A hands-on learning project that rebuilds the core of a flight booking site for one market, **Tel Aviv (TLV) ⇄ Bangkok (BKK)**, direct and with one connection through Dubai or Abu Dhabi. It keeps the hard parts: atomic seat holds over every flight of a trip under concurrency, a payment hold with a timeout, the transactional Outbox pattern into Kafka, role-based access with admin impersonation, and a load simulator that shows where the bottlenecks are.

The flights are real: they come from the [aviationstack](https://aviationstack.com) API, whose free plan allows only **100 requests a month**. So the lab never makes the same request twice (see below).

It is the sister project of `hotel-booking-lab` and uses the same stack, patterns, code layout and shared tables, so the two can later be merged into one travel lab.

Everything runs locally with Docker Compose or Podman Compose. Nothing is production-ready on purpose: passwords are plain text and shown on the login page so anyone can try every role.

## Stack

| Layer | Choice | Why it is here |
|---|---|---|
| API | Node.js 22, TypeScript, Fastify 5 | small, fast, typed REST API |
| Web | React 19, Vite 8, TypeScript | single-file SPA, inline SVG charts |
| Source of truth | PostgreSQL 16 | users, airports, airlines, flights, cabins, bookings, aviationstack request ledger, audit log, outbox |
| Seats / holds | Redis 7 + Lua | booked seats per departure only, atomic all-or-nothing hold over every leg of a trip; capped with `maxmemory` |
| Availability worker | Node.js 22, TypeScript | removes past departure days from Redis once a day |
| Events | Apache Kafka 3.9 (KRaft) + Kafka UI | domain events published from the outbox |
| Flight data | aviationstack `/v1/flights` | real flights, cached forever (ledger + committed snapshots) |
| Auth | JWT | roles CUSTOMER / SELLER (airline) / ADMIN, admin impersonation with audit trail |

## Screens at a glance

| Customer | Seller (airline) | Admin |
|---|---|---|
| Search with fare date strips (outbound and return), recent searches, USD / NIS / THB, booking with passengers, My bookings with payment countdown | Dashboard (seats and load factor), My flights, fare rules and 60-day fare calendar | Flight data (aviationstack budget and ledger), Redis records, concurrency demo, load simulation, country pricing and display currencies |

## Run it

```bash
cp .env.example .env        # optional: put your aviationstack key in it
podman compose up --build   # or: docker compose up --build
```

Without a key the lab still has real flights: the responses of every request made so far are committed in `data/aviationstack/`, and the import reads those first.

| What | Where |
|---|---|
| Web app | http://localhost:5174 |
| API | http://localhost:3020/api/health |
| Kafka UI | http://localhost:8081 |
| PostgreSQL | `localhost:5435`, user / password / db `booking` |
| Redis | `redis://localhost:6380` |

Ports are offset from the hotel lab (5173, 3010, 8080, 5433, 6379) so both can run at the same time. Kafka is not published on the host.

First steps: log in as admin (`admin@example.com` / `admin123`), open **Flight data** and press **Import** (free when everything is cached), then **Search → Generate Sample Data**. That creates two airline sellers (`seller@example.com`, `seller2@example.com`) and two customers (`customer@example.com`, `customer2@example.com`); every password is on the login page.

After changing code:

```bash
podman compose up --build -d --force-recreate --no-deps api availability-worker web
```

## aviationstack: 100 requests a month, never the same one twice

The free plan has real-time flights but no future schedules. So every operating flight seen today (codeshares folded into the operating flight) becomes a **daily schedule for the next 365 days**, with synthetic cabins: economy and business seats by aircraft type or flight length, base fares in baht from the flight time.

| Safeguard | How |
|---|---|
| Request ledger | `api_requests.request_key` is UNIQUE: endpoint + sorted parameters, never the key. A request that exists is answered from PostgreSQL. |
| Claim before calling | the row is inserted as `PENDING` in the same transaction as the budget check, under an advisory lock, *before* the HTTP call. Two concurrent identical requests cannot both go out. |
| Monthly budget | `AVIATIONSTACK_MONTHLY_BUDGET` (default 30 of the plan's 100). Every real HTTP call is a row in `api_calls`; above the budget nothing is called. |
| Snapshots | every successful response is also written to `data/aviationstack/<hash>.json` and committed. A new database, `compose down -v` or a fresh clone costs 0 requests. |
| No automatic retries | a failed or interrupted request stays `FAILED`; only the admin's **Retry** spends another call on it. The import stops at the first failed call. |
| Plan before spending | **Flight data** shows every planned request, which are cached, and how many would really be made; the admin confirms. |

The import plan is 11 requests: TLV→BKK and BKK→TLV direct, all TLV departures (to find the two busiest hubs with service to Bangkok), then TLV→hub, hub→BKK, BKK→hub and hub→TLV for each hub. The first run found **DXB and AUH** and built 55 real flights (El Al, flydubai, Emirates, Etihad, and others).

The key lives only in `.env` (gitignored) and in the API container's environment. It is never stored, logged or sent to the browser; error messages are redacted.

## What you can do

**As a customer**
- Search one way or round trip, direct or with one stop, by cabin and passenger count. The form shows the trip length ("14 days"), and shift buttons move the whole trip by a day, week or month.
- Two date strips show the cheapest fare per day for the 7 days around the departure and around the return. Click a day to move just that flight; return days before the departure are disabled.
- Each result shows every flight, the layover (airport and length), a Direct / 1 stop badge, seats left, and the price per passenger. Hovering the price shows its layers.
- Pick an outbound and a return, enter the passengers, and book. Seats on **every flight of the trip** are held at once (all or nothing) and the booking is *Awaiting payment*. Pay within 60 seconds in **My bookings** or the status becomes *Payment timed out* and the seats go back.
- **Recent searches**: your last 10 searches (route, dates, passengers, cabin, stops, and the cheapest fare at the time) sit under the search form; click one to run it again. Stored per account in Redis, so they follow you to another browser. A search is recorded once its results have stayed on screen for 2 seconds, and a repeated search moves to the top instead of appearing twice.
- Show prices in **baht, US dollars or shekels** (selector in the header, remembered per browser). Prices are always computed and charged in baht; other currencies are converted in the browser with fixed rates the admin sets under **Country pricing → Display currencies**, so no exchange-rate API is called.
- My bookings shows a live countdown, Pay and Cancel buttons, the trip phase (Upcoming / Travelling / Completed), and a warning when two trips overlap in time.

**As a seller (an airline)**
- **Dashboard**: seats booked vs free per day across your departures, and load factor, bookings and revenue per flight, for the next 7 / 14 / 30 days.
- **My flights**: your airlines' flights with cabins, seats, fares and the bookings on each.
- **Prices**: per airline, the base fare of every cabin, your own day-of-week %, and seasons, holidays and discounts for the whole airline, one flight or one cabin. A 60-day fare calendar shows every cabin's price with its layers.

**As an admin**
- **Flight data**: the aviationstack budget, import plan, request ledger and call log.
- Generate / delete sample data, create sample trips for a customer, rebuild the Redis seat cache from PostgreSQL.
- **Redis records** (every departure with the booked seats Redis holds, plus a Redis-vs-PostgreSQL check), **Availability log**, **Country pricing**.
- **Concurrency demo**: every customer books N seats on the same departure at the same instant; see exactly who wins.
- **Load simulation**:
  - *Random trips*: N customers book random TLV ⇄ BKK itineraries in the next 14 days; some pay, some let the hold expire and rebook, the rest abandon.
  - *Same flight*: everyone wants one departure. Rejected customers try other options on the same route that day, then the next day.

## How the booking flow works

1. `POST /api/flight-bookings` validates the trip:
   - legs connect (airport and 90 min – 12 h layover);
   - the return goes back from the destination to the origin;
   - the first departure is at least 2 hours away.
2. The trip is priced, then **one Lua script** checks every leg. Redis stores how many seats are booked per cabin and departure day; capacity comes from PostgreSQL. If any leg lacks enough seats for all passengers, it returns that leg and changes nothing. Otherwise it adds the passengers to every leg atomically.
3. Inside the booking transaction PostgreSQL has the last word:
   - it locks the cabins (in id order, so trips sharing flights cannot deadlock);
   - it sums the active passengers per departure;
   - if Redis was ever wrong, the booking is rejected instead of overbooked.
4. The booking is inserted as `PENDING` with `expires_at = now() + 60s`, together with its legs, its passengers, and `flight.booking.created` plus `flight.seats.changed` outbox rows, all in the same transaction.
5. `POST /api/flight-bookings/:id/pay` is a single conditional `UPDATE ... WHERE status='PENDING' AND expires_at > now()`.
6. A worker moves expired holds to `PAYMENT_TIMEOUT` (`FOR UPDATE SKIP LOCKED`) and gives every leg's seats back to Redis.
7. The outbox publisher sends events to Kafka. Topics:
   - `flight.booking.created`, `flight.booking.confirmed`, `flight.booking.payment_timeout`, `flight.booking.cancelled`
   - `flight.seats.changed`, `flight.data.imported`
8. Redis is treated as a cache. At startup, and from the admin panel, the booked counts are rebuilt from PostgreSQL. Until that has run once (key `fs:loaded`), bookings are refused rather than treating an empty Redis as "all free".

More detail in [ARCHITECTURE.md](ARCHITECTURE.md).

## API overview

| Method | Path | Role |
|---|---|---|
| GET | `/api/auth/demo-accounts`, `/api/stats`, `/api/airports`, `/api/currencies` | public |
| PUT | `/api/admin/currencies/:code` `{ thbPerUnit }` | admin, display rate (THB is the base) |
| POST | `/api/auth/login` | public |
| GET | `/api/flights/search?from=&to=&date=&passengers=&cabin=&stops=&sort=` | public (sellers see only their airlines) |
| GET | `/api/flights/calendar?from=&to=&date=&days=7` | public, cheapest fare per day |
| GET | `/api/flights/:scheduleId?date=` | public, cabins with seats left and prices |
| POST | `/api/flight-bookings` `{ legs: [{cabinId, date, direction}], passengers: [{firstName, lastName, passport?}] }` | customer, holds every leg |
| POST | `/api/flight-bookings/:id/pay`, `/api/flight-bookings/:id/cancel` | customer |
| GET | `/api/flight-bookings/me` | customer |
| GET / POST / DELETE | `/api/me/search-history` | any logged-in user, last 10 searches |
| GET | `/api/seller/dashboard?days=7`, `/api/seller/flights`, `/api/seller/flights/:id/bookings` | seller |
| GET | `/api/seller/airlines/:iata/prices?days=60` | seller |
| PATCH | `/api/seller/cabins/:id` `{ price }` | seller |
| PUT | `/api/seller/airlines/:iata/weekdays` `{ pct }` | seller |
| POST / DELETE | `/api/seller/airlines/:iata/price-rules`, `/api/seller/price-rules/:id` | seller |
| GET / PUT / POST / DELETE | `/api/admin/country-pricing…`, `/api/admin/price-rules/:id` | admin |
| GET | `/api/admin/aviationstack` | admin, budget + plan + ledger |
| POST | `/api/admin/aviationstack/import` `{ confirm: true }`, `/api/admin/aviationstack/retry` `{ key }` | admin |
| GET / POST | `/api/admin/users`, `/api/admin/flights`, `/api/admin/impersonate/:userId` | admin |
| POST / DELETE | `/api/admin/sample-data`, `POST /api/admin/sample-bookings` `{ userId }` | admin |
| POST | `/api/admin/concurrent-booking` `{ cabinId, date, passengers, confirm? }` | admin |
| POST | `/api/admin/simulation` `{ mode: random/same-flight, customers, passengers, payRatio, lateRatio, windowSeconds, cabinId?, date? }` | admin, then `GET /api/admin/simulation/:id`, `POST .../cancel-all` |
| POST | `/api/admin/rebuild-availability`; GET `/api/admin/redis-records`, `/api/admin/availability-log` | admin |

## Project layout

```
apps/api/src/server.ts                   Fastify API, aviationstack client, workers, simulation (single file on purpose)
apps/availability-worker/src/worker.ts   daily cleanup of past departure days in Redis
apps/web/src/main.tsx                    React SPA
apps/web/src/style.css
database/init.sql                        schema for a fresh database
data/aviationstack/                      cached aviationstack responses (committed; no key inside)
docker-compose.yml, .env.example
```

## Not production-ready, by design

Plain-text passwords, no rate limiting, no tests, one API instance, in-process workers, no real payment. Schedules are "today's flights, every day", and cabins and fares are synthetic.
