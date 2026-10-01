# Architecture Notes

The same architecture as `hotel-booking-lab`. The mapping:

| Hotel lab | Flight lab |
|---|---|
| hotel (owned by a seller) | airline (owned by a seller) |
| room type: `rooms.total_rooms`, `rooms.price` | cabin of a flight: `flight_cabins.total_seats`, `flight_cabins.price` |
| night of a stay | departure day of a flight (local date at the departure airport) |
| a stay = consecutive nights of one room type | a trip = 1–4 legs on different flights (outbound and return, each direct or 1 stop) |
| 1 room per booking | 1–9 passengers per booking, each needs a seat on every leg |
| Redis `av:{roomId}:YYYY-MM` | Redis `fs:{cabinId}:YYYY-MM` |
| `bookings` | `flight_bookings` + `flight_booking_legs` + `passengers` |
| `price_rules` (country / hotel / room) | `flight_price_rules` (country / airline / flight / cabin) |
| topics `booking.*`, `room.availability.changed` | topics `flight.booking.*`, `flight.seats.changed`, `flight.data.imported` |

`users`, `audit_logs`, `outbox_events`, `simulation_runs` and `country_pricing` have the same DDL in both labs, so the two can share one database when merged. All flight tables and topics carry a `flight` prefix so nothing collides.

## PostgreSQL
Source of truth for users, airports, airlines, flight schedules, cabins, bookings, passengers, the aviationstack request ledger, audit logs and outbox events.

A **schedule** is a recurring flight: `LY81 TLV 22:20 → BKK 14:00 (+1)`, flown on `days_of_week` (all seven by default). Times are local wall-clock times at each airport. Instants are computed with the airports' IANA timezones through `Intl`, so DST is handled.

Departures are **not** materialized. A departure is (cabin, date), exactly like room type × night. 55 flights × 2 cabins × 365 days would be 40,000 rows that say nothing until someone books.

## aviationstack (flight data)
The free plan allows 100 requests a month and serves real-time flights only (no future schedules, no routes). The lab uses it once, to seed a realistic catalogue.

- **Request key**: endpoint + parameters sorted by name, e.g. `flights?arr_iata=BKK&dep_iata=TLV&limit=100`. The access key is never part of it.
- **`api_requests`**: one row per request key (UNIQUE), with status `PENDING` / `OK` / `FAILED`, source `API` / `SNAPSHOT`, and the full JSON response. A key that is `OK` is served from here forever.
- **`api_calls`**: one row per real HTTP call. The monthly budget (`AVIATIONSTACK_MONTHLY_BUDGET`, default 30) counts these rows, so a retried failure costs twice, as it does at aviationstack.
- **Claim, then call**: in one transaction under `pg_advisory_xact_lock`, check the budget, insert or re-claim the request row as `PENDING`, and insert the `api_calls` row; only after COMMIT does the HTTP call go out. So:
  - two identical requests cannot both be sent;
  - two different requests cannot both take the last unit of budget.
- **Crash safety**: a crash mid-call leaves `PENDING`; at startup it becomes `FAILED` ("API restarted during the call"). Nothing is ever retried automatically: the admin decides whether to spend a request on it.
- **Snapshots**: every `OK` response is written to `data/aviationstack/<sha1(key)[0:16]>.json` (`requestKey`, `endpoint`, `params`, `fetchedAt`, `response`). At startup and before any call, snapshot files are loaded into the ledger as `SNAPSHOT` rows. A fresh database therefore rebuilds the catalogue for free, and so does a clone without a key.
- **Import plan** (11 requests):
  1. TLV→BKK and BKK→TLV direct.
  2. All TLV departures: the two busiest airports among a list of hubs with Bangkok service (DXB, AUH, DEL, ADD, AMM, …) become the hubs.
  3. Four requests per hub.

  The plan, with the cached / uncached state of every step, is shown before anything is spent. The import stops at the first failed call, since a bad key or a refused HTTPS call would fail the same way on every step.
- **Parsing**:
  - Codeshares (`flight.codeshared` set) are dropped in favour of the operating flight.
  - `departure.scheduled` / `arrival.scheduled` carry local wall-clock time despite their `+00:00` suffix (verified: TLV→DXB comes out at 3h25, TLV→BKK at 11h40). They are converted with the airport timezone, with a fallback to reading them as UTC if that gives an impossible duration.
  - Only flights of the TLV ⇄ BKK network (direct, and to/from the chosen hubs) are kept.
  - Real flights replace the synthetic sample flights unless those have bookings.
- **Cabins**: seats by aircraft type (the free data rarely has it), else by flight length: 250 + 30 business seats above 6 hours, 160 + 12 below. Economy base fare = ฿1,500 + ฿38 per minute of flight; business ×3.2.

## Redis
Redis holds only **booked seats**. For each cabin and month there is one small hash, `fs:{<cabinId>}:<YYYY-MM>`. It has a field for each departure day that has bookings or payment holds; the field is the day of the month and the value is the seats booked. No field means every seat is free. Capacity (`flight_cabins.total_seats`) stays in PostgreSQL and is passed to the scripts. Memory grows with bookings, not with the catalogue. Every month key gets `EXPIREAT` two days after its month ends.

**Hold** runs one Lua script over **every leg of the trip**:

- **Arguments**: `KEYS` is `fs:loaded` plus the month keys. `ARGV` is the passenger count, the leg count, then for each leg (key index, day field, total seats), then each key's expiry.
- **Check first**: if any leg has `booked + passengers > total`, the script returns `{-1, leg}` and changes nothing.
- **Then write**: otherwise it adds the passengers to every leg and returns `{0, fewest seats left}`.

A round trip with a connection each way is four legs held atomically, in about 1 ms. **Release** (payment timeout, cancel) runs the inverse on the legs not yet departed. A day that reaches 0 is deleted, which keeps the hashes sparse.

**Cluster note.** Unlike the hotel lab's single-room stays, a trip touches several cabins, whose keys hash to different Redis Cluster slots. A multi-key script needs all keys in one slot, so on a cluster this would need either keys hash-tagged by route or a two-phase hold (reserve per leg, compensate on failure). On the single Redis used here the script is atomic as written.

**Loaded check.** An empty Redis would look fully available, so both scripts first check `fs:loaded`, which only the rebuild writes. Without it they return -2 and the API answers "not loaded yet".

**Rebuild.** At API startup, and via `POST /api/admin/rebuild-availability`, Redis is rebuilt from PostgreSQL:
- the booked seats of every departure with active bookings are written with `HSET` (which only overwrites, so a booking running at that moment never sees its day vanish);
- days no booking backs any more are dropped, as are keys of deleted cabins;
- `fs:loaded` is set.

**PostgreSQL guard.** Redis turns away almost every sold-out request, but PostgreSQL has the last word:
- inside the booking transaction the API locks every cabin of the trip (`SELECT … FOR UPDATE ORDER BY id`, a stable order so two trips sharing flights cannot deadlock);
- it sums the active passengers on each departure;
- if any leg would exceed its seats it rolls back, gives the Redis hold back and answers "sold out".

### Availability cleanup (`apps/availability-worker`)
Runs at startup and just after every UTC midnight. It removes departure days that are already past from the current and previous month's keys. It never adds anything. Every run is written to `availability_window_log` and shown in the admin **Availability log** tab.

## Fare classes
Nested booking classes, as airlines sell them. Every cabin is shared by three classes in `flight_fare_classes`:

| Class | `cap_pct` | `price_pct` | `refund_pct` |
|---|---|---|---|
| SAVER | 40 | −20 | 0 |
| STANDARD | 85 | 0 | 50 |
| FLEX | 100 | +35 | 100 |

A class is on sale while `booked + passengers <= floor(total_seats × cap_pct / 100)`. So the cheap seats run out first and Flex can sell the last seat. The atomic hold needs nothing new: the Lua script receives the cap instead of the total, and the PostgreSQL guard counts against the same cap.

The class is one price layer (`FARE_CLASS`, applied after demand and booking time, before discounts). It is stored on the booking and on each leg. Cancelling a paid booking refunds `Σ leg price × passengers × refund_pct` (`refund_amount`). Search prices every itinerary in every class and shows the cheapest one still on sale. A trip is sold out only when Flex is.

## Seat maps
Layouts are derived, not stored:
- business is 2-2 (A C D F) from row 1;
- economy is 3-3 (A–F) up to 180 seats, else 3-3-3 (A–K without I), numbered on after the business rows.

A seat's index in the cabin (row-major) is its bit in Redis.

- **Redis**: `fsm:{cabinId}:YYYY-MM-DD` is a bitmap per departure (1 = taken). It shares the `{cabinId}` hash tag with the count hash, so both live in one cluster slot, and expires 2 days after the departure.
  - The hold script checks every chosen seat with `GETBIT`, auto-assigns the first free seats to passengers without a choice, then sets the bits together with `HINCRBY`, all in the same `EVAL`.
  - Release clears the bits.
- **PostgreSQL**: `flight_seat_assignments(booking_id, passenger_id, cabin_id, dep_date, seat_index, seat, active)`.
  - The partial unique index `(cabin_id, dep_date, seat_index) WHERE active` is the database guard: an active seat exists once, even if Redis is wrong.
  - Cancel and payment timeout set `active = false`.
- **Invariant**: for every booked departure, `BITCOUNT` of its bitmap = the seats booked. **Redis records** checks it next to the count check, and `rebuildAvailability()` rebuilds the bitmaps from the active assignments. On upgrade, the API gives active bookings made before seat maps the first free seats, so the invariant holds from the first start.

## Search
`GET /api/flights/search` is one-way; a round trip is two searches whose chosen itineraries are booked together.

- **Direct**: flights from → to that fly on that weekday.
- **1 stop**: a flight from the origin to a hub, plus a flight from that hub to the destination on the same or the next day. The layover must be 90 min – 12 h, measured between UTC instants, so hubs in other timezones are right.
- **Pruning**: of several feeders into the same onward flight, only the one with the shortest layover is kept. Otherwise every Tel Aviv–Dubai flight of the day would pair with the same Dubai–Bangkok flight.
- **Pricing and seats**: all legs of all results get their booked seats in one Redis pipeline, then are priced; sold-out results sort last.

## Pricing
A seat's price on one departure is built in layers:

1. **Base**: the cabin's `flight_cabins.price`.
2. **Holiday** (`HOLIDAY`): a fixed price or base +/- %. It replaces steps 3 and 4.
3. **Season** (`SEASON`): a fixed price or base +/- %.
4. **Day of week**: the airline's own value (`airlines.weekday_pct`), else the departure country's (`country_pricing`), the same defaults as the hotel lab (Israel Mon −15%, Thu +30%, Fri +40%, Sat +10%).
5. **Demand**: at least 50% of seats sold +15%, at least 80% +40%, from the Redis count at quote time.
6. **Booking time**: fewer than 7 days before departure +30%, 60 days or more −10%.
7. **Discount** (`DISCOUNT`, airline / flight / cabin only): minus the single largest active discount %.

Rules exist at four levels:
- country (admin, by departure country);
- airline, flight and cabin (seller).

Inside one layer the most specific rule wins, then the newest.

A trip's price is the sum of its legs, times the passengers. `priceSeat()` is the only place prices are computed; search, the date strip, the fare calendar and booking all use it. The booking stores the total and every leg's price with its layers in `price_breakdown`, so later changes never alter an existing booking. Prices are whole baht, like the hotel lab.

## Recent searches
Each user's last 10 searches live in one Redis list, `sh:{userId}` (about 2 KB per user; the key expires 90 days after the last search). An entry is JSON with the search parameters, a `key` built from them, the cheapest fare seen (baht) and the time.

A small Lua script keeps the list clean, atomically: remove any entry with the same `key` (Redis ships `cjson`), `LPUSH` the new one, `LTRIM` to 10, `EXPIRE`. A repeated search therefore moves to the top instead of appearing twice. Search re-runs on every field change, so the browser records a search only after its results have stayed on screen for 2 seconds. History is a convenience and deliberately lives only in Redis: losing it costs nothing, so PostgreSQL does not keep it.

## Display currencies
Everything is priced, stored and charged in baht, as in the hotel lab. `currency_rates` (code, symbol, `thb_per_unit`; seeded THB 1, USD 33, ILS 8.9) holds fixed rates that the admin edits by hand. The browser loads them once (`GET /api/currencies`) and every price goes through one formatter (`money()` in `main.tsx`), so switching currency re-renders without any request. No exchange-rate API is called. Where a converted amount is shown next to a payment, the charged baht amount is shown too.

## Bookings and the payment hold
- **What is stored**: `flight_bookings` holds the status, trip type, passenger count, total, `expires_at` and `paid_at`. `flight_booking_legs` holds one row per flight: cabin, local departure date, UTC departure and arrival, price per passenger. `passengers` holds the names.
- **Payment**: pay is a single conditional `UPDATE`, so a late payment and the expiry worker cannot both win.
- **Expiry**: the worker runs every 2 s. It moves due holds to `PAYMENT_TIMEOUT` (`FOR UPDATE SKIP LOCKED`, so several API instances could run it) and releases every leg.
- **Cancelling**: customers can cancel until the first flight departs.

## Kafka
The API starts and takes bookings even while Kafka is down: the publisher connects (and reconnects) inside its own loop, and events wait in `outbox_events` until Kafka is back. Kafka has `restart: unless-stopped`, so it comes back if the VM kills it for memory. Domain events are written to `outbox_events` in the same transaction as the change they describe. A background publisher sends unpublished events to Kafka and stamps `published_at`. Event keys are the booking id, or the cabin id for `flight.seats.changed`, so each departure's seat changes stay ordered on one partition.

## Load simulation
Same design as the hotel lab. N throw-away customers (`sim-<run>-<n>@example.com`) act at one instant; each step is timed (avg / p50 / p95 / max) and ranked by p95. The run is stored in `simulation_runs`.

- *Random trips* books random TLV ⇄ BKK itineraries in the next 14 days, with pay / late / abandon shares.
- *Same flight* sends everyone at one departure. Rejected customers cascade to other itineraries that day, then the next day.

## CI
`.github/workflows/ci.yml` has two jobs:
- typecheck and build each app (API, worker, web);
- start the compose stack (without Kafka UI or web) and run `scripts/smoke.sh`.

The runner has **no aviationstack key**: the import must be served entirely from the committed snapshots, and the test asserts that zero real calls were made.

## Next learning upgrades
- Cluster-safe multi-leg holds (hash-tagged keys or a saga)
- Merge with hotel-booking-lab: one catalogue, flight + hotel packages
- Batch Kafka sends and LISTEN/NOTIFY instead of polling (the hotel lab's measured bottleneck)
