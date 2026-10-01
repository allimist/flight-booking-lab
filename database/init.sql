-- Shared with hotel-booking-lab (identical DDL, so the two labs can later share one database):
-- users, audit_logs, outbox_events, simulation_runs, country_pricing.
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('CUSTOMER','SELLER','ADMIN')),
  is_sample BOOLEAN NOT NULL DEFAULT FALSE,
  is_load_test BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY,
  actor_user_id UUID REFERENCES users(id),
  action TEXT NOT NULL,
  target_user_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY,
  topic TEXT NOT NULL,
  event_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS simulation_runs (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  report JSONB NOT NULL DEFAULT '{}'::jsonb,
  customer_ids UUID[] NOT NULL DEFAULT '{}',
  created_by UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

-- Day-of-week % per country, Sunday..Saturday. For flights the country is the departure airport's.
CREATE TABLE IF NOT EXISTS country_pricing (
  country TEXT PRIMARY KEY,
  weekday_pct NUMERIC(6,2)[] NOT NULL DEFAULT '{0,0,0,0,0,0,0}'
);

-- Display currencies. Prices are stored and charged in baht (THB); the UI converts with these fixed rates, which the
-- admin edits by hand, so showing USD or NIS never calls an exchange-rate API.
CREATE TABLE IF NOT EXISTS currency_rates (
  code TEXT PRIMARY KEY,                                       -- ISO 4217: THB, USD, ILS
  name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  thb_per_unit NUMERIC(12,4) NOT NULL CHECK (thb_per_unit > 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---- aviationstack: every distinct request is made at most once ---------------------------------------
-- api_requests is the cache: one row per request (endpoint + sorted params, never the access key). A row is
-- inserted before the HTTP call, so the same request can never go out twice, not even concurrently.
CREATE TABLE IF NOT EXISTS api_requests (
  id BIGSERIAL PRIMARY KEY,
  request_key TEXT UNIQUE NOT NULL,
  endpoint TEXT NOT NULL,
  params JSONB NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','OK','FAILED')),
  source TEXT NOT NULL CHECK (source IN ('API','SNAPSHOT')),
  http_status INT,
  error TEXT,
  response JSONB,
  rows INT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);
-- api_calls is the quota log: one row per real HTTP call (a retry of a failed request is a second call).
-- The monthly budget counts these rows.
CREATE TABLE IF NOT EXISTS api_calls (
  id BIGSERIAL PRIMARY KEY,
  request_key TEXT NOT NULL,
  called_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  http_status INT,
  ok BOOLEAN,
  duration_ms INT
);
CREATE INDEX IF NOT EXISTS api_calls_called_idx ON api_calls(called_at);

-- ---- Catalogue ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS airports (
  iata TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  country TEXT NOT NULL,
  timezone TEXT NOT NULL
);

-- An airline is what a hotel is in the hotel lab: owned by a seller, with its own day-of-week % (NULL = country).
CREATE TABLE IF NOT EXISTS airlines (
  iata TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  seller_id UUID REFERENCES users(id),
  weekday_pct NUMERIC(6,2)[]
);

-- One recurring flight: e.g. LY81 TLV 20:55 -> BKK 10:15 (+1 day), every day of the week.
-- Times are local at each airport; durations use the airports' timezones.
CREATE TABLE IF NOT EXISTS flight_schedules (
  id UUID PRIMARY KEY,
  airline_iata TEXT NOT NULL REFERENCES airlines(iata),
  flight_number TEXT NOT NULL,                                 -- e.g. LY81
  dep_iata TEXT NOT NULL REFERENCES airports(iata),
  arr_iata TEXT NOT NULL REFERENCES airports(iata),
  dep_time TIME NOT NULL,
  arr_time TIME NOT NULL,
  arr_day_offset INT NOT NULL DEFAULT 0,
  duration_min INT NOT NULL,
  days_of_week INT[] NOT NULL DEFAULT '{0,1,2,3,4,5,6}',        -- 0 = Sunday
  aircraft TEXT,
  source TEXT NOT NULL CHECK (source IN ('AVIATIONSTACK','SAMPLE')),
  source_request_key TEXT,
  is_sample BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (flight_number, dep_iata)
);

-- A cabin of a flight is what a room type is in the hotel lab: capacity + base price. A departure is (cabin, date).
CREATE TABLE IF NOT EXISTS flight_cabins (
  id UUID PRIMARY KEY,
  schedule_id UUID NOT NULL REFERENCES flight_schedules(id) ON DELETE CASCADE,
  cabin TEXT NOT NULL CHECK (cabin IN ('ECONOMY','BUSINESS')),
  total_seats INT NOT NULL CHECK (total_seats > 0),
  price NUMERIC(12,2) NOT NULL,
  UNIQUE (schedule_id, cabin)
);

-- Seasons, holidays and discounts for a country (admin) or an airline / one flight / one cabin (seller).
CREATE TABLE IF NOT EXISTS flight_price_rules (
  id UUID PRIMARY KEY,
  country TEXT,
  airline_iata TEXT REFERENCES airlines(iata) ON DELETE CASCADE,
  schedule_id UUID REFERENCES flight_schedules(id) ON DELETE CASCADE,
  cabin_id UUID REFERENCES flight_cabins(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('SEASON','HOLIDAY','DISCOUNT')),
  name TEXT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  adjust_type TEXT NOT NULL CHECK (adjust_type IN ('PERCENT','FIXED')),
  adjust_value NUMERIC(12,2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (end_date >= start_date),
  CONSTRAINT flight_price_rules_scope_check CHECK ((country IS NULL) <> (airline_iata IS NULL)
    AND (schedule_id IS NULL OR airline_iata IS NOT NULL) AND (cabin_id IS NULL OR schedule_id IS NOT NULL))
);

-- Nested booking classes inside every cabin: a class is on sale while the cabin's booked seats stay under cap_pct %.
CREATE TABLE IF NOT EXISTS flight_fare_classes (
  code TEXT PRIMARY KEY CHECK (code IN ('SAVER','STANDARD','FLEX')),
  name TEXT NOT NULL,
  cap_pct INT NOT NULL CHECK (cap_pct BETWEEN 1 AND 100),
  price_pct NUMERIC(6,2) NOT NULL,                             -- price layer, e.g. -20 for Saver
  refund_pct INT NOT NULL CHECK (refund_pct BETWEEN 0 AND 100),
  changeable BOOLEAN NOT NULL DEFAULT false,
  sort INT NOT NULL
);
INSERT INTO flight_fare_classes(code,name,cap_pct,price_pct,refund_pct,changeable,sort) VALUES
  ('SAVER','Saver',40,-20,0,false,1), ('STANDARD','Standard',85,0,50,false,2), ('FLEX','Flex',100,35,100,true,3)
ON CONFLICT (code) DO NOTHING;

-- ---- Bookings ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flight_bookings (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id),
  status TEXT NOT NULL CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','EXPIRED','PAYMENT_TIMEOUT')),
  trip_type TEXT NOT NULL CHECK (trip_type IN ('ONE_WAY','ROUND_TRIP')),
  passengers INT NOT NULL CHECK (passengers BETWEEN 1 AND 9),
  price NUMERIC(12,2) NOT NULL,                                -- total for every passenger and leg
  expires_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  price_breakdown JSONB,                                       -- per leg, per passenger, with layers, at booking time
  fare_class TEXT NOT NULL DEFAULT 'STANDARD',
  refund_amount NUMERIC(12,2),                                 -- set on cancel: paid price x the fare class refund %
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per flight of the trip (a round trip with a connection each way has 4). All legs are held together.
CREATE TABLE IF NOT EXISTS flight_booking_legs (
  booking_id UUID NOT NULL REFERENCES flight_bookings(id) ON DELETE CASCADE,
  seq INT NOT NULL,
  direction TEXT NOT NULL CHECK (direction IN ('OUTBOUND','RETURN')),
  cabin_id UUID NOT NULL REFERENCES flight_cabins(id),
  schedule_id UUID NOT NULL REFERENCES flight_schedules(id),
  dep_date DATE NOT NULL,                                      -- local date at the departure airport
  dep_at TIMESTAMPTZ NOT NULL,
  arr_at TIMESTAMPTZ NOT NULL,
  price NUMERIC(12,2) NOT NULL,                                -- per passenger
  fare_class TEXT NOT NULL DEFAULT 'STANDARD',
  PRIMARY KEY (booking_id, seq)
);

CREATE TABLE IF NOT EXISTS passengers (
  id UUID PRIMARY KEY,
  booking_id UUID NOT NULL REFERENCES flight_bookings(id) ON DELETE CASCADE,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  passport TEXT
);

-- Seat of each passenger on each leg. The partial unique index is PostgreSQL's guard: an active seat is taken once.
CREATE TABLE IF NOT EXISTS flight_seat_assignments (
  id BIGSERIAL PRIMARY KEY,
  booking_id UUID NOT NULL REFERENCES flight_bookings(id) ON DELETE CASCADE,
  passenger_id UUID NOT NULL REFERENCES passengers(id) ON DELETE CASCADE,
  cabin_id UUID NOT NULL REFERENCES flight_cabins(id) ON DELETE CASCADE,
  dep_date DATE NOT NULL,
  seat_index INT NOT NULL,                                     -- bit in the Redis seat bitmap
  seat TEXT NOT NULL,                                          -- e.g. 23A
  active BOOLEAN NOT NULL DEFAULT true                         -- false after cancel / payment timeout
);
CREATE UNIQUE INDEX IF NOT EXISTS seat_taken_once ON flight_seat_assignments(cabin_id, dep_date, seat_index) WHERE active;
CREATE INDEX IF NOT EXISTS seat_assignments_booking_idx ON flight_seat_assignments(booking_id);

CREATE INDEX IF NOT EXISTS schedules_route_idx ON flight_schedules(dep_iata, arr_iata);
CREATE INDEX IF NOT EXISTS cabins_schedule_idx ON flight_cabins(schedule_id);
CREATE INDEX IF NOT EXISTS flight_price_rules_airline_idx ON flight_price_rules(airline_iata);
CREATE INDEX IF NOT EXISTS flight_bookings_user_idx ON flight_bookings(user_id);
CREATE INDEX IF NOT EXISTS flight_bookings_pending_expiry_idx ON flight_bookings(expires_at) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS legs_cabin_date_idx ON flight_booking_legs(cabin_id, dep_date);
CREATE INDEX IF NOT EXISTS passengers_booking_idx ON passengers(booking_id);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox_events(published_at);

INSERT INTO users(id,email,password_hash,name,role,is_sample) VALUES ('00000000-0000-0000-0000-000000000001','admin@example.com','admin123','Local Admin','ADMIN',false) ON CONFLICT (email) DO NOTHING;
