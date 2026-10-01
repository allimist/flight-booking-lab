#!/usr/bin/env bash
# End-to-end smoke test against a running stack (podman compose / docker compose up -d).
# It uses only the committed aviationstack snapshots and fails if a single real aviationstack call is made.
#   API=http://localhost:3020/api ./scripts/smoke.sh
set -euo pipefail
API=${API:-http://localhost:3020/api}
day() { date -u -d "+$1 days" +%F 2>/dev/null || date -u -v+"$1"d +%F; }   # GNU date (CI) or BSD date (macOS)
step() { echo "▶ $*"; }
ok() { echo "  ✓ $*"; }
fail() { echo "  ✕ $*" >&2; exit 1; }
call() { # method path token [json] -> sets BODY and STATUS (no subshell, so callers see both)
  local out
  if [ -n "${4:-}" ]; then out=$(curl -s -w '\n%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer ${3:-}" -H 'content-type: application/json' -d "$4")
  else out=$(curl -s -w '\n%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer ${3:-}"); fi
  STATUS=${out##*$'\n'}; BODY=${out%$'\n'*}
}
expect() { [ "$STATUS" = "$1" ] || fail "expected HTTP $1, got $STATUS: $BODY"; }
login() { call POST /auth/login "" "{\"email\":\"$1\",\"password\":\"$2\"}"; jq -r .token <<<"$BODY"; }

step "wait for the API"
for i in $(seq 1 60); do curl -sf "$API/health" >/dev/null && break; [ "$i" = 60 ] && fail "API not healthy"; sleep 3; done
ok "healthy"

step "flight data comes from the committed snapshots (0 aviationstack calls)"
ADMIN=$(login admin@example.com admin123); [ "$ADMIN" != null ] || fail "admin login"
call GET /admin/aviationstack "$ADMIN"; expect 200; PLAN=$BODY
[ "$(jq .requestsNeeded <<<"$PLAN")" = 0 ] || fail "import would need $(jq .requestsNeeded <<<"$PLAN") real requests"
call POST /admin/aviationstack/import "$ADMIN" '{"confirm":true}'; expect 200; IMPORT=$BODY
[ "$(jq '[.results[] | select(.called)] | length' <<<"$IMPORT")" = 0 ] || fail "the import called aviationstack"
ok "$(jq -r .message <<<"$IMPORT")"

step "sample data"
call POST /admin/sample-data "$ADMIN" '{}'; expect 200
call GET /stats ""; FLIGHTS=$(jq .realFlights <<<"$BODY"); [ "$FLIGHTS" -ge 10 ] || fail "only $FLIGHTS real flights"
ok "$FLIGHTS real flights"

step "search TLV -> BKK and pick a direct flight"
CUST=$(login customer@example.com customer123); D=$(day 30)
call GET "/flights/search?from=TLV&to=BKK&date=$D&stops=0" "$CUST"; expect 200; SEARCH=$BODY
CABIN=$(jq -r '.items[0].legs[0].cabinId' <<<"$SEARCH"); FLIGHT=$(jq -r '.items[0].legs[0].flightNumber' <<<"$SEARCH")
[ "$CABIN" != null ] || fail "no direct flight on $D"
ok "$FLIGHT on $D, fares: $(jq -c '[.items[0].fares[] | {code, perPassenger, available}]' <<<"$SEARCH")"

step "book a chosen seat at the Saver fare and pay"
call GET "/flights/seatmap?cabinId=$CABIN&date=$D" "$CUST"; expect 200; MAP=$BODY
SEAT=$(jq -r '. as $m | [range(0; $m.layout.total) | . as $i | "\($m.layout.firstRow + ($i / ($m.layout.letters|length) | floor))\($m.layout.letters[$i % ($m.layout.letters|length)])"] | map(select(. as $s | $m.taken | index($s) | not)) | .[0]' <<<"$MAP")
call POST /flight-bookings "$CUST" "{\"fareClass\":\"SAVER\",\"legs\":[{\"cabinId\":\"$CABIN\",\"date\":\"$D\",\"seats\":[\"$SEAT\"]}],\"passengers\":[{\"firstName\":\"Smoke\",\"lastName\":\"Test\"}]}"; expect 200; B=$BODY
[ "$(jq -r '.priceBreakdown[0].seats[0]' <<<"$B")" = "$SEAT" ] || fail "seat not assigned"
call POST "/flight-bookings/$(jq -r .bookingId <<<"$B")/pay" "$CUST";  expect 200
ok "seat $SEAT, Saver, $(jq .totalPrice <<<"$B") baht, paid"

step "the same seat cannot be sold twice"
CUST2=$(login customer2@example.com customer123)
call POST /flight-bookings "$CUST2" "{\"legs\":[{\"cabinId\":\"$CABIN\",\"date\":\"$D\",\"seats\":[\"$SEAT\"]}],\"passengers\":[{\"firstName\":\"Second\",\"lastName\":\"Try\"}]}"
expect 409; ok "$(jq -r .error <<<"$BODY")"

step "Flex refunds 100% on cancel"
call POST /flight-bookings "$CUST2" "{\"fareClass\":\"FLEX\",\"legs\":[{\"cabinId\":\"$CABIN\",\"date\":\"$D\"}],\"passengers\":[{\"firstName\":\"Flex\",\"lastName\":\"Fare\"}]}"; expect 200; B=$BODY
BID=$(jq -r .bookingId <<<"$B"); call POST "/flight-bookings/$BID/pay" "$CUST2";  expect 200
call POST "/flight-bookings/$BID/cancel" "$CUST2"; expect 200; C=$BODY
[ "$(jq .refundAmount <<<"$C")" = "$(jq .totalPrice <<<"$B")" ] || fail "refund $(jq .refundAmount <<<"$C") != paid $(jq .totalPrice <<<"$B")"
ok "refunded $(jq .refundAmount <<<"$C") baht"

step "Redis agrees with PostgreSQL"
call GET "/admin/redis-records?limit=1" "$ADMIN"; SUM=$(jq .summary <<<"$BODY")
[ "$(jq .mismatches <<<"$SUM")" = 0 ] && [ "$(jq .seatMismatches <<<"$SUM")" = 0 ] || fail "mismatch: $SUM"
ok "counts and seat bitmaps match on $(jq .checkedDepartures <<<"$SUM") booked departures"

step "still no aviationstack call"
call GET /admin/aviationstack "$ADMIN"; CALLS=$(jq .budget.allTime <<<"$BODY")
[ "$CALLS" = "${EXPECT_CALLS:-0}" ] || fail "aviationstack calls: $CALLS (expected ${EXPECT_CALLS:-0})"
ok "$CALLS calls"
echo "✓ smoke test passed"
