#!/usr/bin/env bash
# Checks the running mocks against their contract, then resets PVMBG to schema 1 without outage.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a
source "$( [[ -f .env ]] && echo .env || echo .env.example )"
set +a
bmkg=${BMKG_URL:-http://localhost:${BMKG_PORT:-8081}}
pvmbg=${PVMBG_URL:-http://localhost:${PVMBG_PORT:-8082}}
failures=0

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; failures=$((failures + 1)); }
expect() { [[ "$2" == "$3" ]] && pass "$1" || fail "$1 (got $2, want $3)"; }
status() { curl -s -o /dev/null -w '%{http_code}' "$@" || true; }
seconds() { awk -v d="$1" 'BEGIN { if (d ~ /ms$/) print substr(d, 1, length(d) - 2) / 1000; else print substr(d, 1, length(d) - 1) }'; }
admin() { curl -s -o /dev/null -X POST "$pvmbg/admin/$1" -d "$2"; }
reset() { admin outage '{"enabled":false}'; admin schema-version '{"version":1}'; }
trap reset EXIT

bmkg_key=(-H "X-BMKG-Key: $BMKG_API_KEY")
pvmbg_token=(-H "Authorization: Bearer $PVMBG_TOKEN")
reset

expect "BMKG health" "$(status "$bmkg/health")" 200
expect "PVMBG health" "$(status "$pvmbg/health")" 200

for path in seismic-events tsunami-warnings; do
  expect "BMKG $path with BMKG key" "$(status "${bmkg_key[@]}" "$bmkg/$path")" 200
  expect "BMKG $path without key" "$(status "$bmkg/$path")" 401
  expect "BMKG $path with PVMBG bearer token" "$(status "${pvmbg_token[@]}" "$bmkg/$path")" 401
  expect "BMKG $path with PVMBG token as key" "$(status -H "X-BMKG-Key: $PVMBG_TOKEN" "$bmkg/$path")" 403
done
expect "PVMBG reports with PVMBG token" "$(status "${pvmbg_token[@]}" "$pvmbg/volcanic-reports")" 200
expect "PVMBG reports without token" "$(status "$pvmbg/volcanic-reports")" 401
expect "PVMBG reports with BMKG key as bearer" "$(status -H "Authorization: Bearer $BMKG_API_KEY" "$pvmbg/volcanic-reports")" 401
expect "PVMBG reports with wrong PVMBG token" "$(status -H "Authorization: Bearer pvmbg.smoke.wrong-secret-value" "$pvmbg/volcanic-reports")" 403

seeded=$(curl -s "${bmkg_key[@]}" "$bmkg/seismic-events" | grep -o '"event_id"' | wc -l)
(( seeded >= 20 )) && pass "BMKG serves at least 20 events ($seeded)" || fail "BMKG serves $seeded events"
reports=$(curl -s "${pvmbg_token[@]}" "$pvmbg/volcanic-reports" | grep -o '"report_id"' | wc -l)
(( reports >= 20 )) && pass "PVMBG serves at least 20 reports ($reports)" || fail "PVMBG serves $reports reports"
expect "BMKG future since is empty" "$(curl -s "${bmkg_key[@]}" "$bmkg/seismic-events?since=2999-01-01T00:00:00Z")" "[]"
expect "PVMBG future since is empty" "$(curl -s "${pvmbg_token[@]}" "$pvmbg/volcanic-reports?since=2999-01-01T00:00:00Z")" "[]"
expect "BMKG invalid since" "$(status "${bmkg_key[@]}" "$bmkg/seismic-events?since=yesterday")" 400
expect "PVMBG invalid since" "$(status "${pvmbg_token[@]}" "$pvmbg/volcanic-reports?since=yesterday")" 400
expect "Correlation ID echoed" "$(curl -s -o /dev/null -D - -H 'X-Correlation-ID: smoke-1' "$bmkg/health" | tr -d '\r' | grep -i '^x-correlation-id:' | cut -d' ' -f2)" smoke-1

bmkg_time=$(curl -s -o /dev/null -w '%{time_total}' "${bmkg_key[@]}" "$bmkg/seismic-events")
awk -v t="$bmkg_time" 'BEGIN { exit !(t >= 0.05 && t < 0.5) }' && pass "BMKG delay ${bmkg_time}s" || fail "BMKG delay ${bmkg_time}s outside 0.05s-0.5s"
min_delay=$(seconds "${PVMBG_MIN_RESPONSE_DELAY:-500ms}")
pvmbg_time=$(curl -s -o /dev/null -w '%{time_total}' "${pvmbg_token[@]}" "$pvmbg/volcanic-reports")
max_delay=$(seconds "${PVMBG_MAX_RESPONSE_DELAY:-3s}")
awk -v t="$pvmbg_time" -v min="$min_delay" -v max="$max_delay" 'BEGIN { exit !(t >= min && t < max + 0.5) }' && pass "PVMBG delay ${pvmbg_time}s" || fail "PVMBG delay ${pvmbg_time}s outside ${min_delay}s-${max_delay}s"

switched_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
admin schema-version '{"version":2}'
wait_for=$(awk -v s="$(seconds "${SOURCE_EVENT_INTERVAL:-10s}")" 'BEGIN { print s + 1 }')
echo "      waiting ${wait_for}s for new records"
sleep "$wait_for"
new_events=$(curl -s "${bmkg_key[@]}" "$bmkg/seismic-events?since=$switched_at" | grep -o '"event_id"' | wc -l)
(( new_events >= 1 )) && pass "BMKG generated $new_events new events" || fail "BMKG generated no event within ${wait_for}s"
curl -s "${pvmbg_token[@]}" "$pvmbg/volcanic-reports?since=$switched_at" | grep -q '"confidence_level"' && pass "New reports carry confidence_level" || fail "No confidence_level after schema change"
curl -s "${pvmbg_token[@]}" "$pvmbg/volcanic-reports?since=2026-09-29T00:00:00Z" | head -c 2000 | grep -q '"confidence_level"' && fail "Seed reports gained confidence_level" || pass "Seed reports keep the original shape"

admin outage '{"enabled":true,"mode":"error"}'
expect "Outage error mode" "$(status "${pvmbg_token[@]}" "$pvmbg/volcanic-reports")" 503
expect "Health during outage" "$(status "$pvmbg/health")" 503
admin outage '{"enabled":true,"mode":"hang"}'
hang_exit=0
curl -s -o /dev/null --max-time 2 "${pvmbg_token[@]}" "$pvmbg/volcanic-reports" || hang_exit=$?
expect "Outage hang mode times out" "$hang_exit" 28
admin outage '{"enabled":false}'
expect "Recovery without restart" "$(status "${pvmbg_token[@]}" "$pvmbg/volcanic-reports")" 200

echo
(( failures == 0 )) && echo "All source checks passed." || { echo "$failures source checks failed."; exit 1; }
