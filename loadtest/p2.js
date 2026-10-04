import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const authURL = __ENV.AUTH_URL || 'http://localhost:8084';
const duration = __ENV.DURATION || '90s';
const thinkTime = Number(__ENV.THINK_TIME || 0);

const unexpectedErrors = new Rate('unexpected_errors');
const controlledRejections = new Counter('controlled_rejections');
const authFailures = new Counter('auth_failures');

export const options = {
  scenarios: {
    bmkg_only: { executor: 'constant-vus', exec: 'bmkgOnly', vus: Number(__ENV.BMKG_VUS || 40), duration },
    volcanic: { executor: 'constant-vus', exec: 'volcanic', vus: Number(__ENV.VOLCANIC_VUS || 20), duration },
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  // Tagged submetrics only appear in the summary when a threshold names them.
  thresholds: {
    'http_req_duration{kind:bmkg}': ['p(95)<300'],
    'http_req_duration{kind:volcanic}': ['max>=0'],
    'http_reqs{kind:bmkg}': ['count>=0'],
    'http_reqs{kind:volcanic}': ['count>=0'],
    'unexpected_errors{kind:bmkg}': ['rate>=0'],
    'unexpected_errors{kind:volcanic}': ['rate>=0'],
    'controlled_rejections{kind:bmkg}': ['count>=0'],
    'controlled_rejections{kind:volcanic}': ['count>=0'],
    unexpected_errors: ['rate<0.01'],
  },
};

const json = { 'Content-Type': 'application/json' };
// Module scope is per VU, so each VU keeps its own Auth session.
let session = null;

function accessToken() {
  if (session && Date.now() < session.refreshAt) {
    return session.access;
  }
  const res = session
    ? http.post(`${authURL}/auth/refresh`, JSON.stringify({ refresh_token: session.refresh }), { headers: json, tags: { kind: 'auth' } })
    : http.post(`${authURL}/auth/login`, JSON.stringify({ client_id: __ENV.CLIENT_ID, client_secret: __ENV.CLIENT_SECRET }), { headers: json, tags: { kind: 'auth' } });
  if (res.status !== 200) {
    authFailures.add(1);
    session = null;
    return null;
  }
  const body = res.json();
  // Refresh at 75% of the TTL so a token never expires mid-request.
  session = { access: body.access_token, refresh: body.refresh_token, refreshAt: Date.now() + body.expires_in * 750 };
  return session.access;
}

function readHazards(kind, query) {
  const token = accessToken();
  if (!token) {
    unexpectedErrors.add(true, { kind });
    sleep(1);
    return;
  }
  const res = http.get(`${baseURL}/hazards?${query}`, {
    headers: { Authorization: `Bearer ${token}`, 'X-Correlation-ID': `k6-${kind}-${__VU}-${__ITER}` },
    tags: { kind, name: 'GET /hazards' },
  });
  if (res.status === 429) {
    controlledRejections.add(1, { kind });
  } else {
    const ok = res.status === 200 || res.status === 206;
    unexpectedErrors.add(!ok, { kind });
    check(res, { [`${kind} answered`]: () => ok });
  }
  sleep(thinkTime);
}

export function bmkgOnly() {
  readHazards('bmkg', 'source=BMKG&limit=20');
}

export function volcanic() {
  readHazards('volcanic', 'source=PVMBG&limit=20');
}

function metric(data, name, stat) {
  const m = data.metrics[name];
  return m && m.values[stat] !== undefined ? m.values[stat] : 0;
}

function row(data, kind) {
  const requests = metric(data, `http_reqs{kind:${kind}}`, 'count');
  const ms = (stat) => metric(data, `http_req_duration{kind:${kind}}`, stat).toFixed(1).padStart(8);
  return `${kind.padEnd(9)} ${String(requests).padStart(8)} ${(requests / (data.state.testRunDurationMs / 1000)).toFixed(1).padStart(8)} ${ms('med')} ${ms('p(95)')} ${ms('p(99)')} ${(metric(data, `unexpected_errors{kind:${kind}}`, 'rate') * 100).toFixed(2).padStart(8)}% ${String(metric(data, `controlled_rejections{kind:${kind}}`, 'count')).padStart(6)}`;
}

export function handleSummary(data) {
  const p95 = metric(data, 'http_req_duration{kind:bmkg}', 'p(95)');
  const errors = metric(data, 'unexpected_errors', 'rate');
  const lines = [
    `P2 load test, ${(data.state.testRunDurationMs / 1000).toFixed(0)}s, ${metric(data, 'vus_max', 'max')} VUs`,
    '',
    'kind      requests   req/s    p50 ms   p95 ms   p99 ms   errors     429',
    row(data, 'bmkg'),
    row(data, 'volcanic'),
    '',
    `BMKG-only p95 ${p95.toFixed(1)} ms, target below 300 ms: ${p95 < 300 ? 'PASS' : 'FAIL'}`,
    `Unexpected errors ${(errors * 100).toFixed(2)}%, target below 1%: ${errors < 0.01 ? 'PASS' : 'FAIL'}`,
    `Controlled 429 rejections ${metric(data, 'controlled_rejections', 'count')}, Auth failures ${metric(data, 'auth_failures', 'count')}`,
    '',
  ];
  const out = { stdout: lines.join('\n') };
  if (__ENV.SUMMARY_JSON) {
    out[__ENV.SUMMARY_JSON] = JSON.stringify(data, null, 2);
  }
  return out;
}
