import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';
const authURL = __ENV.AUTH_URL || 'http://localhost:8084';

// sustained proves P2.1 and P2.2. burst exceeds Client API capacity to show controlled 429 rejections.
const burst = __ENV.SCENARIO === 'burst';
const duration = __ENV.DURATION || (burst ? '20s' : '90s');
const thinkTime = Number(__ENV.THINK_TIME || 0);

const unexpectedErrors = new Rate('unexpected_errors');
const controlledRejections = new Counter('controlled_rejections');
const authFailures = new Counter('auth_failures');

const kinds = burst ? ['bmkg'] : ['bmkg', 'volcanic'];

// Tagged submetrics only appear in the summary when a threshold names them.
const thresholds = { unexpected_errors: ['rate<0.01'] };

for (const kind of kinds) {
  thresholds[`http_req_duration{kind:${kind}}`] = ['max>=0'];
  thresholds[`http_reqs{kind:${kind}}`] = ['count>=0'];
  thresholds[`unexpected_errors{kind:${kind}}`] = ['rate>=0'];
  thresholds[`controlled_rejections{kind:${kind}}`] = ['count>=0'];
}

if (burst) {
  thresholds.controlled_rejections = ['count>0'];
} else {
  thresholds['http_req_duration{kind:bmkg}'] = ['p(95)<300'];
}

function vus(exec, count) {
  return { executor: 'constant-vus', exec, vus: Number(count), duration };
}

export const options = {
  scenarios: burst
    ? { burst: vus('bmkgOnly', __ENV.BURST_VUS || 400) }
    : {
        bmkg_only: vus('bmkgOnly', __ENV.BMKG_VUS || 40),
        volcanic: vus('volcanic', __ENV.VOLCANIC_VUS || 20),
      },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds,
};

const authParams = { headers: { 'Content-Type': 'application/json' }, tags: { kind: 'auth' } };

// Module scope is per VU, so each VU keeps its own Auth session.
let session = null;

function accessToken() {
  if (session && Date.now() < session.refreshAt) {
    return session.access;
  }

  const res = session
    ? http.post(`${authURL}/auth/refresh`, JSON.stringify({ refresh_token: session.refresh }), authParams)
    : http.post(
        `${authURL}/auth/login`,
        JSON.stringify({ client_id: __ENV.CLIENT_ID, client_secret: __ENV.CLIENT_SECRET }),
        authParams,
      );

  if (res.status !== 200) {
    authFailures.add(1);
    session = null;

    return null;
  }

  const body = res.json();

  // Refresh at 75% of the TTL so a token never expires mid-request.
  session = {
    access: body.access_token,
    refresh: body.refresh_token,
    refreshAt: Date.now() + body.expires_in * 750,
  };

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
    const ok = res.status === 200;
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
  const seconds = data.state.testRunDurationMs / 1000;
  const requests = metric(data, `http_reqs{kind:${kind}}`, 'count');
  const errors = metric(data, `unexpected_errors{kind:${kind}}`, 'rate');
  const rejected = metric(data, `controlled_rejections{kind:${kind}}`, 'count');
  const ms = (stat) => metric(data, `http_req_duration{kind:${kind}}`, stat).toFixed(1).padStart(8);

  return [
    kind.padEnd(9),
    String(requests).padStart(8),
    (requests / seconds).toFixed(1).padStart(8),
    ms('med'),
    ms('p(95)'),
    ms('p(99)'),
    `${(errors * 100).toFixed(2).padStart(8)}%`,
    String(rejected).padStart(6),
  ].join(' ');
}

export function handleSummary(data) {
  const seconds = (data.state.testRunDurationMs / 1000).toFixed(0);
  const p95 = metric(data, 'http_req_duration{kind:bmkg}', 'p(95)');
  const errors = metric(data, 'unexpected_errors', 'rate');
  const rejected = metric(data, 'controlled_rejections', 'count');
  const verdict = (pass) => (pass ? 'PASS' : 'FAIL');

  const lines = [
    `P2 ${burst ? 'overload burst' : 'sustained load'} test, ${seconds}s, ${metric(data, 'vus_max', 'max')} VUs`,
    '',
    'kind      requests   req/s    p50 ms   p95 ms   p99 ms   errors     429',
    ...kinds.map((kind) => row(data, kind)),
    '',
    burst
      ? `Controlled 429 rejections ${rejected}, target above 0: ${verdict(rejected > 0)}`
      : `BMKG-only p95 ${p95.toFixed(1)} ms, target below 300 ms: ${verdict(p95 < 300)}`,
    `Unexpected errors ${(errors * 100).toFixed(2)}%, target below 1%: ${verdict(errors < 0.01)}`,
    `Controlled 429 rejections ${rejected}, Auth failures ${metric(data, 'auth_failures', 'count')}`,
    '',
  ];
  const text = lines.join('\n');

  const out = { stdout: text };
  if (__ENV.SUMMARY_TEXT) {
    out[__ENV.SUMMARY_TEXT] = text;
  }

  if (__ENV.SUMMARY_JSON) {
    out[__ENV.SUMMARY_JSON] = JSON.stringify(data, null, 2);
  }

  return out;
}
