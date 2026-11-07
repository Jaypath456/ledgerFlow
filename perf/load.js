// Phase 3 load: MODE=ramp (stair-step ramping-arrival-rate until saturation) or MODE=steady (constant-arrival-rate).
// Spreads payments over many payers (200 seeded customers, one credential each) and 20 merchants, like chaos/load.js, but
// NO client retries: a failed request is a failed request, so saturation shows up as errors, drops and latency.
// Every Idempotency-Key is  perf-<RUN_ID>-s<step>-<vu>-<iter>-<random>  so perf/report.py can split request-to-terminal latency per step in SQL.
// Env: MODE, RUN_ID (no '-'), BASE_URL, LOAD_CREDENTIALS (JSON account->password, from chaos/lib.sh), OUT (summary json path)
//   ramp:   START_RATE=25 STEP_RATE=25 STEP_SECONDS=30 MAX_RATE=600      (step n offers START_RATE + (n-1)*STEP_RATE payments/s)
//   steady: RATE=100 WARMUP_SECONDS=60 DURATION_SECONDS=180              (step 0 = warm-up, step 1 = measured window)
import http from 'k6/http';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { Counter, Gauge, Trend } from 'k6/metrics';

const MODE = __ENV.MODE || 'steady';
const RUN_ID = __ENV.RUN_ID || 'r0';
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const PAYERS_FIRST = 10001, PAYERS = 200, PAYEES_FIRST = 20001, PAYEES = 20;

const CREDENTIALS = JSON.parse(__ENV.LOAD_CREDENTIALS || '{}');
const AUTHORIZATION = {};
for (let p = PAYERS_FIRST; p < PAYERS_FIRST + PAYERS; p++) {
  if (!CREDENTIALS[p]) throw new Error(`no load credential for payer ${p}; source chaos/lib.sh`);
  AUTHORIZATION[p] = `Basic ${encoding.b64encode(`payer-${p}:${CREDENTIALS[p]}`)}`;
}

const START_RATE = Number(__ENV.START_RATE || 25), STEP_RATE = Number(__ENV.STEP_RATE || 25);
const STEP_SECONDS = Number(__ENV.STEP_SECONDS || 30), MAX_RATE = Number(__ENV.MAX_RATE || 600);
const RATE = Number(__ENV.RATE || 100), WARMUP = Number(__ENV.WARMUP_SECONDS || 60), DURATION = Number(__ENV.DURATION_SECONDS || 180);
const STEPS = MODE === 'ramp' ? Math.floor((MAX_RATE - START_RATE) / STEP_RATE) + 1 : 1;
const stepIds = MODE === 'ramp' ? Array.from({ length: STEPS }, (_, i) => i + 1) : [0, 1];

const accepted = new Counter('payments_accepted'), declined = new Counter('payments_declined');
const err4xx = new Counter('payments_http_4xx'), err5xx = new Counter('payments_http_5xx'), err0 = new Counter('payments_no_response');
const postLatency = new Trend('post_latency', true);
const scenarioStart = new Gauge('scenario_start_ms');

const thresholds = {};
for (const s of stepIds) {   // empty thresholds only force k6 to keep the per-step sub-metrics for the summary
  thresholds[`post_latency{step:${s}}`] = ['max>=0'];
  for (const c of ['payments_accepted', 'payments_declined', 'payments_http_4xx', 'payments_http_5xx', 'payments_no_response']) thresholds[`${c}{step:${s}}`] = ['count>=0'];
}

const common = { preAllocatedVUs: 300, maxVUs: 3000, timeUnit: '1s' };
export const options = {
  scenarios: { payments: MODE === 'ramp'
    ? { executor: 'ramping-arrival-rate', startRate: START_RATE, ...common,
        stages: stepIds.flatMap((s) => { const r = START_RATE + (s - 1) * STEP_RATE; return [{ target: r, duration: '1s' }, { target: r, duration: `${STEP_SECONDS - 1}s` }]; }) }
    : { executor: 'constant-arrival-rate', rate: RATE, duration: `${WARMUP + DURATION}s`, ...common } },
  thresholds,
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

function stepNow() {
  const elapsed = Date.now() - exec.scenario.startTime;
  return MODE === 'ramp' ? Math.min(STEPS, 1 + Math.floor(elapsed / (STEP_SECONDS * 1000))) : (elapsed < WARMUP * 1000 ? 0 : 1);
}

export default function () {
  const payer = PAYERS_FIRST + Math.floor(Math.random() * PAYERS);
  const payee = PAYEES_FIRST + Math.floor(Math.random() * PAYEES);
  const step = stepNow();
  scenarioStart.add(exec.scenario.startTime);
  const key = `perf-${RUN_ID}-s${step}-${__VU}-${__ITER}-${Math.floor(Math.random() * 1e9)}`;
  const body = JSON.stringify({ payerAccountId: payer, payeeAccountId: payee, amountMinor: 100 + Math.floor(Math.random() * 900), currency: 'USD' });
  const res = http.post(`${BASE_URL}/api/payments`, body, { timeout: '30s', tags: { step: String(step) },
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key, Authorization: AUTHORIZATION[payer] } });
  const t = { step: String(step) };
  if (res.status === 202) { accepted.add(1, t); postLatency.add(res.timings.duration, t); }
  else if (res.status === 201) { declined.add(1, t); postLatency.add(res.timings.duration, t); }
  else if (res.status === 0) err0.add(1, t);
  else if (res.status >= 500) { err5xx.add(1, t); postLatency.add(res.timings.duration, t); }
  else err4xx.add(1, t);
}

export function handleSummary(data) {
  const m = data.metrics, out = [];
  const v = (name, stat, d = 0) => (m[name] && m[name].values[stat] !== undefined ? m[name].values[stat] : d);
  for (const s of stepIds) {
    const t = (n) => `${n}{step:${s}}`;
    out.push({ step: s, offered: MODE === 'ramp' ? START_RATE + (s - 1) * STEP_RATE : RATE,
      seconds: MODE === 'ramp' ? STEP_SECONDS : (s === 0 ? WARMUP : DURATION),
      accepted: v(t('payments_accepted'), 'count'), declined: v(t('payments_declined'), 'count'), http_4xx: v(t('payments_http_4xx'), 'count'),
      http_5xx: v(t('payments_http_5xx'), 'count'), no_response: v(t('payments_no_response'), 'count'),
      p50: v(t('post_latency'), 'p(50)'), p95: v(t('post_latency'), 'p(95)'), p99: v(t('post_latency'), 'p(99)'), max: v(t('post_latency'), 'max') });
  }
  const summary = { mode: MODE, run_id: RUN_ID, scenario_start_ms: v('scenario_start_ms', 'value'), dropped_iterations: v('dropped_iterations', 'count'),
    vus_max: v('vus_max', 'max'), steps: out };
  return { [__ENV.OUT || 'summary.json']: JSON.stringify(summary, null, 1) };
}
