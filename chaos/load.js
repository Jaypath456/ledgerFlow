// k6 load: random funded payer -> random merchant, unique Idempotency-Key per payment.
// A failed attempt (connection error / 5xx) is retried with the SAME key, like a real client.
// Env: BASE_URL, RATE (payments/s), DURATION, PAYERS_FIRST, PAYERS, PAYEES_FIRST, PAYEES, OUT
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const PAYERS_FIRST = Number(__ENV.PAYERS_FIRST || 10001);
const PAYERS = Number(__ENV.PAYERS || 200);
const PAYEES_FIRST = Number(__ENV.PAYEES_FIRST || 20001);
const PAYEES = Number(__ENV.PAYEES || 20);

const accepted = new Counter('payments_accepted');
const declined = new Counter('payments_declined');
const failed = new Counter('payments_failed_after_retries');
const retries = new Counter('payment_retries');
const postLatency = new Trend('post_latency', true);

export const options = {
  scenarios: {
    payments: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 50),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const payer = PAYERS_FIRST + Math.floor(Math.random() * PAYERS);
  const payee = PAYEES_FIRST + Math.floor(Math.random() * PAYEES);
  const key = `k6-${__VU}-${__ITER}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  const body = JSON.stringify({ payerAccountId: payer, payeeAccountId: payee, amountMinor: 100 + Math.floor(Math.random() * 900), currency: 'USD' });
  const params = { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, timeout: '15s' };

  for (let attempt = 1; attempt <= 5; attempt++) {
    const res = http.post(`${BASE_URL}/api/payments`, body, params);
    if (res.status === 202 || res.status === 201) {
      if (attempt === 1) postLatency.add(res.timings.duration); // client-observed latency of first attempts
      (res.status === 202 ? accepted : declined).add(1);
      return;
    }
    if (res.status !== 0 && res.status < 500) break; // 4xx: not retryable
    retries.add(1);
    sleep(1);
  }
  failed.add(1);
}

export function handleSummary(data) {
  const out = __ENV.OUT || '/out/summary.json';
  const m = data.metrics;
  const v = (name, stat) => (m[name] && m[name].values[stat] !== undefined ? m[name].values[stat] : 0);
  const line = `accepted=${v('payments_accepted', 'count')} declined=${v('payments_declined', 'count')} `
    + `failed=${v('payments_failed_after_retries', 'count')} retries=${v('payment_retries', 'count')} `
    + `rate=${v('iterations', 'rate').toFixed(1)}/s post_p50=${v('post_latency', 'p(50)').toFixed(1)}ms `
    + `p95=${v('post_latency', 'p(95)').toFixed(1)}ms p99=${v('post_latency', 'p(99)').toFixed(1)}ms`;
  return { stdout: line + '\n', [out]: JSON.stringify(data) };
}
