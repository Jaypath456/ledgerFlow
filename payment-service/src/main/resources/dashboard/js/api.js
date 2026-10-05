// HTTP access to the two LedgerFlow services. Distinguishes an API *rejection* (the app answered
// 4xx on purpose) from an *infrastructure* error (unreachable, timed out, or 5xx).
const PAYMENT = window.location.origin;
const LEDGER = `${window.location.protocol}//${window.location.hostname}:8082`;

export class ApiRejection extends Error {
  constructor(status, data, message) { super(message); this.status = status; this.data = data; }
}
export class InfraError extends Error {}

async function call(url, { method = 'GET', body, headers = {}, timeoutMs = 15000, raw = false } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let res;
  try {
    res = await fetch(url, {
      method,
      headers: { ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: ctrl.signal,
    });
  } catch (e) {
    throw new InfraError(e.name === 'AbortError'
      ? `Timed out after ${timeoutMs / 1000}s: ${method} ${url}`
      : `Cannot reach ${url}: is the stack running? (${e.message})`);
  } finally {
    clearTimeout(timer);
  }
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { /* not JSON */ }
  if (raw) {
    if (res.status >= 500) throw new InfraError(`${method} ${url} → HTTP ${res.status}${data?.detail ? `: ${data.detail}` : ''}`);
    return { status: res.status, text, data, replayed: res.headers.get('Idempotent-Replayed') === 'true' };
  }
  const detail = data?.detail ? `: ${data.detail}` : '';
  if (res.status >= 500) throw new InfraError(`${method} ${url} → HTTP ${res.status}${detail}`);
  if (res.status >= 400) throw new ApiRejection(res.status, data, `${method} ${url} → HTTP ${res.status}${detail}`);
  return data;
}

export async function health(service) {
  const base = service === 'ledger' ? LEDGER : PAYMENT;
  try {
    const r = await call(`${base}/actuator/health`, { raw: true, timeoutMs: 4000 });
    return r.data?.status ?? `HTTP ${r.status}`;
  } catch (e) {
    return e instanceof InfraError && /HTTP 503/.test(e.message) ? 'DOWN' : 'UNREACHABLE';
  }
}

export const paymentBody = (payer, payee, amountMinor) =>
  ({ payerAccountId: Number(payer), payeeAccountId: Number(payee), amountMinor: Number(amountMinor), currency: 'USD' });

/** POST /api/payments; returns {status, text, data, replayed} for any 2xx/4xx answer. */
export const postPayment = (key, body) =>
  call(`${PAYMENT}/api/payments`, { method: 'POST', body, headers: { 'Idempotency-Key': key }, raw: true });
export const getPayment = (id) => call(`${PAYMENT}/api/payments/${id}`);

// demo-profile endpoints
export const burst = (requests) => call(`${PAYMENT}/demo/burst`, { method: 'POST', body: requests, timeoutMs: 120000 });
export const statuses = (ids) => call(`${PAYMENT}/demo/payments/status`, { method: 'POST', body: ids });
export const riskConfig = () => call(`${PAYMENT}/demo/config`);
export const createAccounts = (specs) => call(`${LEDGER}/demo/accounts`, { method: 'POST', body: specs });
export const balances = (ids) => call(`${LEDGER}/demo/accounts?ids=${ids.join(',')}`);
export const ledgerFor = (paymentIds) => call(`${LEDGER}/demo/payments/ledger`, { method: 'POST', body: paymentIds });
export const deadlocks = () => call(`${LEDGER}/demo/deadlocks`);

/** Both services' invariant summaries (each can only see its own schema). */
export async function invariants() {
  const [ledger, payment] = await Promise.all([call(`${LEDGER}/demo/invariants`), call(`${PAYMENT}/demo/invariants`)]);
  return { ledger, payment, violations: ledger.violations + payment.violations, pass: ledger.pass && payment.pass };
}

// named demo accounts (ledger-service, demo profile)
export const namedAccounts = () => call(`${LEDGER}/demo/named-accounts`);
export const createNamedAccount = (name, balanceMinor) =>
  call(`${LEDGER}/demo/named-accounts`, { method: 'POST', body: { name, balanceMinor } });
/** Moves named accounts to target balances via ordinary balanced postings with their own treasury. */
export const setNamedBalances = (targets) => call(`${LEDGER}/demo/named-accounts/balances`, { method: 'POST', body: targets });

// transaction playground runner (payment-service, demo profile)
export const runTransactions = (specs) => call(`${PAYMENT}/demo/transactions`, { method: 'POST', body: specs, timeoutMs: 120000 });

// resilience controls (demo profile, application-level only)
export const ledgerControls = () => call(`${LEDGER}/demo/controls`);
export const setLedgerControls = (c) => call(`${LEDGER}/demo/controls`, { method: 'POST', body: c });
export const paymentControls = () => call(`${PAYMENT}/demo/controls`);
export const setPaymentControls = (c) => call(`${PAYMENT}/demo/controls`, { method: 'POST', body: c });
