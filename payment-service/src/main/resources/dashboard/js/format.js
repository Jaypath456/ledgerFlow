const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });

export const money = (minor) => (minor === null || minor === undefined ? '–' : usd.format(Number(minor) / 100));
export const toMinor = (dollars) => Math.round(parseFloat(dollars) * 100);
export const seconds = (ms) => `${(ms / 1000).toFixed(2)} s`;
export const shortId = (id) => (id ? `${id.slice(0, 8)}…` : '–');

export function newKey(prefix = 'ui') {
  const rand = globalThis.crypto?.randomUUID?.() ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `${prefix}-${rand}`;
}

export function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Plain-English labels for internal codes (the raw code stays in "technical details").
const STATUS = { PENDING_LEDGER: 'Processing…', COMPLETED: 'Completed', FAILED: 'Failed', DECLINED: 'Declined' };
const REASON = {
  INSUFFICIENT_FUNDS: 'Insufficient funds',
  UNKNOWN_ACCOUNT: 'Account does not exist',
  INVALID_AMOUNT: 'Invalid amount',
  SAME_PAYER_PAYEE: 'Sender and receiver are the same account',
  AMOUNT_LIMIT: 'Over the $10,000 limit',
  BLOCKED_ACCOUNT: 'Blocked account',
  VELOCITY_LIMIT: 'Too many payments in a minute',
};
export const statusLabel = (s) => STATUS[s] ?? s;
export const reasonLabel = (r) => REASON[r] ?? r;
