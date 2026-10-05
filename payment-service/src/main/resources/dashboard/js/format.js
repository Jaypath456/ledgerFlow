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
