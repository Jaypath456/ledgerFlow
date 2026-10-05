// Small per-browser history (accounts, last payment/scenario, activity). Purely a convenience:
// every value shown as a fact is re-read from the services.
const KEY = 'ledgerflow.demo.v1';

function load() {
  try { return JSON.parse(localStorage.getItem(KEY)) ?? {}; } catch { return {}; }
}
function save(state) {
  try { localStorage.setItem(KEY, JSON.stringify(state)); } catch { /* storage unavailable: keep going */ }
}

export function get(name, fallback) { return load()[name] ?? fallback; }
export function set(name, value) { const s = load(); s[name] = value; save(s); }

export function trackAccounts(accounts, createdBy) {
  const known = get('accounts', []);
  const added = accounts.map((a) => ({ id: a.id, type: a.type, createdBy }));
  set('accounts', [...added, ...known.filter((k) => !added.some((a) => a.id === k.id))].slice(0, 40));
}

export function logActivity(entry) {
  set('activity', [{ at: new Date().toISOString(), ...entry }, ...get('activity', [])].slice(0, 100));
}
