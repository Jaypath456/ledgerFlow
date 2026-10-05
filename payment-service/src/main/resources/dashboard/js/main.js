import * as api from './api.js';
import { esc, money, newKey, toMinor } from './format.js';
import { SCENARIOS } from './scenarios.js';
import * as store from './store.js';
import * as view from './views.js';

const $ = (sel, root = document) => root.querySelector(sel);
const running = new Set(); // single-flight guard per action

async function guarded(name, button, fn) {
  if (running.has(name)) return;
  running.add(name);
  if (button) button.disabled = true;
  try { return await fn(); } finally {
    running.delete(name);
    if (button) button.disabled = false;
  }
}

// ---------- tabs ----------
function showTab(name) {
  document.querySelectorAll('[role="tab"]').forEach((t) => t.setAttribute('aria-selected', String(t.dataset.tab === name)));
  document.querySelectorAll('[data-panel]').forEach((p) => { p.hidden = p.dataset.panel !== name; });
  if (name === 'activity') renderActivity();
  if (name === 'overview') refreshAccounts();
}
document.querySelectorAll('[role="tab"]').forEach((t) => t.addEventListener('click', () => showTab(t.dataset.tab)));
document.addEventListener('click', (e) => {
  const link = e.target.closest('[data-goto]');
  if (!link) return;
  e.preventDefault();
  showTab(link.dataset.goto);
  if (link.dataset.anchor) document.getElementById(link.dataset.anchor)?.scrollIntoView({ behavior: 'smooth' });
});

// ---------- health + invariants ----------
async function refreshHealth() {
  const [payment, ledger] = await Promise.all([api.health('payment'), api.health('ledger')]);
  for (const [name, status] of [['payment', payment], ['ledger', ledger]]) {
    const pill = $(`[data-health="${name}"]`);
    pill.className = `pill ${status === 'UP' ? 'up' : 'down'}`;
    $('b', pill).textContent = status === 'UP' ? 'UP' : 'DOWN';
  }
  $('#health-detail').innerHTML = `<dt>payment-service :8081</dt><dd>${view.badge(payment === 'UP' ? 'UP' : 'DOWN')} <span class="muted">${esc(payment)}</span></dd>
    <dt>ledger-service :8082</dt><dd>${view.badge(ledger === 'UP' ? 'UP' : 'DOWN')} <span class="muted">${esc(ledger)}</span></dd>`;
}

async function runInvariants() {
  const pill = $('[data-invariants]');
  try {
    const inv = await api.invariants();
    $('#invariant-table').innerHTML = view.invariantTable(inv);
    pill.className = `pill ${inv.pass ? 'pass' : 'fail'}`;
    $('b', pill).textContent = inv.pass ? 'PASS' : `${inv.violations} violations`;
  } catch (e) {
    $('#invariant-table').innerHTML = view.errorBox(e);
    pill.className = 'pill down';
    $('b', pill).textContent = 'unavailable';
  }
}
$('#run-invariants').addEventListener('click', (e) => guarded('invariants', e.target, runInvariants));

// ---------- accounts ----------
async function refreshAccounts() {
  const accounts = store.get('accounts', []);
  const list = $('#account-list');
  list.innerHTML = accounts.map((a) => `<option value="${a.id}">${esc(a.createdBy)} (${esc(a.type)})</option>`).join('');
  let balances = [];
  if (accounts.length) {
    try { balances = await api.balances(accounts.slice(0, 50).map((a) => a.id)); } catch { /* shown as – */ }
  }
  $('#accounts-table tbody').innerHTML = view.accountRows(accounts, balances);
}
$('#refresh-accounts').addEventListener('click', (e) => guarded('accounts', e.target, refreshAccounts));

// ---------- create payment ----------
const form = $('#pay-form');
let lastRequest = null;
form.key.value = newKey('ui');
$('#gen-key').addEventListener('click', () => { form.key.value = newKey('ui'); });

$('#make-accounts').addEventListener('click', (e) => guarded('make-accounts', e.target, async () => {
  try {
    const created = await api.createAccounts([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }]);
    store.trackAccounts(created.accounts, 'Create Payment');
    form.payer.value = created.accounts[0].id;
    form.payee.value = created.accounts[1].id;
    refreshAccounts();
  } catch (err) {
    $('#pay-response').innerHTML = view.errorBox(err);
  }
}));

function setFlow(state) {
  const steps = ['accepted', 'relay', 'ledger', 'result', 'terminal'];
  document.querySelectorAll('#pay-flow li').forEach((li) => { li.className = ''; });
  const mark = (step, cls) => { $(`#pay-flow [data-step="${step}"]`).className = cls; };
  if (state === 'PENDING_LEDGER') { mark('accepted', 'done'); ['relay', 'ledger', 'result'].forEach((s) => mark(s, 'active')); }
  if (state === 'COMPLETED' || state === 'FAILED') steps.forEach((s) => mark(s, state === 'FAILED' && s === 'terminal' ? 'failed' : 'done'));
  if (state === 'DECLINED') { mark('accepted', 'failed'); }
  $('#pay-flow [data-final]').textContent = ['COMPLETED', 'FAILED', 'DECLINED'].includes(state) ? state : '…';
}

async function send(key, body) {
  const out = $('#pay-response');
  setFlow(null);
  out.innerHTML = '<dt>Status</dt><dd>Sending…</dd>';
  let res;
  try {
    res = await api.postPayment(key, body);
  } catch (e) {
    out.innerHTML = view.errorBox(e);
    return;
  }
  const started = performance.now();
  const head = `<dt>HTTP</dt><dd>${res.status}${res.replayed ? ' · replayed original response' : ''}</dd><dt>Idempotency key</dt><dd class="mono">${esc(key)}</dd>`;
  if (res.status >= 400) {
    out.innerHTML = `${head}<dt>Result</dt><dd>${view.badge('Rejected', 'fail')} ${esc(res.data?.detail ?? res.text)}</dd>`;
    store.logActivity({ text: `Payment request rejected with HTTP ${res.status}: ${res.data?.detail ?? ''}` });
    return;
  }
  const id = res.data.id;
  let p = res.data;
  setFlow(p.status);
  const render = () => {
    out.innerHTML = `${head}<dt>Payment ID</dt><dd class="mono">${esc(id)}</dd><dt>Status</dt><dd>${view.badge(p.status)}</dd>
      ${p.declineReason ? `<dt>Reason</dt><dd>${esc(p.declineReason)}</dd>` : ''}<dt>Elapsed</dt><dd>${((performance.now() - started) / 1000).toFixed(2)} s</dd>`;
  };
  render();
  const deadline = Date.now() + 30000;
  while (p.status === 'PENDING_LEDGER' && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 250));
    try { p = await api.getPayment(id); } catch (e) { out.insertAdjacentHTML('beforeend', view.errorBox(e)); return; }
    render();
  }
  setFlow(p.status);
  if (p.status === 'PENDING_LEDGER') out.insertAdjacentHTML('beforeend', '<dt>Note</dt><dd>Still pending after 30 s; reconciliation re-requests stale payments after 30 s. Check again shortly.</dd>');
  store.set('lastPayment', p);
  store.logActivity({ text: `Payment ${id.slice(0, 8)}… ${money(p.amountMinor)} ${p.payerAccountId} → ${p.payeeAccountId}: ${p.status}${res.replayed ? ' (replayed)' : ''}` });
  renderLast();
  refreshAccounts();
}

form.addEventListener('submit', (e) => {
  e.preventDefault();
  const body = api.paymentBody(form.payer.value, form.payee.value, toMinor(form.amount.value));
  lastRequest = { key: form.key.value.trim(), body };
  $('#retry-payment').disabled = false;
  guarded('pay', $('#send-payment'), () => send(lastRequest.key, lastRequest.body));
});
$('#retry-payment').addEventListener('click', (e) => {
  if (lastRequest) guarded('pay', e.target, () => send(lastRequest.key, lastRequest.body));
});

// ---------- failure lab ----------
$('#scenarios').innerHTML = SCENARIOS.map(view.scenarioCard).join('');
document.querySelectorAll('[data-run]').forEach((button) => button.addEventListener('click', () => {
  const s = SCENARIOS.find((x) => x.id === button.dataset.run);
  const out = $(`[data-scenario="${s.id}"] .result`);
  guarded(`scenario:${s.id}`, button, async () => {
    out.hidden = false;
    out.innerHTML = `${view.badge('running', 'running')} <span class="muted">Running against the live services…</span>`;
    button.textContent = 'Running…';
    try {
      const result = await s.run();
      out.innerHTML = view.scenarioResult(result);
      store.set('lastScenario', { title: s.title, pass: result.pass, headline: result.headline, at: new Date().toISOString() });
      store.logActivity({ pass: result.pass, text: `Scenario ${s.letter} ${s.title}: ${result.headline.map(([k, v]) => `${k} ${v}`).join(', ')}` });
    } catch (e) {
      out.innerHTML = `<div class="verdict fail">FAIL</div>${view.errorBox(e)}`;
      store.set('lastScenario', { title: s.title, pass: false, headline: [['Error', e.message]], at: new Date().toISOString() });
      store.logActivity({ pass: false, text: `Scenario ${s.letter} ${s.title}: ${e.message}` });
    } finally {
      button.textContent = 'Run again';
      renderLast();
      runInvariants();
    }
  });
}));

const INFRA = [
  ['./demo/run.sh ledger-crash', 'kill -9 ledger-service mid-traffic, restart it, show every payment still settles exactly once'],
  ['./demo/run.sh kafka-restart', 'restart the Kafka broker under traffic; outbox relays resend, consumers dedupe'],
  ['./demo/run.sh postgres-pause', 'freeze Postgres for 10 s; requests stall, then everything drains'],
  ['./demo/run.sh invariants', 'full cross-schema I1–I6 check (chaos/verify_invariants.sql as superuser)'],
  ['chaos/run_chaos.sh 9', 'the chaos campaign: 9 fault scenarios under k6 load, invariant gate per run'],
];
$('#infra-commands').innerHTML = INFRA.map(([cmd, what]) => `<li><span><code>${esc(cmd)}</code><br><small class="muted">${esc(what)}</small></span>
  <button class="btn ghost" data-copy="${esc(cmd)}">Copy</button></li>`).join('');
$('#infra-commands').addEventListener('click', async (e) => {
  const b = e.target.closest('[data-copy]');
  if (!b) return;
  try { await navigator.clipboard.writeText(b.dataset.copy); b.textContent = 'Copied'; } catch { b.textContent = 'Select & copy'; }
  setTimeout(() => { b.textContent = 'Copy'; }, 1500);
});

// ---------- overview "last" + activity ----------
function renderLast() {
  const p = store.get('lastPayment');
  if (p) $('#last-payment').innerHTML = view.paymentSummary(p);
  const s = store.get('lastScenario');
  if (s) {
    $('#last-scenario').innerHTML = `<p>${view.badge(s.pass ? 'PASS' : 'FAIL', s.pass ? 'pass' : 'fail')} <b>${esc(s.title)}</b>
      <span class="muted">${esc(new Date(s.at).toLocaleTimeString())}</span></p>
      <dl class="kv">${s.headline.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;
  }
}
function renderActivity() {
  const items = store.get('activity', []);
  $('#activity').innerHTML = items.length ? items.map(view.activityEntry).join('') : '<p class="muted">Nothing yet.</p>';
}
$('#clear-activity').addEventListener('click', () => { store.set('activity', []); renderActivity(); });

// ---------- boot ----------
renderLast();
refreshHealth();
runInvariants();
refreshAccounts();
setInterval(refreshHealth, 5000);
