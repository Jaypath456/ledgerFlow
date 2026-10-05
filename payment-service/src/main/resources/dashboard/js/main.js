import * as api from './api.js';
import { esc, money, newKey, reasonLabel, statusLabel, toMinor } from './format.js';
import { PRESETS, runPlayground, timeline } from './playground.js';
import { EXPERIMENTS, resetFaults } from './resilience.js';
import { SCENARIOS } from './scenarios.js';
import * as view from './views.js';

const $ = (sel, root = document) => root.querySelector(sel);
const running = new Set(); // single-flight guard per action

async function guarded(name, button, fn) {
  if (running.has(name)) return undefined;
  running.add(name);
  if (button) button.disabled = true;
  try { return await fn(); } finally {
    running.delete(name);
    if (button) button.disabled = false;
  }
}

// ---------- navigation ----------
function showTab(name) {
  document.querySelectorAll('[role="tab"]').forEach((t) => t.setAttribute('aria-selected', String(t.dataset.tab === name)));
  document.querySelectorAll('[data-panel]').forEach((p) => { p.hidden = p.dataset.panel !== name; });
  if (name === 'system') refreshSystem();
  if (name === 'lab') refreshControls();
  if (name === 'playground' || name === 'payments') loadAccounts();
}
document.querySelectorAll('[role="tab"]').forEach((t) => t.addEventListener('click', () => showTab(t.dataset.tab)));
$('#system-indicator').addEventListener('click', () => showTab('system'));

// ---------- named accounts (real ledger balances) ----------
let accounts = []; // [{name, id, balanceMinor}]
const byName = () => new Map(accounts.map((a) => [a.name, a]));
const nameOf = (id) => accounts.find((a) => a.id === Number(id))?.name ?? `Account ${id}`;

async function loadAccounts() {
  try {
    accounts = await api.namedAccounts();
  } catch (e) {
    $('#account-cards').innerHTML = view.errorBox(e);
    return;
  }
  $('#account-cards').innerHTML = accounts.length
    ? accounts.map((a) => `<div class="account-card"><span class="name">${esc(a.name)}</span>
        <span class="balance">${money(a.balanceMinor)}</span><span class="id">Account #${a.id}</span></div>`).join('')
    : '<p class="muted">No accounts yet. Create one.</p>';
  fillSelects();
  renderPlayBalances();
}

function options(selected) {
  return accounts.map((a) => `<option value="${a.id}"${a.id === Number(selected) ? ' selected' : ''}>${esc(a.name)}</option>`).join('');
}

function fillSelects() {
  const form = $('#pay-form');
  const payer = form.payer.value || accounts[0]?.id;
  const payee = form.payee.value || accounts[1]?.id;
  form.payer.innerHTML = options(payer);
  form.payee.innerHTML = options(payee);
  document.querySelectorAll('#tx-rows select').forEach((s) => {
    const current = s.value || s.dataset.want;
    s.innerHTML = `<option value="">choose…</option>${accounts.map((a) => `<option${a.name === current ? ' selected' : ''}>${esc(a.name)}</option>`).join('')}`;
  });
}

$('#refresh-accounts').addEventListener('click', (e) => guarded('accounts', e.target, loadAccounts));
$('#open-create').addEventListener('click', () => { $('#create-form').hidden = false; $('#create-form').name.focus(); });
$('#cancel-create').addEventListener('click', () => { $('#create-form').hidden = true; });
$('#create-form').addEventListener('submit', (e) => {
  e.preventDefault();
  const f = e.target;
  guarded('create', f.querySelector('[type=submit]'), async () => {
    $('#create-error').innerHTML = '';
    try {
      await api.createNamedAccount(f.name.value.trim(), toMinor(f.balance.value));
      f.hidden = true;
      f.name.value = '';
      await loadAccounts();
    } catch (err) {
      const plain = err instanceof api.ApiRejection
        ? (err.status === 409 ? 'An account with that name already exists.'
          : 'Use a name of 1–24 letters or digits (starting with a letter) and a balance between $0 and $1,000,000.')
        : null;
      $('#create-error').innerHTML = plain ? `<div class="error"><b>Account not created</b>${esc(plain)}</div>` : view.errorBox(err);
    }
  });
});

// ---------- system status ----------
let lastHealth = { payment: '…', ledger: '…' };
let lastInvariants = null;

function paintIndicator() {
  const up = lastHealth.payment === 'UP' && lastHealth.ledger === 'UP';
  const ind = $('#system-indicator');
  ind.className = `system-indicator ${up && lastInvariants !== false ? 'ok' : 'bad'}`;
  $('.label', ind).textContent = !up ? 'Service unavailable' : lastInvariants === false ? 'Invariant problem' : 'All systems normal';
}

function setRow(name, text, ok) {
  const b = $(`[data-row="${name}"] b`);
  b.textContent = text;
  b.className = ok ? 'ok' : 'bad';
}

async function refreshHealth() {
  const [payment, ledger] = await Promise.all([api.health('payment'), api.health('ledger')]);
  lastHealth = { payment, ledger };
  setRow('payment', payment === 'UP' ? 'UP' : 'DOWN', payment === 'UP');
  setRow('ledger', ledger === 'UP' ? 'UP' : 'DOWN', ledger === 'UP');
  paintIndicator();
}

async function runInvariants() {
  try {
    const inv = await api.invariants();
    lastInvariants = inv.pass;
    setRow('invariants', inv.pass ? 'PASS' : `FAIL (${inv.violations})`, inv.pass);
    $('#invariant-detail').innerHTML = view.invariantTable(inv);
  } catch (e) {
    lastInvariants = null;
    setRow('invariants', 'UNAVAILABLE', false);
    $('#invariant-detail').innerHTML = view.errorBox(e);
  }
  paintIndicator();
}

async function refreshProcessingRows() {
  try {
    const [l, p] = await Promise.all([api.ledgerControls(), api.paymentControls()]);
    setRow('ledger-processing', l.ledgerPaused ? 'PAUSED' : l.delayMs ? `DELAYED ${l.delayMs / 1000} s` : 'RUNNING', !l.ledgerPaused && !l.delayMs);
    setRow('result-processing', p.resultsPaused ? 'PAUSED' : p.loseResults ? 'LOSING RESULTS' : 'RUNNING', !p.resultsPaused && !p.loseResults);
  } catch { /* shown by the health rows */ }
}

const refreshSystem = () => Promise.all([refreshHealth(), runInvariants(), refreshProcessingRows()]);
$('#run-invariants').addEventListener('click', (e) => guarded('invariants', e.target, runInvariants));
$('#refresh-status').addEventListener('click', (e) => guarded('refresh', e.target, refreshSystem));

// ---------- payments ----------
const form = $('#pay-form');
let lastRequest = null;

function card(html) {
  const el = $('#pay-result');
  el.className = 'card result-card';
  el.innerHTML = html;
}

const outcome = (kind, icon, headline, body) =>
  `<div class="outcome ${kind}"><div class="headline"><span class="icon">${icon}</span>${esc(headline)}</div>${body}</div>`;

const flowNote = `<p class="small muted">Journey: the payment service saves the payment together with a <code>PaymentRequested</code> message (transactional outbox),
  publishes it to Kafka, the ledger service records the money movement and answers with <code>LedgerPosted</code> or <code>LedgerRejected</code>.
  This page only sees the payment's state through the API (processing, then final); the steps in between are inferred from that change, not observed directly.</p>`;

async function balancesOf(ids) {
  const rows = await api.balances(ids);
  return ids.map((id) => rows.find((r) => r.id === id)?.balanceMinor);
}

async function sendPayment(key, body) {
  const route = `${nameOf(body.payerAccountId)} → ${nameOf(body.payeeAccountId)}`;
  card(outcome('wait', '…', 'Sending…', `<div class="amount">${money(body.amountMinor)}</div><div class="route">${esc(route)}</div>`));
  let before = [];
  try { before = await balancesOf([body.payerAccountId, body.payeeAccountId]); } catch { /* bonus info */ }
  const started = performance.now();
  let res;
  try {
    res = await api.postPayment(key, body);
  } catch (e) {
    card(view.errorBox(e));
    return;
  }
  const baseDetails = [['HTTP status', res.status], ['Idempotency key (generated)', key],
    ['Response replayed', res.replayed ? 'yes (same request ID sent before)' : 'no'],
    ['Accounts', `#${body.payerAccountId} → #${body.payeeAccountId}`]];

  if (res.status >= 400) {
    const why = res.status === 422 ? 'This request ID was already used for a different payment' : (res.data?.detail ?? 'Invalid request');
    card(outcome('bad', '✕', 'Payment not accepted', `<div class="reason">${esc(why)}</div>
      ${view.story([{ text: 'No payment was created', ok: true }])}${view.technical(view.kv([...baseDetails, ['Detail', res.data?.detail ?? res.text]]))}`));
    return;
  }

  let p = res.data;
  const render = () => {
    const secs = `${((performance.now() - started) / 1000).toFixed(2)} s`;
    if (p.status === 'PENDING_LEDGER') {
      card(outcome('wait', '…', 'Payment accepted · Processing…', `<div class="amount">${money(p.amountMinor)}</div><div class="route">${esc(route)}</div>
        <div class="facts-line"><span class="pill wait">${statusLabel(p.status)}</span><span>${secs}</span></div>
        <p class="small muted">If ledger processing is paused or delayed in the Resilience Lab, the payment waits here safely.</p>`));
    }
  };
  render();
  const deadline = Date.now() + 90000;
  while (p.status === 'PENDING_LEDGER' && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 250));
    try { p = await api.getPayment(p.id); } catch (e) { card(view.errorBox(e)); return; }
    render();
  }
  const secs = `${((performance.now() - started) / 1000).toFixed(2)} s`;
  let after = [];
  let postings = null;
  try {
    after = await balancesOf([body.payerAccountId, body.payeeAccountId]);
    postings = (await api.ledgerFor([p.id]))[0]?.postings ?? null;
  } catch { /* shown as unknown */ }
  const details = view.technical(view.kv([...baseDetails, ['Payment ID', p.id],
    ['Status', `PENDING_LEDGER → ${p.status}`], ...(p.declineReason ? [['Reason code', p.declineReason]] : []),
    ['Ledger postings', postings ?? 'unknown']]) + flowNote);

  if (p.status === 'COMPLETED') {
    card(outcome('ok', '✓', res.replayed ? 'Payment successful (original result replayed)' : 'Payment successful',
      `<div class="amount">${money(p.amountMinor)}</div><div class="route">${esc(route)}</div>
       <div class="facts-line"><span class="pill ok">COMPLETED</span><span>${secs}</span></div>
       ${after.length ? view.story([{ text: `${nameOf(body.payerAccountId)}: ${money(before[0])} → ${money(after[0])}` },
         { text: `${nameOf(body.payeeAccountId)}: ${money(before[1])} → ${money(after[1])}` }]) : ''}${details}`));
  } else if (p.status === 'FAILED' || p.status === 'DECLINED') {
    const unchanged = before.length && after.length && before[0] === after[0] && before[1] === after[1];
    card(outcome('bad', '✕', p.status === 'DECLINED' ? 'Payment declined' : 'Payment failed',
      `<div class="reason">${esc(reasonLabel(p.declineReason))}</div><div class="route">${money(p.amountMinor)} · ${esc(route)}</div>
       ${view.story([{ text: 'Balance unchanged', ok: res.replayed ? undefined : Boolean(unchanged) },
         { text: 'No partial ledger write', ok: postings === 0 }])}${details}`));
  } else {
    card(outcome('wait', '…', 'Still processing', `<p>No final answer yet. If processing is paused in the Resilience Lab, resume it;
      otherwise LedgerFlow re-checks stuck payments automatically after 30 s.</p>${details}`));
  }
  loadAccounts();
}

form.addEventListener('submit', (e) => {
  e.preventDefault();
  if (form.payer.value === form.payee.value) {
    card(outcome('bad', '✕', 'Choose two different accounts', ''));
    return;
  }
  // Every NEW payment gets a fresh, automatically generated idempotency key.
  lastRequest = { key: newKey('pay'), body: api.paymentBody(form.payer.value, form.payee.value, toMinor(form.amount.value)) };
  $('#advanced-key').innerHTML = `<dt>Last request ID</dt><dd class="mono">${esc(lastRequest.key)}</dd>`;
  $('#retry-payment').disabled = false;
  guarded('pay', $('#send-payment'), () => sendPayment(lastRequest.key, lastRequest.body));
});
// Advanced: deliberately resend the exact same request (same key, same body).
$('#retry-payment').addEventListener('click', (e) => {
  if (lastRequest) guarded('pay', e.target, () => sendPayment(lastRequest.key, lastRequest.body));
});

// ---------- reliability tests ----------
$('#scenarios').innerHTML = SCENARIOS.map(view.scenarioCard).join('');
document.querySelectorAll('#scenarios [data-run]').forEach((button) => button.addEventListener('click', () => {
  const s = SCENARIOS.find((x) => x.id === button.dataset.run);
  const out = $(`[data-scenario="${s.id}"] .result`);
  guarded(`scenario:${s.id}`, button, async () => {
    out.innerHTML = '<div class="verdict running">Running against the live system…</div>';
    button.textContent = 'Running…';
    try {
      out.innerHTML = view.scenarioResult(await s.run());
    } catch (e) {
      out.innerHTML = `<div class="verdict fail">FAIL</div>${view.errorBox(e)}`;
    } finally {
      button.textContent = 'Run again';
      runInvariants();
    }
  });
}));

// ---------- transaction playground ----------
let activePreset = null;
$('#preset-buttons').innerHTML = PRESETS.map((p) => `<button class="btn" data-preset="${p.id}">${esc(p.label)}</button>`).join('');

function txRow(from = '', to = '', dollars = 10, delay = 0) {
  return `<div class="tx-row">
    <span class="tx-label"></span>
    <label>From <select data-f="from" data-want="${esc(from)}"></select></label>
    <label>To <select data-f="to" data-want="${esc(to)}"></select></label>
    <label>Amount <span class="money-input"><span>$</span><input data-f="amount" type="number" min="0.01" step="0.01" value="${dollars}"></span></label>
    <label>Start delay <span class="money-input"><input data-f="delay" type="number" min="0" max="10000" step="100" value="${delay}"><span class="suffix">ms</span></span></label>
    <button class="btn link remove" title="Remove transaction">Remove</button>
  </div>`;
}

function setRows(rows) {
  $('#tx-rows').innerHTML = rows.map((r) => txRow(...r)).join('');
  relabel();
  fillSelects();
}

function relabel() {
  const rows = [...document.querySelectorAll('#tx-rows .tx-row')];
  rows.forEach((r, i) => { r.querySelector('.tx-label').textContent = `Transaction ${i + 1}`; });
  document.querySelectorAll('#tx-rows .remove').forEach((b) => { b.disabled = rows.length <= 2; });
  $('#add-tx').disabled = rows.length >= 5;
}

function readRows() {
  return [...document.querySelectorAll('#tx-rows .tx-row')].map((r) => ({
    from: r.querySelector('[data-f=from]').value,
    to: r.querySelector('[data-f=to]').value,
    amountMinor: toMinor(r.querySelector('[data-f=amount]').value),
    delayMs: Math.max(0, Math.min(10000, Number(r.querySelector('[data-f=delay]').value) || 0)),
  }));
}

function renderPlayBalances() {
  const strip = $('#play-balances');
  if (!strip) return;
  strip.innerHTML = accounts.map((a) => `<div><span>${esc(a.name)}</span><b>${money(a.balanceMinor)}</b></div>`).join('');
  renderPresetNote();
}

function renderPresetNote() {
  const note = $('#preset-note');
  const p = PRESETS.find((x) => x.id === activePreset);
  if (!p) { note.hidden = true; return; }
  note.hidden = false;
  const current = byName();
  const differs = p.balances && Object.entries(p.balances).some(([n, b]) => current.get(n)?.balanceMinor !== b);
  const wanted = p.balances ? Object.entries(p.balances).map(([n, b]) => `${n} ${money(b)}`).join(', ') : '';
  note.innerHTML = `<p>${esc(p.explain)}</p>${differs ? `<p class="small">This example assumes ${esc(wanted)}. Current balances differ.
    <button class="btn" id="apply-balances">Set these balances</button>
    <span class="muted">(moves money to/from each account's own treasury with ordinary balanced entries; creates missing accounts)</span></p>` : ''}`;
  $('#apply-balances')?.addEventListener('click', (e) => guarded('apply-balances', e.target, async () => {
    try {
      for (const [name, b] of Object.entries(p.balances)) {
        if (!current.has(name)) await api.createNamedAccount(name, b);
      }
      await api.setNamedBalances(p.balances);
      await loadAccounts();
    } catch (err) {
      note.insertAdjacentHTML('beforeend', view.errorBox(err));
    }
  }));
}

document.querySelectorAll('[data-preset]').forEach((b) => b.addEventListener('click', () => {
  const p = PRESETS.find((x) => x.id === b.dataset.preset);
  activePreset = p.id;
  document.querySelectorAll('[data-preset]').forEach((x) => x.classList.toggle('selected', x === b));
  setRows(p.txs);
  $('#reverse-delays').hidden = p.id !== 'delayed';
  renderPresetNote();
}));
$('#reverse-delays').addEventListener('click', () => {
  const inputs = [...document.querySelectorAll('#tx-rows [data-f=delay]')];
  const values = inputs.map((i) => i.value).reverse();
  inputs.forEach((i, k) => { i.value = values[k]; });
});
$('#add-tx').addEventListener('click', () => {
  if (document.querySelectorAll('#tx-rows .tx-row').length >= 5) return;
  $('#tx-rows').insertAdjacentHTML('beforeend', txRow());
  relabel();
  fillSelects();
});
$('#tx-rows').addEventListener('click', (e) => {
  if (!e.target.classList.contains('remove') || document.querySelectorAll('#tx-rows .tx-row').length <= 2) return;
  e.target.closest('.tx-row').remove();
  relabel();
});

$('#run-tx').addEventListener('click', (e) => guarded('playground', e.target, async () => {
  const out = $('#play-result');
  out.hidden = false;
  const rows = readRows();
  const problem = rows.find((r) => !r.from || !r.to) ? 'Choose a From and To account for every transaction.'
    : rows.find((r) => r.from === r.to) ? 'A transaction cannot pay the same account it comes from.'
      : rows.find((r) => !(r.amountMinor > 0)) ? 'Every amount must be more than $0.' : null;
  if (problem) { out.innerHTML = `<div class="error"><b>Check the transactions</b>${esc(problem)}</div>`; return; }
  out.innerHTML = `<div class="verdict running">Running ${rows.length} transactions against the live system…</div>`;
  try {
    await loadAccounts();
    const r = await runPlayground(rows, byName());
    out.innerHTML = view.playgroundResult(r, timeline(r.tx));
  } catch (err) {
    out.innerHTML = `<div class="verdict fail">FAIL</div>${view.errorBox(err)}`;
  } finally {
    loadAccounts();
    runInvariants();
  }
}));

// ---------- resilience lab ----------
async function refreshControls() {
  try {
    const [l, p] = await Promise.all([api.ledgerControls(), api.paymentControls()]);
    const set = (name, text, ok, button) => {
      const li = $(`[data-ctl="${name}"]`);
      $('b', li).textContent = text;
      $('b', li).className = ok ? 'ok' : 'bad';
      if (button) $('[data-action]', li).textContent = button;
    };
    set('ledger', l.ledgerPaused ? 'PAUSED' : 'RUNNING', !l.ledgerPaused, l.ledgerPaused ? 'Resume' : 'Pause');
    set('results', p.resultsPaused ? 'PAUSED' : 'RUNNING', !p.resultsPaused, p.resultsPaused ? 'Resume' : 'Pause');
    set('lose', p.loseResults ? `BEING LOST (${p.resultsLost} so far)` : 'DELIVERED', !p.loseResults, p.loseResults ? 'Stop losing results' : 'Lose results');
    set('delay', l.delayMs ? `${l.delayMs / 1000} s` : 'NONE', !l.delayMs);
    document.querySelectorAll('[data-delay]').forEach((b) => b.classList.toggle('selected', Number(b.dataset.delay) === l.delayMs));
    return { l, p };
  } catch (e) {
    return null;
  }
}

$('#control-list').addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (!b) return;
  guarded('controls', b, async () => {
    const s = await refreshControls();
    if (!s) return;
    const { l, p } = s;
    if (b.dataset.action === 'toggle-ledger') await api.setLedgerControls({ ledgerPaused: !l.ledgerPaused, delayMs: l.delayMs });
    if (b.dataset.action === 'toggle-results') await api.setPaymentControls({ resultsPaused: !p.resultsPaused, loseResults: p.loseResults });
    if (b.dataset.action === 'toggle-lose') await api.setPaymentControls({ resultsPaused: p.resultsPaused, loseResults: !p.loseResults });
    if (b.dataset.delay !== undefined) await api.setLedgerControls({ ledgerPaused: l.ledgerPaused, delayMs: Number(b.dataset.delay) });
    await refreshControls();
  });
});
$('#reset-faults').addEventListener('click', (e) => guarded('reset-faults', e.target, async () => { await resetFaults(); await refreshControls(); }));

$('#experiments').innerHTML = EXPERIMENTS.map(view.experimentCard).join('');
document.querySelectorAll('#experiments [data-run]').forEach((button) => button.addEventListener('click', () => {
  const x = EXPERIMENTS.find((e) => e.id === button.dataset.run);
  const out = $(`[data-scenario="${x.id}"] .result`);
  guarded('experiment', button, async () => {
    document.querySelectorAll('#experiments [data-run]').forEach((b) => { b.disabled = true; });
    button.textContent = 'Running…';
    const progress = (text) => { out.innerHTML = `<div class="verdict running">${esc(text)}</div>`; refreshControls(); };
    progress('Starting…');
    try {
      out.innerHTML = view.experimentResult(await x.run(progress));
    } catch (e) {
      out.innerHTML = `<div class="verdict fail">FAIL</div>${view.errorBox(e)}`;
    } finally {
      document.querySelectorAll('#experiments [data-run]').forEach((b) => { b.disabled = false; });
      button.textContent = 'Run again';
      refreshControls();
      runInvariants();
    }
  });
}));

const INFRA = [
  ['./demo/run.sh ledger-crash', 'Crash the ledger service mid-traffic and restart it'],
  ['./demo/run.sh kafka-restart', 'Restart Kafka while payments are flowing'],
  ['./demo/run.sh postgres-pause', 'Freeze the database for 10 seconds'],
  ['./demo/run.sh invariants', 'Check all six invariants across the whole database'],
];
$('#infra-commands').innerHTML = INFRA.map(([cmd, what]) => `<li><span><code>${esc(cmd)}</code><br><span class="small muted">${esc(what)}</span></span>
  <button class="btn" data-copy="${esc(cmd)}">Copy</button></li>`).join('');
$('#infra-commands').addEventListener('click', async (e) => {
  const b = e.target.closest('[data-copy]');
  if (!b) return;
  try { await navigator.clipboard.writeText(b.dataset.copy); b.textContent = 'Copied'; } catch { b.textContent = 'Select & copy'; }
  setTimeout(() => { b.textContent = 'Copy'; }, 1500);
});

// ---------- boot ----------
setRows(PRESETS.find((p) => p.id === 'dependency').txs);
loadAccounts();
refreshSystem();
setInterval(refreshHealth, 5000);
