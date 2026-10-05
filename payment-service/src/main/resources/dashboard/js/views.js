import { ApiRejection } from './api.js';
import { esc, money, reasonLabel } from './format.js';

export const kv = (rows) => `<dl class="kv small">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('')}</dl>`;

export const technical = (inner) => `<details class="tech"><summary>View technical details</summary>${inner}</details>`;

export function errorBox(error) {
  const title = error instanceof ApiRejection ? 'Request rejected' : 'Couldn\'t reach LedgerFlow';
  return `<div class="error"><b>${title}</b>${esc(error?.message ?? error)}</div>`;
}

/** Measured plain-English lines: ok true → ✓, false → ✗, undefined → plain info. */
export const story = (lines) => `<ul class="story">${lines.map((l) =>
  `<li class="${l.ok === undefined ? 'info' : l.ok ? 'ok' : 'bad'}">${esc(l.text)}</li>`).join('')}</ul>`;

export function scenarioCard(s) {
  return `<article class="card scenario" data-scenario="${esc(s.id)}">
    <h2>${esc(s.title)}</h2>
    <p class="what">${esc(s.what)}</p>
    <p class="why">${esc(s.why)}</p>
    <button class="btn primary" data-run="${esc(s.id)}">Run test</button>
    <div class="result"></div>
  </article>`;
}

export function scenarioResult(r) {
  const facts = r.facts?.length
    ? `<div class="facts">${r.facts.map(([n, label]) => `<div><b>${esc(n)}</b><span>${esc(label)}</span></div>`).join('')}</div>` : '';
  const checks = `<p class="small muted">Measured checks (all must hold for PASS):</p>
    <ul class="checks">${r.checks.map((c) => `<li class="${c.ok ? '' : 'bad'}">${esc(c.label)}</li>`).join('')}</ul>`;
  return `<div class="verdict ${r.pass ? 'pass' : 'fail'}">${r.pass ? 'PASS' : 'FAIL'}</div>
    <p class="lead">${esc(r.lead)}</p>
    ${facts}
    ${story(r.story)}
    ${technical(kv(r.details) + checks)}`;
}

export function invariantTable(inv) {
  const rows = [...inv.ledger.checks.map((c) => ({ ...c, source: 'ledger-service' })),
    ...inv.payment.checks.map((c) => ({ ...c, source: 'payment-service' }))]
    .sort((a, b) => a.invariant.localeCompare(b.invariant))
    .map((c) => `<tr><td><b>${esc(c.invariant)}</b></td><td>${c.pass ? 'PASS' : 'FAIL'}</td>
      <td>${esc(c.description)}<br><span class="muted">${esc(c.source)} · ${c.violations} violation(s)</span></td></tr>`).join('');
  return `<table class="inv-table">${rows}</table>
    <p class="muted">${inv.payment.pending} payment(s) processing right now. Each service checks its own database schema.
    Checks that need both (e.g. "completed ⇔ recorded in the ledger") run inside each reliability test, and for the whole
    database with <code>./demo/run.sh invariants</code>.</p>`;
}

export function experimentCard(x) {
  return `<article class="card scenario" data-scenario="${esc(x.id)}">
    <h2>${esc(x.title)}</h2>
    <p class="what">${esc(x.what)}</p>
    <button class="btn primary" data-run="${esc(x.id)}">Run experiment</button>
    <div class="result"></div>
  </article>`;
}

export function experimentResult(r) {
  return `<div class="verdict ${r.pass ? 'pass' : 'fail'}">${r.pass ? 'PASS' : 'FAIL'}</div>
    <p class="section-label">What we did</p><p>${esc(r.did)}</p>
    <p class="section-label">What was paused or slowed</p><p>${esc(r.paused)}</p>
    <p class="section-label">What you would see</p>${story(r.observed)}
    <p class="section-label">How it recovered</p>${story(r.recovered)}
    ${technical(kv(r.details))}`;
}

const STATUS_WORD = { COMPLETED: 'Completed', FAILED: 'Failed', DECLINED: 'Declined', PENDING_LEDGER: 'Still processing' };

export function playgroundResult(r, events) {
  const accounts = r.accounts.map((a) => `<div class="move"><span class="name">${esc(a.name)}</span>
    <span>${money(a.before)} → <b>${money(a.after)}</b></span></div>`).join('');
  const txs = r.tx.map((t) => {
    const ok = t.status === 'COMPLETED';
    const word = STATUS_WORD[t.status] ?? (t.error ? 'Not sent' : t.status);
    const why = t.declineReason ? ` — ${reasonLabel(t.declineReason)}` : '';
    return `<li class="${ok ? 'ok' : 'bad'}"><span>${esc(t.from)} → ${esc(t.to)}</span><span class="amt">${money(t.amountMinor)}</span>
      <span class="pill ${ok ? 'ok' : t.status === 'PENDING_LEDGER' ? 'wait' : 'bad'}">${esc(word)}</span><span class="muted">${esc(why)}</span></li>`;
  }).join('');
  const completed = r.tx.filter((t) => t.status === 'COMPLETED').length;
  const rejected = r.tx.filter((t) => t.status === 'FAILED' || t.status === 'DECLINED').length;
  const processing = r.tx.length - completed - rejected;
  // "safely" only when the correctness checks actually held
  const summary = `${completed} completed · ${rejected} rejected${rejected && r.pass ? ' safely' : ''}`
    + (processing ? ` · ${processing} still processing` : '');
  const tl = events.map((e) => `<li><span class="ms">${e.ms} ms</span>${esc(e.text)}</li>`).join('');
  const tech = r.tx.map((t, i) => [[`#${i + 1} accounts`, `#${t.payerAccountId ?? '?'} → #${t.payeeAccountId ?? '?'}`],
    [`#${i + 1} idempotency key`, t.key], [`#${i + 1} payment ID`, t.id ?? '–'], [`#${i + 1} HTTP`, t.httpStatus],
    [`#${i + 1} status / reason`, `${t.status ?? '–'} ${t.declineReason ?? ''}`], [`#${i + 1} ledger postings`, t.postings ?? '–']]).flat();
  const checks = `<ul class="checks">${r.checks.map((c) => `<li class="${c.ok ? '' : 'bad'}">${esc(c.label)}</li>`).join('')}</ul>`;
  return `<div class="verdict long ${r.pass ? 'pass' : 'fail'}">${r.pass ? 'RACE HANDLED CORRECTLY ✓' : 'SCENARIO FAILED ✕'}</div>
    <p class="lead">${esc(summary)}</p>
    <p class="section-label">Balances</p><div class="moves">${accounts}</div>
    <p class="section-label">Transactions</p><ul class="tx-list">${txs}</ul>
    <p class="section-label">Correctness</p>${story(r.checks.map((c) => ({ text: c.label.replace(/ \(\d+ violations\)$/, ''), ok: c.ok })))}
    <p class="small muted">"Race handled correctly" means every correctness rule held. A payment rejected for lack of funds is a correct outcome, not a failed scenario.</p>
    <details class="tech"><summary>View timeline</summary><ul class="timeline">${tl}</ul>
      <p class="small muted">Times are measured by the demo runner from the start of the run: when each request was sent, when the payment API answered,
      and when the final state was first observed (it checks every 20 ms). Database locks and Kafka timings are not measured here.</p></details>
    ${technical(kv(tech) + checks)}`;
}
