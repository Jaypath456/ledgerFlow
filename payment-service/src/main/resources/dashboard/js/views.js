import { esc, money, shortId } from './format.js';

export const badge = (text, cls = text) => `<span class="badge ${esc(cls)}">${esc(text)}</span>`;

export function errorBox(error) {
  const kind = error?.name === 'ApiRejection' || error?.constructor?.name === 'ApiRejection'
    ? 'Rejected by the API' : 'Infrastructure error';
  return `<div class="error-box"><b>${kind}</b>${esc(error?.message ?? error)}</div>`;
}

export function scenarioCard(s) {
  return `<article class="card scenario" data-scenario="${esc(s.id)}">
    <h2><span class="letter">${esc(s.letter)}</span>${esc(s.title)}</h2>
    <p class="expect">${esc(s.expect)}</p>
    <button class="btn primary" data-run="${esc(s.id)}">Run scenario</button>
    <div class="result" hidden></div>
  </article>`;
}

export function scenarioResult(r) {
  const headline = r.headline.map(([k, v]) => `<div><b>${esc(v)}</b><span class="muted">${esc(k)}</span></div>`).join('');
  const rows = r.rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('');
  const checks = r.checks.map((c) => `<li class="${c.ok ? '' : 'bad'}">${esc(c.label)}</li>`).join('');
  return `<div class="verdict ${r.pass ? 'pass' : 'fail'}">${r.pass ? 'PASS' : 'FAIL'}</div>
    <div class="headline">${headline}</div>
    <dl class="kv">${rows}</dl>
    <ul class="checks">${checks}</ul>`;
}

export function invariantTable(inv) {
  const rows = [...inv.ledger.checks.map((c) => ({ ...c, source: 'ledger-service' })),
    ...inv.payment.checks.map((c) => ({ ...c, source: 'payment-service' }))]
    .sort((a, b) => a.invariant.localeCompare(b.invariant))
    .map((c) => `<tr><td><b>${esc(c.invariant)}</b></td><td>${badge(c.pass ? 'PASS' : 'FAIL', c.pass ? 'pass' : 'fail')}</td>
      <td>${esc(c.description)}<br><small class="muted">${esc(c.source)} · ${c.violations} violations</small></td></tr>`).join('');
  return `<table>${rows}</table>
    <p class="small muted">${inv.payment.pending} payment(s) in flight right now. Cross-schema checks (COMPLETED ⇔ posted) run per scenario; the full database check is <code>./demo/run.sh invariants</code>.</p>`;
}

export function accountRows(accounts, balances) {
  if (!accounts.length) return '<tr><td colspan="4" class="muted">No demo accounts yet; run a scenario or use "Create demo accounts".</td></tr>';
  return accounts.map((a) => {
    const b = balances.find((x) => x.id === a.id);
    return `<tr><td class="mono">${a.id}</td><td>${esc(a.type)}</td><td>${esc(a.createdBy)}</td><td class="num">${b ? money(b.balanceMinor) : '–'}</td></tr>`;
  }).join('');
}

export function paymentSummary(p) {
  return `<dl class="kv"><dt>Payment</dt><dd class="mono">${esc(shortId(p.id))}</dd><dt>Status</dt><dd>${badge(p.status)}</dd>
    <dt>Amount</dt><dd>${money(p.amountMinor)} (${p.payerAccountId} → ${p.payeeAccountId})</dd>
    ${p.declineReason ? `<dt>Reason</dt><dd>${esc(p.declineReason)}</dd>` : ''}</dl>`;
}

export function activityEntry(e) {
  const when = new Date(e.at).toLocaleTimeString();
  return `<div class="entry"><time>${esc(when)}</time>${e.pass === undefined ? '' : badge(e.pass ? 'PASS' : 'FAIL', e.pass ? 'pass' : 'fail')} ${esc(e.text)}</div>`;
}
