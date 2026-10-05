// Transaction Playground: user-defined concurrent payments between named accounts.
// The scenario verdict (PASS/FAIL) is about correctness properties, never about which payment wins:
// a payment that FAILS for insufficient funds is a correct outcome.
import * as api from './api.js';
import { money, newKey } from './format.js';
import { settle } from './scenarios.js';

/** Examples only PREFILL the editor; the user can change anything before running. */
export const PRESETS = [
  {
    id: 'dependency', label: 'Dependency race',
    balances: { Jay: 7500, Ajay: 10000, Jaysus: 10000 },
    txs: [['Jay', 'Ajay', 50, 0], ['Ajay', 'Jaysus', 140, 0]],
    explain: 'Ajay starts with $100 and cannot afford $140 on his own. If Jay\'s $50 reaches the ledger first, Ajay can pay Jaysus; '
      + 'if Ajay\'s payment is evaluated first, it fails. Either outcome is correct. PASS means: no overdraft, no partial write, '
      + 'a balanced ledger, and each payment recorded at most once.',
  },
  {
    id: 'shared-receiver', label: 'Shared receiver',
    balances: { Jay: 7500, Ajay: 10000, Jaysus: 10000 },
    txs: [['Jay', 'Ajay', 50, 0], ['Jaysus', 'Ajay', 50, 0]],
    explain: 'Two people pay Ajay at the same moment. Neither credit may be lost: Ajay should end up with exactly both amounts added.',
  },
  {
    id: 'shared-payer', label: 'Shared payer',
    balances: { Jay: 7500, Ajay: 10000, Jaysus: 10000 },
    txs: [['Jay', 'Ajay', 60, 0], ['Jay', 'Jaysus', 15, 0]],
    explain: 'Jay has $75 and sends $60 and $15 at once: exactly what he has. Both can complete; Jay must never go below $0. '
      + 'Try changing $15 to $25: then there is only money for one of them, and which one wins can depend on timing.',
  },
  {
    id: 'shared-payer-short', label: 'Shared payer (not enough)',
    balances: { Jay: 7500, Ajay: 10000, Jaysus: 10000 },
    txs: [['Jay', 'Ajay', 60, 0], ['Jay', 'Jaysus', 25, 0]],
    explain: 'Jay has $75 but $85 is requested at once. Only one payment can be covered; which one depends on ordering. '
      + 'One FAILED payment is the correct result, so the run can still PASS.',
  },
  {
    id: 'delayed', label: 'Delayed requests',
    balances: { Jay: 7500, Ajay: 10000, Jaysus: 10000 },
    txs: [['Jay', 'Ajay', 60, 0], ['Jay', 'Jaysus', 15, 1000]],
    explain: 'The second request is launched 1 second later, so the first one is normally decided first. Use "Reverse delays" to flip the order.',
  },
  { id: 'custom', label: 'Custom', balances: null, txs: [['', '', 10, 0], ['', '', 10, 0]], explain: 'Build your own: 2 to 5 transactions between any accounts.' },
];

/**
 * Runs the transactions for real (through POST /api/payments, via the demo runner) and measures:
 * balances before/after, each payment's final state, ledger postings, invariants.
 */
export async function runPlayground(rows, accountsByName) {
  const started = performance.now();
  const involved = [...new Set(rows.flatMap((r) => [r.from, r.to]))];
  const idOf = (name) => accountsByName.get(name)?.id;
  const ids = involved.map(idOf);
  const before = await balancesById(ids);

  const specs = rows.map((r) => ({
    key: newKey('play'), payerAccountId: idOf(r.from), payeeAccountId: idOf(r.to),
    amountMinor: r.amountMinor, startDelayMs: r.delayMs,
  }));
  const run = await api.runTransactions(specs);
  const paymentIds = run.results.map((r) => r.id).filter(Boolean);
  // The runner follows each payment for up to 60 s; settle covers anything it gave up on.
  const settled = paymentIds.length ? await settle(paymentIds, 30000).catch(() => null) : new Map();
  const after = await balancesById(ids);
  const ledger = paymentIds.length ? await api.ledgerFor(paymentIds) : [];
  const inv = await api.invariants();

  const tx = run.results.map((r, i) => {
    const s = settled?.get(r.id);
    const l = ledger.find((x) => x.paymentId === r.id);
    return { ...rows[i], ...r, payerAccountId: specs[i].payerAccountId, payeeAccountId: specs[i].payeeAccountId, status: s?.status ?? r.status, declineReason: s?.declineReason ?? r.declineReason,
      postings: l?.postings ?? null, outcome: l?.outcome ?? null };
  });

  // expected after-balance per account = before + completed credits − completed debits
  const expected = new Map(involved.map((n, i) => [n, before[i]]));
  for (const t of tx.filter((x) => x.status === 'COMPLETED')) {
    expected.set(t.from, expected.get(t.from) - t.amountMinor);
    expected.set(t.to, expected.get(t.to) + t.amountMinor);
  }
  const accounts = involved.map((name, i) => ({ name, id: ids[i], before: before[i], after: after[i], expected: expected.get(name) }));
  const sum = (xs) => xs.reduce((a, b) => a + b, 0);
  const allFinal = tx.every((t) => ['COMPLETED', 'FAILED', 'DECLINED'].includes(t.status));
  const postingsOk = tx.every((t) => (t.status === 'COMPLETED' ? t.postings === 1 && t.outcome === 'POSTED' : t.postings === 0));
  const checks = [
    { label: 'Every request was answered by the payment API', ok: tx.every((t) => t.httpStatus === 202 || t.httpStatus === 201) },
    { label: 'Every payment reached a final state', ok: allFinal },
    { label: 'No negative balances', ok: accounts.every((a) => a.after >= 0) },
    { label: 'No duplicate postings (completed = exactly 1, failed = 0)', ok: postingsOk },
    { label: 'Money conserved (total before = total after)', ok: sum(before) === sum(after) },
    { label: 'Every balance reconciles with the completed payments', ok: accounts.every((a) => a.after === a.expected) },
    { label: `Invariants passed (${inv.violations} violations)`, ok: inv.pass },
  ];
  return { pass: checks.every((c) => c.ok), checks, accounts, tx, run, elapsedMs: performance.now() - started };
}

async function balancesById(ids) {
  const rows = await api.balances(ids);
  return ids.map((id) => rows.find((r) => r.id === id)?.balanceMinor);
}

/** Measured timeline events (runner clock, ms since the run began). */
export function timeline(tx) {
  const events = [];
  for (const t of tx) {
    const label = `${t.from} → ${t.to} ${money(t.amountMinor)}`;
    events.push({ ms: t.startedMs, text: `${label}  request sent` });
    events.push({ ms: t.respondedMs, text: `${label}  accepted by the payment API (HTTP ${t.httpStatus})` });
    if (t.finishedMs !== null && t.finishedMs !== undefined) events.push({ ms: t.finishedMs, text: `${label}  ${t.status} (observed)` });
  }
  return events.sort((a, b) => a.ms - b.ms);
}
