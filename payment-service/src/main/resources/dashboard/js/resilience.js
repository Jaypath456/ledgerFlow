// Resilience Lab experiments. They use the demo-profile application controls only (Kafka listener
// pause/resume, a real processing delay, discarding results) — never Docker, processes or SQL.
// Each experiment uses two fresh demo accounts ($100 payer, $0 payee) so it is repeatable.
import * as api from './api.js';
import { money, newKey } from './format.js';
import { balanceOf, settle } from './scenarios.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function freshPair(label) {
  const created = await api.createAccounts([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }]);
  return created.accounts.map((a) => a.id);
}

async function status(id) { return (await api.getPayment(id)).status; }
async function ledgerOf(id) { return (await api.ledgerFor([id]))[0]; }

/** Polls until fn() is truthy; returns its value. */
async function until(fn, timeoutMs, onTick) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) throw new api.InfraError(`Gave up after ${Math.round(timeoutMs / 1000)} s`);
    onTick?.(Math.round((timeoutMs - (end - Date.now())) / 1000));
    await sleep(250);
  }
}

const step = (text, ok) => ({ text, ok });

async function verdict(r) {
  const inv = await api.invariants();
  r.recovered.push(step(inv.pass ? 'Invariants passed' : `Invariant check found ${inv.violations} problem(s)`, inv.pass));
  const all = [...r.observed, ...r.recovered];
  return { ...r, pass: all.every((s) => s.ok !== false) };
}

export const EXPERIMENTS = [
  {
    id: 'pause-ledger', title: 'Ledger processing paused',
    what: 'Pause the ledger\'s message processing, send a $25 payment, then resume.',
    async run(progress) {
      const [payer, payee] = await freshPair();
      try {
        await api.setLedgerControls({ ledgerPaused: true, delayMs: (await api.ledgerControls()).delayMs });
        progress('Ledger processing paused. Sending $25…');
        const r = await api.postPayment(newKey('lab-ledger'), api.paymentBody(payer, payee, 2500));
        const id = r.data.id;
        progress('Payment accepted. Watching it for 4 seconds while the ledger is paused…');
        await sleep(4000);
        const waiting = await status(id);
        const [pbPaused] = await balanceOf(payer);
        const lPaused = await ledgerOf(id);
        progress('Resuming ledger processing…');
        await api.setLedgerControls({ ledgerPaused: false, delayMs: (await api.ledgerControls()).delayMs });
        const final = await until(async () => { const s = await status(id); return s !== 'PENDING_LEDGER' && s; }, 60000,
          (s) => progress(`Processing… ${s} s since resume`));
        const [pb, qb] = await balanceOf(payer, payee);
        const l = await ledgerOf(id);
        return verdict({
          did: 'Paused the ledger service\'s Kafka listener (the service itself kept running), sent a $25 payment, then resumed.',
          paused: 'Ledger processing: the payment request waited in Kafka.',
          observed: [step(`Payment accepted (HTTP ${r.status})`, r.status === 202),
            step(`Payment waited safely: still "Processing…" after 4 s`, waiting === 'PENDING_LEDGER'),
            step(`Balance unchanged while paused (${money(pbPaused)})`, pbPaused === 10000 && lPaused.postings === 0)],
          recovered: [step(`Payment completed after resuming (${final})`, final === 'COMPLETED'),
            step(`Ledger postings: ${l.postings}`, l.postings === 1),
            step(`Balances correct: ${money(10000)} → ${money(pb)}, ${money(0)} → ${money(qb)}`, pb === 7500 && qb === 2500)],
          details: [['Payment ID', id], ['Status while paused', waiting], ['Final status', final], ['Ledger postings', l.postings]],
        });
      } finally {
        await api.setLedgerControls({ ledgerPaused: false, delayMs: (await api.ledgerControls()).delayMs }).catch(() => {});
      }
    },
  },
  {
    id: 'pause-results', title: 'Result processing paused',
    what: 'Pause the payment service\'s handling of ledger results, send $25, then resume.',
    async run(progress) {
      const [payer, payee] = await freshPair();
      const ctl = await api.paymentControls();
      try {
        await api.setPaymentControls({ resultsPaused: true, loseResults: ctl.loseResults });
        progress('Result processing paused. Sending $25…');
        const r = await api.postPayment(newKey('lab-results'), api.paymentBody(payer, payee, 2500));
        const id = r.data.id;
        progress('Waiting for the ledger to record the payment…');
        const posted = await until(async () => { const l = await ledgerOf(id); return l.outcome && l; }, 30000);
        const stillPending = await status(id);
        const [pbMid] = await balanceOf(payer);
        progress('Ledger has moved the money; the payment service has not heard yet. Resuming…');
        await api.setPaymentControls({ resultsPaused: false, loseResults: ctl.loseResults });
        const final = await until(async () => { const s = await status(id); return s !== 'PENDING_LEDGER' && s; }, 60000);
        const l = await ledgerOf(id);
        return verdict({
          did: 'Paused the payment service\'s result listener, sent a $25 payment, then resumed it.',
          paused: 'Result processing: the ledger\'s answer waited in Kafka.',
          observed: [step(`Ledger outcome exists (${posted.outcome}) and money moved (payer now ${money(pbMid)})`, posted.outcome === 'POSTED' && pbMid === 7500),
            step('Payment state still "Processing…" in the payment service', stillPending === 'PENDING_LEDGER')],
          recovered: [step(`Payment completed after resuming (${final})`, final === 'COMPLETED'),
            step(`Ledger postings: ${l.postings} (never a second posting)`, l.postings === 1)],
          details: [['Payment ID', id], ['Ledger outcome while paused', posted.outcome], ['Payment status while paused', stillPending],
            ['Final status', final], ['Ledger postings', l.postings]],
        });
      } finally {
        await api.setPaymentControls({ resultsPaused: false, loseResults: ctl.loseResults }).catch(() => {});
      }
    },
  },
  {
    id: 'lose-results', title: 'Lost result, recovered by reconciliation',
    what: 'Make the payment service drop the ledger\'s answer, then let automatic reconciliation recover it (about 30–45 s).',
    async run(progress) {
      const [payer, payee] = await freshPair();
      const ctl = await api.paymentControls();
      try {
        await api.setPaymentControls({ resultsPaused: ctl.resultsPaused, loseResults: true });
        progress('Results are now being dropped. Sending $25…');
        const r = await api.postPayment(newKey('lab-lost'), api.paymentBody(payer, payee, 2500));
        const id = r.data.id;
        const posted = await until(async () => { const l = await ledgerOf(id); return l.outcome && l; }, 30000);
        await until(async () => (await api.paymentControls()).resultsLost > ctl.resultsLost, 15000);
        const stillPending = await status(id);
        await api.setPaymentControls({ resultsPaused: ctl.resultsPaused, loseResults: false });
        progress('The ledger posted, but its answer was dropped. Waiting for reconciliation…');
        const t0 = performance.now();
        const final = await until(async () => { const s = await status(id); return s !== 'PENDING_LEDGER' && s; }, 120000,
          (s) => progress(`Payment still "Processing…". Reconciliation re-checks payments older than 30 s (${s} s so far)`));
        const l = await ledgerOf(id);
        return verdict({
          did: 'Told the payment service to drop ledger results (a simulated lost message), sent $25, then stopped dropping.',
          paused: 'The ledger\'s answer for this payment was lost; nothing re-sends it on its own.',
          observed: [step(`Ledger moved the money (${posted.outcome})`, posted.outcome === 'POSTED'),
            step('Payment service still showed "Processing…"', stillPending === 'PENDING_LEDGER')],
          recovered: [step(`Reconciliation asked the ledger again; payment ${final} after ${((performance.now() - t0) / 1000).toFixed(0)} s`, final === 'COMPLETED'),
            step(`Ledger re-sent its recorded decision (${l.resultsEmitted} results sent in total)`, l.resultsEmitted >= 2),
            step(`Ledger postings: ${l.postings} (no second posting)`, l.postings === 1)],
          details: [['Payment ID', id], ['Ledger outcome', posted.outcome], ['Results sent by the ledger', l.resultsEmitted],
            ['Final status', final], ['Ledger postings', l.postings]],
        });
      } finally {
        await api.setPaymentControls({ resultsPaused: ctl.resultsPaused, loseResults: false }).catch(() => {});
      }
    },
  },
  ...[2000, 5000].map((ms) => ({
    id: `delay-${ms}`, title: `${ms / 1000}-second ledger delay`,
    what: `Make the ledger wait ${ms / 1000} seconds before processing each message, then send $25.`,
    async run(progress) {
      const [payer, payee] = await freshPair();
      const before = await api.ledgerControls();
      try {
        await api.setLedgerControls({ ledgerPaused: before.ledgerPaused, delayMs: ms });
        progress(`Ledger delay set to ${ms / 1000} s. Sending $25…`);
        const t0 = performance.now();
        const r = await api.postPayment(newKey(`lab-delay${ms}`), api.paymentBody(payer, payee, 2500));
        const final = await until(async () => { const s = await status(r.data.id); return s !== 'PENDING_LEDGER' && s; }, ms + 60000,
          (s) => progress(`Processing… ${s} s`));
        const took = performance.now() - t0;
        const l = await ledgerOf(r.data.id);
        return verdict({
          did: `Set a real ${ms / 1000}-second processing delay inside the ledger service, then sent $25.`,
          paused: `Ledger processing was slowed by ${ms / 1000} s per message.`,
          observed: [step(`Payment showed "Processing…" for ${(took / 1000).toFixed(1)} s`, took >= ms)],
          recovered: [step(`Payment completed (${final})`, final === 'COMPLETED'), step(`Ledger postings: ${l.postings}`, l.postings === 1)],
          details: [['Payment ID', r.data.id], ['Time to final state', `${Math.round(took)} ms`], ['Configured delay', `${ms} ms`]],
        });
      } finally {
        await api.setLedgerControls({ ledgerPaused: before.ledgerPaused, delayMs: 0 }).catch(() => {});
      }
    },
  })),
];

/** Clears every demo fault in both services. */
export async function resetFaults() {
  await Promise.all([api.setLedgerControls({ ledgerPaused: false, delayMs: 0 }),
    api.setPaymentControls({ resultsPaused: false, loseResults: false })]);
}
