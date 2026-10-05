// Failure Lab scenarios. Each one: fresh accounts → real requests → wait for settlement →
// measure → checks. The verdict is PASS only if every measured check holds.
import * as api from './api.js';
import { money, newKey } from './format.js';
import { trackAccounts } from './store.js';

const SETTLE_TIMEOUT_MS = 90000;

// ---------- shared steps ----------

async function setup(specs, scenario) {
  const created = await api.createAccounts(specs);
  trackAccounts(created.accounts, scenario);
  return created.accounts.map((a) => a.id);
}

async function balanceOf(...ids) {
  const rows = await api.balances(ids);
  return ids.map((id) => rows.find((r) => r.id === id)?.balanceMinor);
}

/** Polls until every payment is terminal; returns Map id → {status, declineReason}. */
async function settle(ids, timeoutMs = SETTLE_TIMEOUT_MS) {
  const unique = [...new Set(ids)];
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const rows = [];
    for (let i = 0; i < unique.length; i += 1000) rows.push(...await api.statuses(unique.slice(i, i + 1000)));
    const pending = unique.length - rows.filter((r) => r.status !== 'PENDING_LEDGER').length;
    if (pending === 0) return new Map(rows.map((r) => [r.id, r]));
    if (Date.now() > deadline) {
      throw new api.InfraError(`${pending} of ${unique.length} payments still PENDING_LEDGER after ${timeoutMs / 1000}s`);
    }
    await new Promise((r) => setTimeout(r, 300));
  }
}

/**
 * Cross-checks payment status against the ledger for these payments (the part of I2/I3 that needs
 * both schemas): COMPLETED ⇔ exactly one posting + POSTED outcome; FAILED ⇔ none + REJECTED.
 */
async function crossCheck(statusById) {
  const ids = [...statusById.keys()];
  const ledger = [];
  for (let i = 0; i < ids.length; i += 1000) ledger.push(...await api.ledgerFor(ids.slice(i, i + 1000)));
  let postings = 0;
  let mismatches = 0;
  for (const l of ledger) {
    postings += l.postings;
    const s = statusById.get(l.paymentId)?.status;
    const ok = s === 'COMPLETED' ? l.postings === 1 && l.outcome === 'POSTED'
      : s === 'FAILED' ? l.postings === 0 && l.outcome === 'REJECTED'
        : s === 'DECLINED' ? l.postings === 0 && l.outcome === null : false;
    if (!ok) mismatches += 1;
  }
  return { postings, mismatches, duplicatePostings: ledger.filter((l) => l.postings > 1).length };
}

const count = (statusById, status) => [...statusById.values()].filter((p) => p.status === status).length;
const check = (label, ok) => ({ label, ok: Boolean(ok) });

async function finish(started, headline, rows, checks, extra = {}) {
  const inv = await api.invariants();
  checks.push(check(`Invariant check (I1–I6 service checks): ${inv.violations} violations`, inv.violations === 0));
  rows.push(['Invariant violations', inv.violations]);
  rows.push(['Elapsed', `${((performance.now() - started) / 1000).toFixed(2)} s`]);
  return { headline, rows, checks, pass: checks.every((c) => c.ok), ...extra };
}

// ---------- scenarios ----------

export const SCENARIOS = [
  {
    id: 'happy', letter: 'A', title: 'Happy path',
    expect: 'Fresh payer $100 → payee $0, pay $25. Expect COMPLETED, payer $75, payee $25, one ledger posting.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Happy path');
      const r = await api.postPayment(newKey('happy'), api.paymentBody(payer, payee, 2500));
      const settled = await settle([r.data.id]);
      const p = settled.get(r.data.id);
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      return finish(t,
        [['Status', p.status], ['Payer', money(pb)], ['Payee', money(qb)]],
        [['HTTP status', r.status], ['Payment ID', r.data.id], ['Ledger postings', x.postings]],
        [check('Accepted with HTTP 202', r.status === 202), check('Reached COMPLETED', p.status === 'COMPLETED'),
          check('Payer $100 → $75', pb === 7500), check('Payee $0 → $25', qb === 2500),
          check('Posted exactly once, matching the ledger outcome', x.postings === 1 && x.mismatches === 0)]);
    },
  },
  {
    id: 'insufficient', letter: 'B', title: 'Insufficient funds',
    expect: 'Fresh payer $10, attempt $25. Expect FAILED (INSUFFICIENT_FUNDS), no ledger write, balances unchanged.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 1000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Insufficient funds');
      const r = await api.postPayment(newKey('insufficient'), api.paymentBody(payer, payee, 2500));
      const settled = await settle([r.data.id]);
      const p = settled.get(r.data.id);
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      return finish(t,
        [['Status', p.status], ['Reason', p.declineReason ?? '–'], ['Payer', money(pb)]],
        [['HTTP status', r.status], ['Payment ID', r.data.id], ['Payee', money(qb)], ['Ledger postings', x.postings]],
        [check('Accepted for processing (HTTP 202)', r.status === 202), check('Reached FAILED', p.status === 'FAILED'),
          check('Reason INSUFFICIENT_FUNDS', p.declineReason === 'INSUFFICIENT_FUNDS'),
          check('Payer still $10.00', pb === 1000), check('Payee still $0.00', qb === 0),
          check('No ledger posting (no partial write)', x.postings === 0 && x.mismatches === 0)]);
    },
  },
  {
    id: 'retry', letter: 'C', title: 'Idempotent retry',
    expect: 'Send the exact same key + body twice. Expect one payment, one ledger effect, and a byte-identical replayed response.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Idempotent retry');
      const key = newKey('retry');
      const body = api.paymentBody(payer, payee, 1500);
      const first = await api.postPayment(key, body);
      const second = await api.postPayment(key, body);
      const ids = new Set([first.data?.id, second.data?.id]);
      const settled = await settle([...ids]);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      return finish(t,
        [['Requests', 2], ['Unique payment IDs', ids.size], ['Ledger effects', x.postings]],
        [['First response', `HTTP ${first.status}`], ['Second response', `HTTP ${second.status}, replayed: ${second.replayed ? 'yes' : 'no'}`],
          ['Idempotency key', key], ['Payer balance', money(pb)]],
        [check('Both answered HTTP 202', first.status === 202 && second.status === 202),
          check('Second response marked as replay', second.replayed && !first.replayed),
          check('Replayed body is byte-identical to the original', first.text === second.text),
          check('One payment ID', ids.size === 1), check('One ledger posting', x.postings === 1 && x.mismatches === 0),
          check('Payer debited once ($100 → $85)', pb === 8500)]);
    },
  },
  {
    id: 'conflict', letter: 'D', title: 'Idempotency conflict',
    expect: 'Reuse a key with a different amount. Expect HTTP 422 and no second payment.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Idempotency conflict');
      const key = newKey('conflict');
      const first = await api.postPayment(key, api.paymentBody(payer, payee, 1000));
      const second = await api.postPayment(key, api.paymentBody(payer, payee, 1100));
      const settled = await settle([first.data.id]);
      const original = await api.getPayment(first.data.id);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      return finish(t,
        [['Second request', `HTTP ${second.status}`], ['Payments created', second.data?.id ? 2 : 1], ['Payer', money(pb)]],
        [['First request', `HTTP ${first.status} → ${original.status}`], ['Original amount', money(original.amountMinor)],
          ['Rejection detail', second.data?.detail ?? '–'], ['Ledger postings', x.postings]],
        [check('Original accepted (HTTP 202)', first.status === 202), check('Conflicting reuse rejected with HTTP 422', second.status === 422),
          check('No payment ID in the 422 response', !second.data?.id), check('Original payment still $10.00', original.amountMinor === 1000),
          check('Only the original was posted (payer $100 → $90)', pb === 9000 && x.postings === 1 && x.mismatches === 0)]);
    },
  },
  {
    id: 'same-key-race', letter: 'E', title: 'Same-key race',
    expect: '32 truly concurrent requests, same key and body. Expect one logical payment, one ledger posting, 31 replays.',
    async run() {
      const t = performance.now();
      const n = 32;
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Same-key race');
      const key = newKey('race');
      const body = api.paymentBody(payer, payee, 1500);
      const b = await api.burst(Array.from({ length: n }, () => ({ key, body })));
      const ids = new Set(b.results.map((r) => r.id).filter(Boolean));
      const replays = b.results.filter((r) => r.replayed).length;
      const settled = await settle([...ids]);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      const final = [...settled.values()][0]?.status;
      return finish(t,
        [['Unique payment IDs', ids.size], ['Ledger postings', x.postings], ['Replays absorbed', replays]],
        [['Requests sent', b.requests], ['Responses received', b.responses], ['Burst duration', `${b.elapsedMs} ms`],
          ['Final payment status', final], ['Payer balance', money(pb)]],
        [check(`All ${n} requests answered`, b.responses === n), check('All answered HTTP 202', b.results.every((r) => r.httpStatus === 202)),
          check('Exactly one payment ID', ids.size === 1), check(`${n - 1} responses were replays`, replays === n - 1),
          check('Exactly one ledger posting', x.postings === 1 && x.mismatches === 0), check('Payer debited once ($100 → $85)', pb === 8500)]);
    },
  },
  {
    id: 'hot-account', letter: 'F', title: 'Hot-account race',
    expect: 'Payer $500, 200 simultaneous $10 payments with unique keys. Expect exactly 50 COMPLETED, 150 FAILED, balance $0, no overdraft.',
    async run() {
      const t = performance.now();
      const n = 200; const start = 50000; const amount = 1000;
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: start }, { type: 'MERCHANT', balanceMinor: 0 }], 'Hot-account race');
      const body = api.paymentBody(payer, payee, amount);
      const b = await api.burst(Array.from({ length: n }, () => ({ key: newKey('hot'), body })));
      const ids = b.results.map((r) => r.id).filter(Boolean);
      const settled = await settle(ids);
      const completed = count(settled, 'COMPLETED');
      const failed = count(settled, 'FAILED');
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      const fits = Math.floor(start / amount);
      return finish(t,
        [['COMPLETED', completed], ['FAILED', failed], ['Ending balance', money(pb)]],
        [['Simultaneous requests', `${b.requests} × ${money(amount)}`], ['Responses received', b.responses], ['Starting balance', money(start)],
          ['Payee received', money(qb)], ['Overdrafts', pb < 0 ? 1 : 0], ['Ledger postings', x.postings],
          ['Duplicate postings', x.duplicatePostings], ['Burst duration', `${b.elapsedMs} ms`]],
        [check(`All ${n} requests accepted (HTTP 202)`, b.results.filter((r) => r.httpStatus === 202).length === n),
          check(`${n} distinct payments`, new Set(ids).size === n),
          check(`Exactly ${fits} COMPLETED (what $500 can pay)`, completed === fits),
          check(`Remaining ${n - fits} FAILED`, failed === n - fits),
          check('Ending balance $0.00, never negative', pb === start - completed * amount && pb >= 0),
          check('Payee received exactly the completed total', qb === completed * amount),
          check('One posting per COMPLETED payment, none for FAILED', x.postings === completed && x.mismatches === 0)],
        { big: true });
    },
  },
  {
    id: 'opposite-direction', letter: 'G', title: 'Opposite-direction race',
    expect: 'Accounts A and B ($100 each) pay each other 100 × $1 in both directions at once. Locks are taken in id order, so no deadlock; balances are conserved.',
    async run() {
      const t = performance.now();
      const n = 100; const amount = 100;
      const [a, b] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'CUSTOMER', balanceMinor: 10000 }], 'Opposite-direction race');
      const requests = [];
      for (let i = 0; i < n; i++) {
        requests.push({ key: newKey('ab'), body: api.paymentBody(a, b, amount) });
        requests.push({ key: newKey('ba'), body: api.paymentBody(b, a, amount) });
      }
      const deadlocksBefore = await api.deadlocks();
      const burst = await api.burst(requests);
      const ids = burst.results.map((r) => r.id).filter(Boolean);
      const settled = await settle(ids);
      const deadlocksAfter = await api.deadlocks();
      const completed = count(settled, 'COMPLETED');
      const failed = count(settled, 'FAILED');
      const [ab, bb] = await balanceOf(a, b);
      const x = await crossCheck(settled);
      const timeouts = burst.results.filter((r) => r.httpStatus === 0).length;
      return finish(t,
        [['COMPLETED', completed], ['FAILED', failed], ['Postgres deadlocks', deadlocksAfter - deadlocksBefore]],
        [['Requests', `${n} A→B + ${n} B→A`], ['Responses received', burst.responses], ['Request timeouts/errors', timeouts],
          ['Ending balances', `A ${money(ab)} · B ${money(bb)}`], ['Ledger postings', x.postings]],
        [check('Every request answered', burst.responses === 2 * n && timeouts === 0),
          check('Every payment settled (none stuck)', settled.size === 2 * n),
          check('All 200 COMPLETED (each side can always pay)', completed === 2 * n),
          check(`No deadlocks recorded by Postgres during the run (database-wide counter: +${deadlocksAfter - deadlocksBefore})`, deadlocksAfter === deadlocksBefore),
          check('Money conserved: A + B = $200.00', ab + bb === 20000),
          check('One posting per COMPLETED payment', x.postings === completed && x.mismatches === 0)]);
    },
  },
  {
    id: 'risk', letter: 'H', title: 'Risk rules',
    expect: 'An amount over $10,000 and a payment to a blocked account are DECLINED up front (HTTP 201) and never reach the ledger.',
    async run() {
      const t = performance.now();
      const cfg = await api.riskConfig();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Risk rules');
      const big = await api.postPayment(newKey('risk-amount'), api.paymentBody(payer, payee, cfg.maxAmountMinor + 1));
      const blockedId = cfg.blockedAccounts[0];
      const blocked = blockedId ? await api.postPayment(newKey('risk-blocked'), api.paymentBody(payer, blockedId, 500)) : null;
      const ids = [big.data?.id, blocked?.data?.id].filter(Boolean);
      const settled = await settle(ids);
      const x = await crossCheck(settled);
      const [pb] = await balanceOf(payer);
      const checks = [
        check(`Over ${money(cfg.maxAmountMinor)} → DECLINED (AMOUNT_LIMIT), HTTP 201`,
          big.status === 201 && big.data?.status === 'DECLINED' && big.data?.declineReason === 'AMOUNT_LIMIT'),
        check(blockedId ? `Blocked account ${blockedId} → DECLINED (BLOCKED_ACCOUNT), HTTP 201` : 'Blocked-account rule: no blocked account configured',
          blocked && blocked.status === 201 && blocked.data?.declineReason === 'BLOCKED_ACCOUNT'),
        check('Declined payments never reached the ledger', x.postings === 0 && x.mismatches === 0),
        check('Payer balance untouched ($100.00)', pb === 10000),
      ];
      return finish(t,
        [['Amount rule', big.data?.declineReason ?? `HTTP ${big.status}`], ['Blocked rule', blocked?.data?.declineReason ?? 'not configured'], ['Ledger postings', x.postings]],
        [['Velocity limit in effect', `${cfg.velocityMaxPerMinute} per payer per 60 s (normal profile: 5; the demo stack raises it so race scenarios reach the ledger)`],
          ['Blocked accounts', cfg.blockedAccounts.join(', ') || 'none']],
        checks);
    },
  },
];
