// Reliability tests. Each one: fresh accounts → real requests → wait for settlement → measure.
// The verdict is PASS only if every measured check holds. Every scenario returns:
//   story   – plain-English lines for the result card ({text, ok?}); built from the same measurements
//   checks  – the exact measured checks (technical details)
//   details – raw values: HTTP codes, ids, counts, timings (technical details)
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

export async function balanceOf(...ids) {
  const rows = await api.balances(ids);
  return ids.map((id) => rows.find((r) => r.id === id)?.balanceMinor);
}

/** Polls until every payment is terminal; returns Map id → {status, declineReason}. */
export async function settle(ids, timeoutMs = SETTLE_TIMEOUT_MS) {
  const unique = [...new Set(ids)];
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const rows = [];
    for (let i = 0; i < unique.length; i += 1000) rows.push(...await api.statuses(unique.slice(i, i + 1000)));
    const pending = unique.length - rows.filter((r) => r.status !== 'PENDING_LEDGER').length;
    if (pending === 0) return new Map(rows.map((r) => [r.id, r]));
    if (Date.now() > deadline) {
      throw new api.InfraError(`${pending} of ${unique.length} payments still processing after ${timeoutMs / 1000}s`);
    }
    await new Promise((r) => setTimeout(r, 300));
  }
}

/**
 * Cross-checks payment status against the ledger for these payments (the part of I2/I3 that needs
 * both schemas): COMPLETED ⇔ exactly one posting + POSTED outcome; FAILED ⇔ none + REJECTED.
 */
export async function crossCheck(statusById) {
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
const line = (text, ok) => ({ text, ok });

async function finish(started, r) {
  const inv = await api.invariants();
  const clean = inv.violations === 0;
  r.checks.push(check(`Invariant check (I1–I6 service checks): ${inv.violations} violations`, clean));
  r.story.push(line(clean ? 'Invariants passed' : `Invariant check found ${inv.violations} problem(s)`, clean));
  r.details.push(['Invariant violations', inv.violations], ['Elapsed', `${((performance.now() - started) / 1000).toFixed(2)} s`]);
  return { ...r, pass: r.checks.every((c) => c.ok) };
}

// ---------- scenarios ----------

export const SCENARIOS = [
  {
    id: 'happy', title: 'Happy Payment',
    what: 'Send $25 from an account holding $100.',
    why: 'Shows a normal payment completing and both balances updating.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Happy Payment');
      const key = newKey('happy');
      const r = await api.postPayment(key, api.paymentBody(payer, payee, 2500));
      const settled = await settle([r.data.id]);
      const p = settled.get(r.data.id);
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      const once = x.postings === 1 && x.mismatches === 0;
      return finish(t, {
        lead: `${money(2500)} payment`,
        story: [line('Payment completed', p.status === 'COMPLETED'), line(`Payer: ${money(10000)} → ${money(pb)}`, pb === 7500),
          line(`Payee: ${money(0)} → ${money(qb)}`, qb === 2500), line('Recorded exactly once in the ledger', once)],
        checks: [check('Accepted with HTTP 202', r.status === 202), check('Reached COMPLETED', p.status === 'COMPLETED'),
          check('Payer $100 → $75', pb === 7500), check('Payee $0 → $25', qb === 2500),
          check('Posted exactly once, matching the ledger outcome', once)],
        details: [['HTTP status', r.status], ['Payment ID', r.data.id], ['Idempotency key (generated)', key],
          ['Status', `PENDING_LEDGER → ${p.status}`], ['Ledger postings', x.postings]],
      });
    },
  },
  {
    id: 'insufficient', title: 'Insufficient Funds',
    what: 'Try to send $25 from an account that only has $10.',
    why: 'Shows the payment is rejected without moving any money.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 1000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Insufficient Funds');
      const key = newKey('insufficient');
      const r = await api.postPayment(key, api.paymentBody(payer, payee, 2500));
      const settled = await settle([r.data.id]);
      const p = settled.get(r.data.id);
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      const rejected = p.status === 'FAILED' && p.declineReason === 'INSUFFICIENT_FUNDS';
      const noWrite = x.postings === 0 && x.mismatches === 0;
      return finish(t, {
        lead: `Attempted payment: ${money(2500)} · Available balance: ${money(1000)}`,
        story: [line('Payment rejected safely: insufficient funds', rejected), line(`Balance remained ${money(pb)}`, pb === 1000),
          line(`Payee remained ${money(qb)}`, qb === 0), line('No ledger posting created', noWrite)],
        checks: [check('Accepted for processing (HTTP 202)', r.status === 202), check('Reached FAILED', p.status === 'FAILED'),
          check('Reason INSUFFICIENT_FUNDS', p.declineReason === 'INSUFFICIENT_FUNDS'),
          check('Payer still $10.00', pb === 1000), check('Payee still $0.00', qb === 0),
          check('No ledger posting (no partial write)', noWrite)],
        details: [['HTTP status', r.status], ['Payment ID', r.data.id], ['Idempotency key (generated)', key],
          ['Status', `PENDING_LEDGER → ${p.status}`], ['Reason code', p.declineReason ?? '–'], ['Ledger postings', x.postings]],
      });
    },
  },
  {
    id: 'retry', title: 'Idempotent Retry',
    what: 'Send the exact same payment twice, as a client would after a timeout.',
    why: 'Shows the retry gets the original answer instead of paying twice.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Idempotent Retry');
      const key = newKey('retry'); // one key, deliberately reused for both requests
      const body = api.paymentBody(payer, payee, 1500);
      const first = await api.postPayment(key, body);
      const second = await api.postPayment(key, body);
      const ids = new Set([first.data?.id, second.data?.id]);
      const settled = await settle([...ids]);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      const replayed = second.replayed && !first.replayed && first.text === second.text;
      return finish(t, {
        lead: `Same ${money(1500)} payment sent twice with the same request ID`,
        facts: [[2, 'requests sent'], [ids.size, 'payment created'], [x.postings, 'ledger posting'], [second.replayed ? 1 : 0, 'response replayed']],
        story: [line('The retry returned the original response', replayed),
          line(`Charged once: ${money(10000)} → ${money(pb)}`, pb === 8500 && ids.size === 1 && x.postings === 1)],
        checks: [check('Both answered HTTP 202', first.status === 202 && second.status === 202),
          check('Second response marked as replay', second.replayed && !first.replayed),
          check('Replayed body is byte-identical to the original', first.text === second.text),
          check('One payment ID', ids.size === 1), check('One ledger posting', x.postings === 1 && x.mismatches === 0),
          check('Payer debited once ($100 → $85)', pb === 8500)],
        details: [['Idempotency key (generated once, reused)', key], ['First response', `HTTP ${first.status}`],
          ['Second response', `HTTP ${second.status}, Idempotent-Replayed: ${second.replayed}`],
          ['Payment ID', [...ids].join(', ')], ['Ledger postings', x.postings]],
      });
    },
  },
  {
    id: 'conflict', title: 'Idempotency Conflict',
    what: 'Reuse a payment\'s request ID, but change the amount.',
    why: 'Shows the changed request is refused and no second payment is made.',
    async run() {
      const t = performance.now();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Idempotency Conflict');
      const key = newKey('conflict'); // reused with a different body on purpose
      const first = await api.postPayment(key, api.paymentBody(payer, payee, 1000));
      const second = await api.postPayment(key, api.paymentBody(payer, payee, 1100));
      const settled = await settle([first.data.id]);
      const original = await api.getPayment(first.data.id);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      const onlyOriginal = pb === 9000 && x.postings === 1 && x.mismatches === 0 && !second.data?.id;
      return finish(t, {
        lead: `${money(1000)} payment, then the same request ID with ${money(1100)}`,
        story: [line('Original payment accepted', first.status === 202),
          line(`Changed request rejected: HTTP ${second.status}`, second.status === 422),
          line('No second payment created', onlyOriginal), line(`Original amount unchanged: ${money(original.amountMinor)}`, original.amountMinor === 1000)],
        checks: [check('Original accepted (HTTP 202)', first.status === 202), check('Conflicting reuse rejected with HTTP 422', second.status === 422),
          check('No payment ID in the 422 response', !second.data?.id), check('Original payment still $10.00', original.amountMinor === 1000),
          check('Only the original was posted (payer $100 → $90)', pb === 9000 && x.postings === 1 && x.mismatches === 0)],
        details: [['Idempotency key (reused on purpose)', key], ['First request', `HTTP ${first.status} → ${original.status}`],
          ['Second request', `HTTP ${second.status}: ${second.data?.detail ?? '–'}`], ['Payment ID', first.data.id],
          ['Payer balance', money(pb)], ['Ledger postings', x.postings]],
      });
    },
  },
  {
    id: 'same-key-race', title: 'Same-Key Race',
    what: 'Fire the same payment 32 times at the same moment.',
    why: 'Shows that exactly one payment is made, however many copies arrive at once.',
    async run() {
      const t = performance.now();
      const n = 32;
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Same-Key Race');
      const key = newKey('race');
      const body = api.paymentBody(payer, payee, 1500);
      const b = await api.burst(Array.from({ length: n }, () => ({ key, body })));
      const ids = new Set(b.results.map((r) => r.id).filter(Boolean));
      const replays = b.results.filter((r) => r.replayed).length;
      const settled = await settle([...ids]);
      const [pb] = await balanceOf(payer);
      const x = await crossCheck(settled);
      const final = [...settled.values()][0]?.status;
      return finish(t, {
        lead: `${n} identical ${money(1500)} requests at the same moment`,
        facts: [[ids.size, 'payment created'], [replays, 'duplicates absorbed'], [x.postings, 'ledger posting']],
        story: [line(`Charged once: ${money(10000)} → ${money(pb)}`, pb === 8500), line('Every copy got the same answer', b.results.every((r) => r.httpStatus === 202))],
        checks: [check(`All ${n} requests answered`, b.responses === n), check('All answered HTTP 202', b.results.every((r) => r.httpStatus === 202)),
          check('Exactly one payment ID', ids.size === 1), check(`${n - 1} responses were replays`, replays === n - 1),
          check('Exactly one ledger posting', x.postings === 1 && x.mismatches === 0), check('Payer debited once ($100 → $85)', pb === 8500)],
        details: [['Idempotency key (shared by all)', key], ['Requests sent / responses', `${b.requests} / ${b.responses}`],
          ['Burst duration', `${b.elapsedMs} ms`], ['Payment ID', [...ids].join(', ')], ['Final status', final]],
      });
    },
  },
  {
    id: 'hot-account', title: 'Hot-Account Race',
    what: 'Try 200 payments at the same time against an account that only has enough money for 50.',
    why: 'Demonstrates that concurrent requests cannot overdraft the account.',
    async run() {
      const t = performance.now();
      const n = 200; const start = 50000; const amount = 1000;
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: start }, { type: 'MERCHANT', balanceMinor: 0 }], 'Hot-Account Race');
      const body = api.paymentBody(payer, payee, amount);
      const b = await api.burst(Array.from({ length: n }, () => ({ key: newKey('hot'), body })));
      const ids = b.results.map((r) => r.id).filter(Boolean);
      const settled = await settle(ids);
      const completed = count(settled, 'COMPLETED');
      const failed = count(settled, 'FAILED');
      const [pb, qb] = await balanceOf(payer, payee);
      const x = await crossCheck(settled);
      const fits = Math.floor(start / amount);
      const noOverdraft = pb === start - completed * amount && pb >= 0;
      return finish(t, {
        lead: `${n} simultaneous ${money(amount)} payments`,
        facts: [[completed, 'completed'], [failed, 'rejected safely']],
        story: [line(`Starting balance: ${money(start)}`), line(`Ending balance: ${money(pb)}`),
          line('No overdraft', noOverdraft && completed === fits && failed === n - fits),
          line('No duplicate postings', x.duplicatePostings === 0 && x.postings === completed && x.mismatches === 0)],
        checks: [check(`All ${n} requests accepted (HTTP 202)`, b.results.filter((r) => r.httpStatus === 202).length === n),
          check(`${n} distinct payments`, new Set(ids).size === n),
          check(`Exactly ${fits} COMPLETED (what $500 can pay)`, completed === fits),
          check(`Remaining ${n - fits} FAILED`, failed === n - fits),
          check('Ending balance $0.00, never negative', noOverdraft),
          check('Payee received exactly the completed total', qb === completed * amount),
          check('One posting per COMPLETED payment, none for FAILED', x.postings === completed && x.mismatches === 0)],
        details: [['HTTP 202 responses', `${b.results.filter((r) => r.httpStatus === 202).length} of ${b.requests}`],
          ['Unique payments', new Set(ids).size], ['Ledger postings', x.postings], ['Duplicate postings', x.duplicatePostings],
          ['Payee received', money(qb)], ['Burst duration', `${b.elapsedMs} ms`], ['Failure reason', 'INSUFFICIENT_FUNDS']],
      });
    },
  },
  {
    id: 'opposite-direction', title: 'Opposite-Direction Race',
    what: 'Two accounts pay each other 100 times, in both directions, all at once.',
    why: 'Shows there are no deadlocks and no money is created or lost.',
    async run() {
      const t = performance.now();
      const n = 100; const amount = 100;
      const [a, b] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'CUSTOMER', balanceMinor: 10000 }], 'Opposite-Direction Race');
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
      const [ab, bb] = await balanceOf(a, b);
      const x = await crossCheck(settled);
      const timeouts = burst.results.filter((r) => r.httpStatus === 0).length;
      const deadlocks = deadlocksAfter - deadlocksBefore;
      return finish(t, {
        lead: `${2 * n} payments of ${money(amount)} in opposite directions`,
        facts: [[completed, 'completed'], [deadlocks, 'deadlocks']],
        story: [line(`Balances: A ${money(ab)} · B ${money(bb)}`), line('No money created or lost', ab + bb === 20000),
          line('Every payment finished', completed === 2 * n && timeouts === 0)],
        checks: [check('Every request answered', burst.responses === 2 * n && timeouts === 0),
          check('Every payment settled (none stuck)', settled.size === 2 * n),
          check('All 200 COMPLETED (each side can always pay)', completed === 2 * n),
          check(`No deadlocks recorded by Postgres during the run (database-wide counter: +${deadlocks})`, deadlocks === 0),
          check('Money conserved: A + B = $200.00', ab + bb === 20000),
          check('One posting per COMPLETED payment', x.postings === completed && x.mismatches === 0)],
        details: [['Requests', `${n} A→B + ${n} B→A`], ['Responses received', burst.responses], ['Request timeouts/errors', timeouts],
          ['Postgres deadlock counter', `+${deadlocks} (database-wide)`], ['Ledger postings', x.postings], ['Burst duration', `${burst.elapsedMs} ms`]],
      });
    },
  },
  {
    id: 'risk', title: 'Risk Rules',
    what: 'Try a payment over $10,000 and a payment to a blocked account.',
    why: 'Shows risky payments are declined before any money moves.',
    async run() {
      const t = performance.now();
      const cfg = await api.riskConfig();
      const [payer, payee] = await setup([{ type: 'CUSTOMER', balanceMinor: 10000 }, { type: 'MERCHANT', balanceMinor: 0 }], 'Risk Rules');
      const big = await api.postPayment(newKey('risk-amount'), api.paymentBody(payer, payee, cfg.maxAmountMinor + 1));
      const blockedId = cfg.blockedAccounts[0];
      const blocked = blockedId ? await api.postPayment(newKey('risk-blocked'), api.paymentBody(payer, blockedId, 500)) : null;
      const ids = [big.data?.id, blocked?.data?.id].filter(Boolean);
      const settled = await settle(ids);
      const x = await crossCheck(settled);
      const [pb] = await balanceOf(payer);
      const amountOk = big.status === 201 && big.data?.status === 'DECLINED' && big.data?.declineReason === 'AMOUNT_LIMIT';
      const blockedOk = blocked && blocked.status === 201 && blocked.data?.declineReason === 'BLOCKED_ACCOUNT';
      const untouched = x.postings === 0 && x.mismatches === 0 && pb === 10000;
      return finish(t, {
        lead: 'Two risky payments',
        story: [line(`Over ${money(cfg.maxAmountMinor)}: declined`, amountOk),
          line(blockedId ? `To blocked account ${blockedId}: declined` : 'Blocked-account rule: no blocked account configured', blockedOk),
          line('No money moved', untouched)],
        checks: [check(`Over ${money(cfg.maxAmountMinor)} → DECLINED (AMOUNT_LIMIT), HTTP 201`, amountOk),
          check(blockedId ? `Blocked account ${blockedId} → DECLINED (BLOCKED_ACCOUNT), HTTP 201` : 'Blocked-account rule: no blocked account configured', blockedOk),
          check('Declined payments never reached the ledger', x.postings === 0 && x.mismatches === 0),
          check('Payer balance untouched ($100.00)', pb === 10000)],
        details: [['Amount rule', `HTTP ${big.status} ${big.data?.status ?? ''} ${big.data?.declineReason ?? ''}`],
          ['Blocked rule', blocked ? `HTTP ${blocked.status} ${blocked.data?.status ?? ''} ${blocked.data?.declineReason ?? ''}` : 'not configured'],
          ['Velocity limit in effect', `${cfg.velocityMaxPerMinute} per payer per 60 s (normal profile: 5; raised in the demo so the races reach the ledger)`],
          ['Ledger postings', x.postings]],
      });
    },
  },
];
