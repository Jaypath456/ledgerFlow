// LedgerFlow invariant checker. Run only after both services have settled.
// mongosh --quiet < chaos/verify_invariants.js
// check() also accepts session databases so corruption fixtures can roll back.
function check(payments, ledger) {
  const violations = [];
  const report = (invariant, detail) => violations.push({invariant, detail});
  const n = value => BigInt(value.toString());
  const entries = ledger.ledger_entries.find().toArray();
  const transactions = ledger.ledger_transactions.find().toArray();
  const accounts = ledger.accounts.find().toArray();
  const paymentRows = payments.payments.find().toArray();
  const keys = payments.idempotency_keys.find().toArray();
  const outcomes = ledger.payment_outcomes.find().toArray();
  const sum = rows => rows.reduce((total, row) => total + n(row.amount_minor), 0n);
  const txEntries = tx => entries.filter(e => n(e.transaction_id) === n(tx._id));
  const postings = payment => transactions.filter(t => t.payment_id === payment._id);

  for (const tx of transactions) {
    const rows = txEntries(tx);
    if (rows.length < 2 || sum(rows) !== 0n)
      report('I1', `ledger transaction ${tx._id} has ${rows.length} entries summing to ${sum(rows)}`);
  }
  if (sum(entries) !== 0n) report('I1', `global ledger sum is ${sum(entries)}`);

  const counts = new Map();
  for (const tx of transactions) counts.set(tx.payment_id, (counts.get(tx.payment_id) || 0) + 1);
  for (const [id, count] of counts) if (count > 1) report('I2', `payment ${id} posted ${count} times`);
  for (const p of paymentRows) {
    const txs = postings(p);
    for (const tx of txs) {
      if (['DECLINED', 'FAILED'].includes(p.status)) report('I2', `payment ${p._id} is ${p.status} but has a ledger posting`);
      const rows = txEntries(tx);
      if (rows.length !== 2 ||
          !rows.some(e => n(e.account_id) === n(p.payer_account_id) && n(e.amount_minor) === -n(p.amount_minor)) ||
          !rows.some(e => n(e.account_id) === n(p.payee_account_id) && n(e.amount_minor) === n(p.amount_minor)))
        report('I2', `payment ${p._id} posting does not match payer/payee/amount`);
    }
    if (p.status === 'PENDING_LEDGER') report('I3', `payment ${p._id} still PENDING_LEDGER`);
    if (p.status === 'COMPLETED' && txs.length === 0) report('I3', `payment ${p._id} is COMPLETED without a ledger posting`);
    const outcome = outcomes.find(o => o.payment_id === p._id);
    if ((p.status === 'COMPLETED' && outcome?.status !== 'POSTED') ||
        (p.status === 'FAILED' && outcome?.status !== 'REJECTED') ||
        (p.status === 'DECLINED' && outcome))
      report('I3', `payment ${p._id} is ${p.status} but ledger outcome is ${outcome?.status || 'missing'}`);
  }

  for (const account of accounts) {
    if (account.type !== 'SYSTEM' && n(account.balance_minor) < 0n)
      report('I4', `account ${account._id} (${account.type}) balance ${account.balance_minor}`);
    const total = sum(entries.filter(e => n(e.account_id) === n(account._id)));
    if (n(account.balance_minor) !== total) report('I5', `account ${account._id} cached ${account.balance_minor} <> entries ${total}`);
  }

  for (const p of paymentRows) {
    const count = keys.filter(k => k.payment_id === p._id).length;
    if (count !== 1) report('I6', `payment ${p._id} has ${count} idempotency keys`);
  }
  for (const key of keys) {
    const p = paymentRows.find(p => p._id === key.payment_id);
    if (!p) { report('I6', `idempotency key ${key.key} points to missing payment ${key.payment_id}`); continue; }
    const hash = require('crypto').createHash('sha256')
      .update(`${p.payer_account_id}|${p.payee_account_id}|${p.amount_minor}|${p.currency}`).digest('hex');
    let response;
    try { response = JSON.parse(key.response_body); } catch (_) { response = null; }
    if (key.request_hash !== hash || response?.id !== p._id || key.response_status !== (p.status === 'DECLINED' ? 201 : 202))
      report('I6', `idempotency key ${key.key} does not match payment ${p._id}`);
  }
  return violations;
}

if (typeof LEDGERFLOW_CHECK_LIBRARY === 'undefined') {
  for (const v of check(db.getSiblingDB('payments'), db.getSiblingDB('ledger'))) print(`${v.invariant} | ${v.detail}`);
}
