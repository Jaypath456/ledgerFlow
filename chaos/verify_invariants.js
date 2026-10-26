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
  // Normalize numeric fields once; BSON Long objects are not Map value keys.
  const entriesByTransaction = new Map();
  const entriesByAccount = new Map();
  const transactionsByPayment = new Map();
  const paymentsById = new Map();
  const outcomesByPayment = new Map();
  const keysByPayment = new Map();
  const group = (index, id, row) => {
    if (!index.has(id)) index.set(id, []);
    index.get(id).push(row);
  };
  let globalSum = 0n;
  for (const entry of entries) {
    entry.transactionNumber = n(entry.transaction_id);
    entry.accountNumber = n(entry.account_id);
    entry.amountNumber = n(entry.amount_minor);
    group(entriesByTransaction, entry.transactionNumber, entry);
    group(entriesByAccount, entry.accountNumber, entry);
    globalSum += entry.amountNumber;
  }
  for (const tx of transactions) {
    tx.idNumber = n(tx._id);
    group(transactionsByPayment, tx.payment_id, tx);
  }
  for (const p of paymentRows) {
    p.payerNumber = n(p.payer_account_id);
    p.payeeNumber = n(p.payee_account_id);
    p.amountNumber = n(p.amount_minor);
    if (!paymentsById.has(p._id)) paymentsById.set(p._id, p);
  }
  // Preserve find()'s first-outcome behavior if there are duplicates.
  for (const outcome of outcomes)
    if (!outcomesByPayment.has(outcome.payment_id)) outcomesByPayment.set(outcome.payment_id, outcome);
  for (const key of keys) group(keysByPayment, key.payment_id, key);
  const sum = rows => rows.reduce((total, row) => total + row.amountNumber, 0n);
  const txEntries = tx => entriesByTransaction.get(tx.idNumber) || [];
  const postings = payment => transactionsByPayment.get(payment._id) || [];

  for (const tx of transactions) {
    const rows = txEntries(tx);
    const total = sum(rows);
    if (rows.length < 2 || total !== 0n)
      report('I1', `ledger transaction ${tx._id} has ${rows.length} entries summing to ${total}`);
  }
  if (globalSum !== 0n) report('I1', `global ledger sum is ${globalSum}`);

  for (const [id, txs] of transactionsByPayment) {
    const count = txs.length;
    if (count > 1) report('I2', `payment ${id} posted ${count} times`);
  }
  for (const p of paymentRows) {
    const txs = postings(p);
    for (const tx of txs) {
      if (['DECLINED', 'FAILED'].includes(p.status)) report('I2', `payment ${p._id} is ${p.status} but has a ledger posting`);
      const rows = txEntries(tx);
      if (rows.length !== 2 ||
          !rows.some(e => e.accountNumber === p.payerNumber && e.amountNumber === -p.amountNumber) ||
          !rows.some(e => e.accountNumber === p.payeeNumber && e.amountNumber === p.amountNumber))
        report('I2', `payment ${p._id} posting does not match payer/payee/amount`);
    }
    if (p.status === 'PENDING_LEDGER') report('I3', `payment ${p._id} still PENDING_LEDGER`);
    if (p.status === 'COMPLETED' && txs.length === 0) report('I3', `payment ${p._id} is COMPLETED without a ledger posting`);
    const outcome = outcomesByPayment.get(p._id);
    if ((p.status === 'COMPLETED' && outcome?.status !== 'POSTED') ||
        (p.status === 'FAILED' && outcome?.status !== 'REJECTED') ||
        (p.status === 'DECLINED' && outcome))
      report('I3', `payment ${p._id} is ${p.status} but ledger outcome is ${outcome?.status || 'missing'}`);
  }

  for (const account of accounts) {
    const balance = n(account.balance_minor);
    if (account.type !== 'SYSTEM' && balance < 0n)
      report('I4', `account ${account._id} (${account.type}) balance ${account.balance_minor}`);
    const total = sum(entriesByAccount.get(n(account._id)) || []);
    if (balance !== total) report('I5', `account ${account._id} cached ${account.balance_minor} <> entries ${total}`);
  }

  for (const p of paymentRows) {
    const count = (keysByPayment.get(p._id) || []).length;
    if (count !== 1) report('I6', `payment ${p._id} has ${count} idempotency keys`);
  }
  for (const key of keys) {
    const p = paymentsById.get(key.payment_id);
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
