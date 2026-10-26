#!/usr/bin/env bash
# Isolated Mongo-only performance and corruption check.
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=ledgerflow-perf
source chaos/lib.sh
cleanup() { $C down -v; }
trap cleanup EXIT
$C up -d --no-deps --wait --wait-timeout 180 mongo

{
  cat <<'JS'
const LEDGERFLOW_CHECK_LIBRARY = true;
const payments = db.getSiblingDB('payments');
const ledger = db.getSiblingDB('ledger');
const crypto = require('crypto');
const count = 30000;
const amount = Long('101');
const payer = Long('1');
const payee = Long('2');
// Only this isolated Compose project's databases are populated.
for (let first = 0; first < count; first += 1000) {
  const paymentRows = [], keys = [], outcomes = [], transactions = [], entries = [];
  for (let i = first; i < first + 1000; i++) {
    const id = `perf-payment-${i}`;
    const tx = Long((9007199254740993n + BigInt(i)).toString());
    paymentRows.push({_id: id, payer_account_id: payer, payee_account_id: payee,
      amount_minor: amount, currency: 'USD', status: 'COMPLETED'});
    keys.push({key: `perf-key-${i}`, payment_id: id,
      request_hash: crypto.createHash('sha256').update(`${payer}|${payee}|${amount}|USD`).digest('hex'),
      response_body: JSON.stringify({id}), response_status: 202});
    outcomes.push({payment_id: id, status: 'POSTED'});
    transactions.push({_id: tx, payment_id: id});
    entries.push({transaction_id: tx, account_id: payer, amount_minor: Long('-101')},
      {transaction_id: tx, account_id: payee, amount_minor: amount});
  }
  payments.payments.insertMany(paymentRows);
  payments.idempotency_keys.insertMany(keys);
  ledger.payment_outcomes.insertMany(outcomes);
  ledger.ledger_transactions.insertMany(transactions);
  for (let offset = 0; offset < entries.length; offset += 1000)
    ledger.ledger_entries.insertMany(entries.slice(offset, offset + 1000));
}
ledger.accounts.insertMany([
  {_id: payer, type: 'SYSTEM', balance_minor: Long(String(-count * 101))},
  {_id: payee, type: 'MERCHANT', balance_minor: Long(String(count * 101))}
]);
JS
  cat chaos/verify_invariants.js
  cat <<'JS'
const started = Date.now();
const consistent = check(payments, ledger);
const seconds = (Date.now() - started) / 1000;
print(`checker_seconds=${seconds}`);
for (const v of consistent) print(`${v.invariant} | ${v.detail}`);
if (consistent.length !== 0) throw Error('consistent fixture has violations');
if (seconds >= 60) throw Error('checker took 60 seconds or more');
print('consistent_violations=0');
ledger.ledger_entries.updateOne({transaction_id: Long('9007199254740993'), account_id: payee},
  {$set: {amount_minor: Long('102')}});
const corrupted = check(payments, ledger);
for (const v of corrupted) print(`${v.invariant} | ${v.detail}`);
if (!corrupted.some(v => v.invariant === 'I1')) throw Error('entry corruption did not trigger I1');
print('corruption_detected=I1');
JS
} | $C exec -T mongo mongosh admin --quiet --username local_admin --password local_admin --file /dev/stdin
