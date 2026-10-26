// Creates and funds accounts atomically. Set FIRST, COUNT, TYPE and AMOUNT before loading.
// Funding uses balanced ledger entries from the SYSTEM account, preserving I1/I5.
function createAccounts(first, count, type, amount) {
  const session = db.getMongo().startSession();
  try {
    session.withTransaction(() => {
      const ledger = session.getDatabase('ledger');
      const treasury = ledger.accounts.findOne({_id: Long('1')});
      if (!treasury || treasury.type !== 'SYSTEM') throw Error('SYSTEM treasury is missing');
      for (let i = 0; i < count; i++) {
        const id = Long(String(first + i));
        ledger.accounts.insertOne({_id: id, type, balance_minor: Long(String(amount))});
        if (amount > 0) {
          const txId = Long(BigInt('0x' + require('crypto').randomBytes(8).toString('hex')) & 0x7fffffffffffffffn);
          ledger.ledger_transactions.insertOne({_id: txId, payment_id: require('crypto').randomUUID(), created_at: new Date()});
          ledger.ledger_entries.insertMany([
            {transaction_id: txId, account_id: Long('1'), amount_minor: Long(String(-amount))},
            {transaction_id: txId, account_id: id, amount_minor: Long(String(amount))}
          ]);
          ledger.accounts.updateOne({_id: Long('1')}, {$inc: {balance_minor: Long(String(-amount))}});
        }
      }
    });
  } finally { session.endSession(); }
}
createAccounts(FIRST, COUNT, TYPE, AMOUNT);
