package com.ledgerflow.ledger;

public record Account(long id, AccountType type, long balanceMinor) {}
