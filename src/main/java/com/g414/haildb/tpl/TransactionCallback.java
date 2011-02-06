package com.g414.haildb.tpl;

import com.g414.haildb.Transaction;

public interface TransactionCallback<T> {
    public T inTransaction(Transaction txn);
}
