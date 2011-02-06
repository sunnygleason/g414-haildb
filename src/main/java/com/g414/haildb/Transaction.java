package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Transaction {
    protected final Pointer trx;

    public Transaction(Pointer trx) {
        this.trx = trx;
    }

    public Cursor openTable(TableDef tableDef) {
        PointerByReference crsr = new PointerByReference();

        Util.assertSuccess(HailDB.ib_cursor_open_table(tableDef.getName(), trx,
                crsr));

        return new Cursor(crsr, tableDef, null);
    }

    public void commit() {
        Util.assertSuccess(HailDB.ib_trx_commit(trx));
    }

    public void rollback() {
        Util.assertSuccess(HailDB.ib_trx_rollback(trx));
    }

    public void release() {
        Util.assertSuccess(HailDB.ib_trx_release(trx));
    }

    public void start(TransactionLevel level) {
        Util.assertSuccess(HailDB.ib_trx_start(trx, level.getCode()));
    }

    public TransactionState getState() {
        return TransactionState.fromCode(HailDB.ib_trx_state(trx));
    }

    public Pointer getTrx() {
        return trx;
    }
}
