/**
 * 
 */
package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;

public enum TransactionState {
    NOT_STARTED(HailDB.ib_trx_state_t.IB_TRX_NOT_STARTED), ACTIVE(
            HailDB.ib_trx_state_t.IB_TRX_ACTIVE), COMMITTED_IN_MEMORY(
            HailDB.ib_trx_state_t.IB_TRX_COMMITTED_IN_MEMORY), PREPARED(
            HailDB.ib_trx_state_t.IB_TRX_PREPARED);

    private final int code;

    private TransactionState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TransactionState fromCode(int code) {
        return TransactionState.values()[code];
    }
}