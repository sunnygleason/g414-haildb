/**
 * 
 */
package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;

public enum TransactionLevel {
    READ_UNCOMMITTED(HailDB.ib_trx_level_t.IB_TRX_READ_UNCOMMITTED), READ_COMMITTED(
            HailDB.ib_trx_level_t.IB_TRX_READ_COMMITTED), REPEATABLE_READ(
            HailDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ), SERIALIZABLE(
            HailDB.ib_trx_level_t.IB_TRX_SERIALIZABLE);

    private final int code;

    private TransactionLevel(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TransactionLevel fromCode(int code) {
        return TransactionLevel.values()[code];
    }
}