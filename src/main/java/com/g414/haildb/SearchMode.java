package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;

public enum SearchMode {
    /* see InnoDB.ib_srch_mode_t.IB_CUR_G */
    G(HailDB.ib_srch_mode_t.IB_CUR_G), GE(HailDB.ib_srch_mode_t.IB_CUR_GE), L(
            HailDB.ib_srch_mode_t.IB_CUR_L), LE(HailDB.ib_srch_mode_t.IB_CUR_LE);

    private final int code;

    private SearchMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static SearchMode fromCode(int code) {
        return SearchMode.values()[code - 1];
    }
}