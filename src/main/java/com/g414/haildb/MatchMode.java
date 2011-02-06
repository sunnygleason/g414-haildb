package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;

public enum MatchMode {
    /* see InnoDB.ib_match_mode_t */
    CLOSEST(HailDB.ib_match_mode_t.IB_CLOSEST_MATCH), EXACT(
            HailDB.ib_match_mode_t.IB_EXACT_MATCH), EXACT_PREFIX(
            HailDB.ib_match_mode_t.IB_EXACT_PREFIX);

    private final int code;

    private MatchMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static MatchMode fromCode(int code) {
        return MatchMode.values()[code];
    }
}