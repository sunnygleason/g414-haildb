package com.g414.haildb;

import com.g414.haildb.impl.jna.HailDB;

public enum LockMode {
    /* see InnoDB.ib_lck_mode_t */
    INTENTION_SHARED(HailDB.ib_lck_mode_t.IB_LOCK_IS), INTENTION_EXCLUSIVE(
            HailDB.ib_lck_mode_t.IB_LOCK_IX), LOCK_SHARED(
            HailDB.ib_lck_mode_t.IB_LOCK_S), LOCK_EXCLUSIVE(
            HailDB.ib_lck_mode_t.IB_LOCK_X), NOT_USED(
            HailDB.ib_lck_mode_t.IB_LOCK_NOT_USED), NONE(
            HailDB.ib_lck_mode_t.IB_LOCK_NONE);

    private final int code;

    private LockMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static LockMode fromCode(int code) {
        return LockMode.values()[code];
    }
}