package com.g414.haildb;

public enum SearchResultCode {
    /* returns -1, 0 or 1 based on search result */
    BEFORE(0), EQUALS(1), AFTER(2);

    private final int code;

    private SearchResultCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code - 1;
    }

    public static SearchResultCode fromCode(int code) {
        if (code == 0) {
            return EQUALS;
        } else if (code > 0) {
            return AFTER;
        } else {
            return BEFORE;
        }
    }
}