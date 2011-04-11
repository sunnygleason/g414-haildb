package com.g414.haildb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import junit.framework.Assert;

import org.testng.annotations.Test;

@Test
public class TupleStorageTest {
    public void testAreEqual() {
        doEqualsCases(0L, 0, ColumnType.INT);
        doEqualsCases("0", 0, ColumnType.INT);
        doEqualsCases(new BigInteger("0"), 0L, ColumnType.INT);
        doEqualsCases(0.0, new BigDecimal("0.0"), ColumnType.FLOAT,
                ColumnType.DOUBLE, ColumnType.DECIMAL);
        doEqualsCases("foo", "foo", ColumnType.CHAR, ColumnType.VARCHAR,
                ColumnType.CHAR_ANYCHARSET, ColumnType.VARCHAR_ANYCHARSET);
        doEqualsCases("foo".getBytes(), "foo".getBytes(), ColumnType.BLOB,
                ColumnType.BINARY, ColumnType.VARBINARY);
    }

    public void testCoerceType() {
        Assert.assertTrue(Arrays.equals(
                (byte[]) TupleStorage.coerceType("0", ColumnType.BINARY),
                "0".getBytes()));
        Assert.assertTrue(Arrays.equals(
                (byte[]) TupleStorage.coerceType("0", ColumnType.VARBINARY),
                "0".getBytes()));
        Assert.assertTrue(Arrays.equals(
                (byte[]) TupleStorage.coerceType("0", ColumnType.BLOB),
                "0".getBytes()));

        try {
            TupleStorage.coerceType(null, ColumnType.INT);
            Assert.fail("expected exception!");
        } catch (IllegalArgumentException expected) {
        }
    }

    private void doEqualsCases(Object a, Object b, ColumnType... types) {
        for (ColumnType type : types) {
            Assert.assertTrue(TupleStorage.areEqual(a, b, type));
            Assert.assertTrue(TupleStorage.areEqual(b, a, type));

            if (!(a instanceof byte[]) && !(b instanceof byte[])) {
                Assert.assertTrue(TupleStorage.areEqual(a.toString(), b, type));
                Assert.assertTrue(TupleStorage.areEqual(b.toString(), a, type));
                Assert.assertTrue(TupleStorage.areEqual(a.toString(),
                        b.toString(), type));
                Assert.assertTrue(TupleStorage.areEqual(b.toString(),
                        a.toString(), type));
            }
        }
    }
}
