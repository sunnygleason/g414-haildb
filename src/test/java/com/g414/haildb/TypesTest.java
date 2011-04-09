package com.g414.haildb;

import junit.framework.Assert;

import org.testng.annotations.Test;

@Test
public class TypesTest {
    public void testColumnTypes() {
        Assert.assertTrue(ColumnType.BINARY.isByteArrayType());
        Assert.assertTrue(ColumnType.VARBINARY.isByteArrayType());
        Assert.assertTrue(ColumnType.BLOB.isByteArrayType());
        Assert.assertFalse(ColumnType.CHAR.isByteArrayType());
        Assert.assertTrue(ColumnType.CHAR.isStringType());
        Assert.assertTrue(ColumnType.VARCHAR.isStringType());
        Assert.assertFalse(ColumnType.BLOB.isStringType());
        Assert.assertTrue(ColumnType.CHAR_ANYCHARSET.isStringType());
        Assert.assertTrue(ColumnType.VARCHAR_ANYCHARSET.isStringType());
        Assert.assertTrue(ColumnType.INT.isIntegerType());
        Assert.assertFalse(ColumnType.BLOB.isIntegerType());
        Assert.assertFalse(ColumnType.VARCHAR.isIntegerType());
    }
}
