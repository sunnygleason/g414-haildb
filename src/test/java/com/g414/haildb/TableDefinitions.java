package com.g414.haildb;

public class TableDefinitions {
    public static final String SCHEMA_NAME = "foo";

    public static final String TABLE_1_NAME = "foo/bar";
    public static final TableDef TABLE_1;

    public static final String TABLE_2_NAME = "foo/vtest";
    public static final TableDef TABLE_2;

    public static final String TABLE_3_NAME = "foo/buz";
    public static final TableDef TABLE_3;

    static {
        TableBuilder b1 = new TableBuilder(TABLE_1_NAME);
        b1.addColumn("c1", ColumnType.VARCHAR, 32);
        b1.addColumn("c2", ColumnType.VARCHAR, 32);
        b1.addColumn("c3", ColumnType.INT, 8);
        b1.addColumn("c4", ColumnType.INT, 8);
        b1.addColumn("c5", ColumnType.INT, 4);
        b1.addColumn("c6", ColumnType.BLOB, 0);

        b1.addIndex("c1_c2", "c1", 0, true, true);
        b1.addIndex("c1_c2", "c2", 0, true, true);

        b1.addIndex("c3", "c3", 0, false, false);

        TABLE_1 = b1.build();

        TableBuilder b2 = new TableBuilder(TABLE_2_NAME);
        b2.addColumn("key_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        b2.addColumn("version_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        b2.addColumn("value_", ColumnType.BLOB, 0);

        b2.addIndex("PRIMARY", "key_", 0, true, true);
        b2.addIndex("PRIMARY", "version_", 0, true, true);

        TABLE_2 = b2.build();

        TableBuilder b3 = new TableBuilder(TABLE_3_NAME);
        b3.addColumn("a", ColumnType.INT, 1, ColumnAttribute.NOT_NULL,
                ColumnAttribute.UNSIGNED);
        b3.addColumn("b", ColumnType.INT, 2, ColumnAttribute.NOT_NULL);
        b3.addColumn("c", ColumnType.INT, 4, ColumnAttribute.NOT_NULL);
        b3.addColumn("d", ColumnType.INT, 8, ColumnAttribute.NOT_NULL);
        b3.addColumn("e", ColumnType.VARCHAR, 20, ColumnAttribute.NOT_NULL);
        b3.addColumn("f", ColumnType.BLOB, 20);

        b3.addIndex("PRIMARY", "a", 0, true, true);
        b3.addIndex("PRIMARY", "b", 0, true, true);
        b3.addIndex("PRIMARY", "c", 0, true, true);

        b3.addIndex("bc", "b", 0, false, false);
        b3.addIndex("bc", "c", 0, false, false);

        TABLE_3 = b3.build();
    }

    public static void createTables(Database d) {
        if (!d.tableExists(TableDefinitions.TABLE_1)) {
            d.createTable(TableDefinitions.TABLE_1);
        }

        if (!d.tableExists(TableDefinitions.TABLE_2)) {
            d.createTable(TableDefinitions.TABLE_2);
        }

        if (!d.tableExists(TableDefinitions.TABLE_3)) {
            d.createTable(TableDefinitions.TABLE_3);
        }
    }

    public static void clearTables(Database d) {
        d.truncateTable(TABLE_1);
        d.truncateTable(TABLE_2);
        d.truncateTable(TABLE_3);
        d.dropTable(TABLE_1);
        d.dropTable(TABLE_2);
        d.dropTable(TABLE_3);
    }
}
