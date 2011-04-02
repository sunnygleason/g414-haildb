package com.g414.haildb.tpl;

import java.util.List;
import java.util.Map;

import com.g414.haildb.ColumnDef;
import com.g414.haildb.IndexDef;
import com.g414.haildb.TableDef;
import com.g414.haildb.TupleBuilder;
import com.g414.haildb.TupleBuilder.Options;
import com.g414.haildb.TupleStorage;

public class KeyHelper {
    public static boolean matchesPrimaryKey(IndexDef primary,
            Map<String, Object> toFind, Map<String, Object> found) {
        for (ColumnDef col : primary.getColumns()) {
            String colName = col.getName();
            Object seekVal = toFind.get(colName);
            Object foundVal = found.get(colName);

            if (!TupleStorage.areEqual(seekVal, foundVal, col.getType())) {
                return false;
            }
        }

        return true;
    }

    public static TupleBuilder createTupleBuilder(TableDef table,
            Map<String, Object> data) {
        TupleBuilder tpl = new TupleBuilder(table, Options.COERCE);
        for (ColumnDef col : table.getColDefs()) {
            tpl.addValue(data.get(col.getName()));
        }

        return tpl;
    }

    public static TupleBuilder createTupleBuilder(TableDef table,
            List<ColumnDef> defs, Map<String, Object> data) {
        TupleBuilder tpl = new TupleBuilder(table, Options.COERCE);
        for (ColumnDef col : defs) {
            tpl.addValue(data.get(col.getName()));
        }

        return tpl;
    }
}
