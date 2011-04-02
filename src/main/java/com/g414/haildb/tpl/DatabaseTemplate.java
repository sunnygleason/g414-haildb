package com.g414.haildb.tpl;

import java.util.Map;

import com.g414.haildb.Cursor;
import com.g414.haildb.Database;
import com.g414.haildb.IndexDef;
import com.g414.haildb.LockMode;
import com.g414.haildb.SearchMode;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.TransactionLevel;
import com.g414.haildb.Tuple;
import com.g414.haildb.TupleBuilder;

public class DatabaseTemplate {
    public interface TransactionCallback<T> {
        public T inTransaction(Transaction txn);
    }

    protected final Database database;

    public DatabaseTemplate(Database database) {
        this.database = database;
    }

    public <T> T inTransaction(TransactionLevel level,
            TransactionCallback<T> callback) throws Exception {
        Transaction txn = database.beginTransaction(level);
        try {
            return callback.inTransaction(txn);
        } catch (Exception e) {
            txn.rollback();
            txn = null;

            throw e;
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }

    }

    public Map<String, Object> load(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toReturn = null;
        try {
            c = txn.openTable(def);

            TupleBuilder tpl = KeyHelper.createTupleBuilder(def,
                    primary.getColumns(), data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toReturn = c.createClusteredIndexReadTuple();
                c.readRow(toReturn);

                Map<String, Object> found = toReturn.valueMap();

                if (!KeyHelper.matchesPrimaryKey(primary, data, found)) {
                    return null;
                }
            } else {
                return null;
            }

            Map<String, Object> res = toReturn.valueMap();
            toReturn.clear();

            return res;
        } finally {
            if (toReturn != null) {
                toReturn.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public void insert(Transaction txn, TableDef def, Map<String, Object> data) {
        Cursor c = null;
        Tuple toInsert = null;
        try {
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            toInsert = c.createClusteredIndexReadTuple();
            TupleBuilder tpl = KeyHelper.createTupleBuilder(def, data);
            c.insertRow(toInsert, tpl);
            toInsert.clear();
        } finally {
            if (toInsert != null) {
                toInsert.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean update(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toUpdate = null;
        try {
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder tpl = KeyHelper.createTupleBuilder(def,
                    primary.getColumns(), data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toUpdate = c.createClusteredIndexReadTuple();
                c.readRow(toUpdate);

                Map<String, Object> found = toUpdate.valueMap();

                if (!KeyHelper.matchesPrimaryKey(primary, data, found)) {
                    return false;
                }
            } else {
                return false;
            }

            TupleBuilder val = KeyHelper.createTupleBuilder(def, data);
            c.updateRow(toUpdate, val);
            toUpdate.clear();

            return true;
        } finally {
            if (toUpdate != null) {
                toUpdate.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean insertOrUpdate(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toInsert = null;
        Tuple toUpdate = null;

        try {
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder val = KeyHelper.createTupleBuilder(def, data);
            TupleBuilder tpl = KeyHelper.createTupleBuilder(def,
                    primary.getColumns(), data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toUpdate = c.createClusteredIndexReadTuple();
                c.readRow(toUpdate);

                Map<String, Object> found = toUpdate.valueMap();

                if (KeyHelper.matchesPrimaryKey(primary, data, found)) {
                    c.updateRow(toUpdate, val);
                    toUpdate.clear();

                    return true;
                }
            }

            toInsert = c.createClusteredIndexReadTuple();
            c.insertRow(toInsert, val);
            toInsert.clear();

            return false;
        } finally {
            if (toInsert != null) {
                toInsert.delete();
            }

            if (toUpdate != null) {
                toUpdate.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean delete(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toDelete = null;
        try {
            c = txn.openTable(def);
            c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
            c.lock(LockMode.LOCK_EXCLUSIVE);

            TupleBuilder tpl = KeyHelper.createTupleBuilder(def,
                    primary.getColumns(), data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toDelete = c.createClusteredIndexReadTuple();
                c.readRow(toDelete);

                Map<String, Object> found = toDelete.valueMap();
                if (KeyHelper.matchesPrimaryKey(primary, data, found)) {
                    c.deleteRow();
                    toDelete.clear();
                    return true;
                }
            }

            return false;
        } finally {
            if (toDelete != null) {
                toDelete.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }
}
