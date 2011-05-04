package com.g414.haildb;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.haildb.Cursor.CursorDirection;
import com.g414.haildb.Cursor.SearchMode;
import com.g414.haildb.Transaction.TransactionLevel;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Mutation;
import com.g414.haildb.tpl.Functional.MutationType;
import com.g414.haildb.tpl.Functional.Reduction;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.Traversal;
import com.g414.haildb.tpl.Functional.TraversalSpec;

@Test
public class FunctionalTest {
    private Database db = new Database();
    private DatabaseTemplate dt = new DatabaseTemplate(db);

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        db.createDatabase(TableDefinitions.SCHEMA_NAME);
        TableDefinitions.createTables(db);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        TableDefinitions.clearTables(db);
    }

    public void testTableCreateDelete() throws Exception {
        for (int i = 0; i < 1000; i++) {
            TableDef def = (new TableBuilder(String.format("%s/atable%02d",
                    TableDefinitions.SCHEMA_NAME, i))).addColumn("a",
                    ColumnType.INT, 4).build();
            db.createTable(def);
            db.dropTable(def);
        }
    }

    public void testDoubleDeletion() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Cursor c = txn.openTable(TableDefinitions.TABLE_1);
                        Tuple t = c.createClusteredIndexReadTuple();
                        t.delete();

                        try {
                            t.delete();
                            Assert.fail("expected exception!");
                        } catch (IllegalStateException expected) {
                        } finally {
                            c.close();
                        }

                        return null;
                    }
                });
    }

    public void testClearAfterDeletion() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Cursor c = txn.openTable(TableDefinitions.TABLE_1);
                        Tuple t = c.createClusteredIndexReadTuple();
                        t.delete();

                        try {
                            t.clear();
                            Assert.fail("expected exception!");
                        } catch (IllegalStateException expected) {
                        } finally {
                            c.close();
                        }

                        return null;
                    }
                });
    }

    public void testException() throws Exception {
        try {
            dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                    new TransactionCallback<Void>() {
                        @Override
                        public Void inTransaction(Transaction txn) {
                            throw new IllegalStateException();
                        }
                    });
            throw new RuntimeException("fail");
        } catch (IllegalStateException expected) {
            // cool
        }
    }

    public void testInsertDuplicateFail() throws Exception {
        try {
            dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                    new TransactionCallback<Void>() {
                        @Override
                        public Void inTransaction(Transaction txn) {
                            Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                            p2.put("a", -1L);
                            p2.put("b", -1L);
                            p2.put("c", -1L);
                            p2.put("d", -1L);
                            p2.put("e", "t");
                            p2.put("f", "f".getBytes());

                            dt.insert(txn, TableDefinitions.TABLE_3, p2);
                            dt.insert(txn, TableDefinitions.TABLE_3, p2);

                            return null;
                        }
                    });
            throw new RuntimeException("fail");
        } catch (InnoException expected) {
            // cool
        }
    }

    public void testInsertUpdateOK() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);
                        p2.put("d", -1L);
                        p2.put("e", "t");
                        p2.put("f", "f".getBytes());

                        dt.insert(txn, TableDefinitions.TABLE_3, p2);
                        dt.update(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testUpdateInsertFail() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);
                        p2.put("d", -1L);
                        p2.put("e", "t");
                        p2.put("f", "f".getBytes());

                        Assert.assertFalse(dt.update(txn,
                                TableDefinitions.TABLE_3, p2));

                        dt.insert(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testInsertDeleteOK() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);
                        p2.put("d", -1L);
                        p2.put("e", "t");
                        p2.put("f", "f".getBytes());

                        dt.insert(txn, TableDefinitions.TABLE_3, p2);
                        dt.delete(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testDeleteNothingOK() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);

                        dt.delete(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testDelete() throws Exception {
        populate();

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", 0L);
                        p2.put("b", 0L);
                        p2.put("c", 0L);

                        dt.delete(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testInsertOrUpdateDuplicateOK() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", 1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);
                        p2.put("d", -1L);
                        p2.put("e", "t");
                        p2.put("f", "f".getBytes());

                        dt.insertOrUpdate(txn, TableDefinitions.TABLE_3, p2);
                        dt.insertOrUpdate(txn, TableDefinitions.TABLE_3, p2);

                        return null;
                    }
                });
    }

    public void testEmptyLoad() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);

                        Assert.assertNull(dt.load(txn,
                                TableDefinitions.TABLE_3, p2));

                        return null;
                    }
                });
    }

    public void testLoad() throws Exception {
        populate();

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Map<String, Object> p1 = new LinkedHashMap<String, Object>();
                        p1.put("a", 0L);
                        p1.put("b", 0L);
                        p1.put("c", 0L);

                        Assert.assertNotNull(dt.load(txn,
                                TableDefinitions.TABLE_3, p1));

                        Map<String, Object> p2 = new LinkedHashMap<String, Object>();
                        p2.put("a", -1L);
                        p2.put("b", -1L);
                        p2.put("c", -1L);

                        Assert.assertNull(dt.load(txn,
                                TableDefinitions.TABLE_3, p2));

                        Map<String, Object> p3 = new LinkedHashMap<String, Object>();
                        p3.put("a", 0L);
                        p3.put("b", 0L);
                        p3.put("c", 999L);

                        Assert.assertNull(dt.load(txn,
                                TableDefinitions.TABLE_3, p3));

                        return null;
                    }
                });
    }

    public void testAscendingTraversal() throws Exception {
        populate();

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return true;
            }
        };

        final Mapping<Map<String, Object>> m = new Mapping<Map<String, Object>>() {
            public Map<String, Object> map(Map<String, Object> row) {
                return row;
            };
        };

        final TraversalSpec spec = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3), null, primaryFilter, null);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Traversal<Map<String, Object>> iter = Functional.map(
                                txn, spec, m);

                        try {
                            Assert.assertEquals(
                                    mapOf("a", 0, "b", 0, "c", 0, "d", 0, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                            Assert.assertEquals(
                                    mapOf("a", 0, "b", 0, "c", 1, "d", 1, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                            Assert.assertEquals(
                                    mapOf("a", 0, "b", 0, "c", 2, "d", 2, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                        } finally {
                            iter.close();
                        }
                        return null;
                    }
                });
    }

    public void testDescendingTraversal() throws Exception {
        populate();

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return true;
            }
        };

        final Mapping<Map<String, Object>> m = new Mapping<Map<String, Object>>() {
            public Map<String, Object> map(Map<String, Object> row) {
                return row;
            };
        };

        final TraversalSpec spec = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3), CursorDirection.DESC, SearchMode.LE,
                null, primaryFilter, null);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Traversal<Map<String, Object>> iter = Functional.map(
                                txn, spec, m);

                        try {
                            Assert.assertEquals(
                                    mapOf("a", 5, "b", 1, "c", 5, "d", 5, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                            Assert.assertEquals(
                                    mapOf("a", 5, "b", 1, "c", 4, "d", 4, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                            Assert.assertEquals(
                                    mapOf("a", 5, "b", 1, "c", 3, "d", 3, "e",
                                            "t", "f", null).toString(), iter
                                            .next().toString());
                        } finally {
                            iter.close();
                        }
                        return null;
                    }
                });
    }

    public void testPrimaryIndex() throws Exception {
        populate();

        final Map<String, Object> primary = new HashMap<String, Object>();
        primary.put("a", 3);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("a")).intValue() < 5;
            }
        };

        final Filter filter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("b")).intValue() % 2 == 1
                        && ((Number) row.get("c")).intValue() % 2 == 0;
            }
        };

        final AtomicLong found = new AtomicLong();

        final Mapping<Void> m = new Mapping<Void>() {
            public Void map(Map<String, Object> row) {
                found.getAndIncrement();

                return null;
            };
        };

        final TraversalSpec spec = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3), primary, primaryFilter, filter);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.foreach(txn, spec, m);

                        return null;
                    }
                });

        Assert.assertEquals(6, found.get());

        found.set(0);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.map(txn, spec, m).traverseAll();

                        return null;
                    }
                });

        Assert.assertEquals(6, found.get());

        final Reduction<Integer> r = new Reduction<Integer>() {
            public Integer reduce(Map<String, Object> row, Integer initial) {
                return initial + ((Number) row.get("d")).intValue();
            }
        };

        final Integer sum = 0;

        Integer val = dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Transaction txn) {
                        return Functional.reduce(txn, spec, r, sum);
                    }
                });

        Assert.assertEquals(12, val.intValue());

        final Mapping<Mutation> a = new Mapping<Mutation>() {
            public Mutation map(Map<String, Object> row) {
                int d = ((Number) row.get("d")).intValue();

                Map<String, Object> newMap = new LinkedHashMap<String, Object>();
                newMap.putAll(row);
                newMap.put("d", d * 2);

                return new Mutation(MutationType.INSERT_OR_UPDATE, newMap);
            };
        };

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(txn, dt, spec, a).traverseAll();

                        return null;
                    }
                });

        final Integer sum2 = 0;

        final Integer val2 = dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Transaction txn) {
                        return Functional.reduce(txn, spec, r, sum2);
                    }
                });

        Assert.assertEquals(24, val2.intValue());
    }

    public void testSecondaryIndex() throws Exception {
        populate();

        final Map<String, Object> primary = new HashMap<String, Object>();
        primary.put("b", 1);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("b")).intValue() == 1;
            }
        };

        final AtomicLong found = new AtomicLong();

        final Mapping<Void> m = new Mapping<Void>() {
            public Void map(Map<String, Object> row) {
                found.getAndIncrement();

                return null;
            };
        };

        final TraversalSpec spec0 = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3, "bc"), primary, primaryFilter, null);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.foreach(txn, spec0, m);

                        return null;
                    }
                });

        Assert.assertEquals(36, found.get());

        found.set(0);

        final Filter filter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("b")).intValue() % 2 == 1
                        && ((Number) row.get("a")).intValue() % 2 == 0;
            }
        };

        final TraversalSpec spec1 = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3, "bc"), primary, primaryFilter, filter);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.map(txn, spec1, m).traverseAll();

                        return null;
                    }
                });

        Assert.assertEquals(18, found.get());

        final Reduction<Integer> r = new Reduction<Integer>() {
            public Integer reduce(Map<String, Object> row, Integer initial) {
                return initial + ((Number) row.get("d")).intValue();
            }
        };

        final Integer sum = 0;

        Integer val = dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Transaction txn) {
                        return Functional.reduce(txn, spec1, r, sum);
                    }
                });

        Assert.assertEquals(45, val.intValue());

        final Mapping<Mutation> a = new Mapping<Mutation>() {
            public Mutation map(Map<String, Object> row) {
                int d = ((Number) row.get("d")).intValue();

                Map<String, Object> newMap = new LinkedHashMap<String, Object>();
                newMap.putAll(row);
                newMap.put("d", d * 2);

                return new Mutation(MutationType.INSERT_OR_UPDATE, newMap);
            };
        };

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(txn, dt, spec1, a).traverseAll();

                        return null;
                    }
                });

        final Integer sum2 = 0;

        final Integer val2 = dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Transaction txn) {
                        return Functional.reduce(txn, spec1, r, sum2);
                    }
                });

        Assert.assertEquals(90, val2.intValue());
    }

    public void testNoneMutation() throws Exception {
        populate();

        final Map<String, Object> primary = new HashMap<String, Object>();
        primary.put("b", 1);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("b")).intValue() == 1;
            }
        };

        final TraversalSpec spec = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3, "bc"), primary, primaryFilter, null);

        final Mapping<Mutation> a = new Mapping<Mutation>() {
            public Mutation map(Map<String, Object> row) {
                return new Mutation(MutationType.NONE, null);
            };
        };

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(txn, dt, spec, a).traverseAll();

                        return null;
                    }
                });
    }

    public void testDeleteMutation() throws Exception {
        populate();

        final Map<String, Object> primary = new HashMap<String, Object>();
        primary.put("b", 1);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("b")).intValue() == 1;
            }
        };

        final TraversalSpec spec = new TraversalSpec(new Target(
                TableDefinitions.TABLE_3, "bc"), primary, primaryFilter, null);

        final Mapping<Mutation> a = new Mapping<Mutation>() {
            public Mutation map(Map<String, Object> row) {
                return new Mutation(MutationType.DELETE, row);
            };
        };

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(txn, dt, spec, a).traverseAll();

                        return null;
                    }
                });
    }

    private void populate() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        for (int i = 0; i < 6; i++) {
                            for (int j = 0; j < 6; j++) {
                                for (int k = 0; k < 2; k++) {
                                    Map<String, Object> m = new LinkedHashMap<String, Object>();
                                    m.put("a", i);
                                    m.put("b", k);
                                    m.put("c", j);
                                    m.put("d", j);
                                    m.put("e", "t");
                                    m.put("f", null);

                                    dt.insert(txn, TableDefinitions.TABLE_3, m);
                                }
                            }
                        }

                        return null;
                    }
                });
    }

    private Map<String, Object> mapOf(Object... vals) {
        if (vals == null) {
            return Collections.emptyMap();
        }

        if (vals.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "must have even number of keys/values");
        }

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (int i = 0; i < vals.length; i += 2) {
            String key = (String) vals[i];
            Object value = vals[i + 1];

            result.put(key, value);
        }

        return Collections.unmodifiableMap(result);
    }
}
