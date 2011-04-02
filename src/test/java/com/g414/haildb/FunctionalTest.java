package com.g414.haildb;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Mutation;
import com.g414.haildb.tpl.Functional.MutationType;
import com.g414.haildb.tpl.Functional.Reduction;

@Test
public class FunctionalTest {
    private Database db = new Database();
    private DatabaseTemplate dt = new DatabaseTemplate(db);

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        db.createDatabase(G414InnoDBTableDefs.SCHEMA_NAME);
        G414InnoDBTableDefs.createTables(db);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        G414InnoDBTableDefs.clearTables(db);
    }

    public void testStuff() throws Exception {
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

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.foreach(txn, G414InnoDBTableDefs.TABLE_3,
                                primary, primaryFilter, filter, m);

                        return null;
                    }
                });

        Assert.assertEquals(6, found.get());

        found.set(0);

        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    public Void inTransaction(Transaction txn) {
                        Functional.map(txn, G414InnoDBTableDefs.TABLE_3,
                                primary, primaryFilter, filter, m)
                                .traverseAll();

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
                        return Functional.reduce(txn,
                                G414InnoDBTableDefs.TABLE_3, primary,
                                primaryFilter, filter, r, sum);
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
                        Functional.apply(txn, G414InnoDBTableDefs.TABLE_3, dt,
                                primary, primaryFilter, filter, a)
                                .traverseAll();

                        return null;
                    }
                });

        final Integer sum2 = 0;

        final Integer val2 = dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Transaction txn) {
                        return Functional.reduce(txn,
                                G414InnoDBTableDefs.TABLE_3, primary,
                                primaryFilter, filter, r, sum2);
                    }
                });

        Assert.assertEquals(24, val2.intValue());
    }

    private void populate() throws Exception {
        dt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        for (int i = 0; i < 6; i++) {
                            for (int j = 0; j < 6; j++) {
                                for (int k = 0; k < 2; k++) {
                                    Map<String, Object> m = new HashMap<String, Object>();
                                    m.put("a", i);
                                    m.put("b", k);
                                    m.put("c", j);
                                    m.put("d", j);

                                    dt.insert(txn, G414InnoDBTableDefs.TABLE_3,
                                            m);
                                }
                            }
                        }

                        return null;
                    }
                });
    }
}
