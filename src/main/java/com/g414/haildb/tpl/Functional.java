package com.g414.haildb.tpl;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

import com.g414.haildb.Cursor;
import com.g414.haildb.IndexDef;
import com.g414.haildb.LockMode;
import com.g414.haildb.SearchMode;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.Tuple;
import com.g414.haildb.TupleBuilder;

public class Functional {
    public interface Traversal<T> extends Closeable, Iterator<T> {
        public void traverseAll();

        @Override
        public void close();
    }

    public enum MutationType {
        NONE, INSERT_OR_UPDATE, DELETE;
    }

    public static class Mutation {
        private final MutationType type;
        private final Map<String, Object> instance;

        public Mutation(MutationType type, Map<String, Object> instance) {
            this.type = type;
            this.instance = instance;
        }

        public MutationType getType() {
            return type;
        }

        public Map<String, Object> getInstance() {
            return instance;
        }
    }

    public interface Mapping<T> {
        public T map(Map<String, Object> row);
    }

    public interface Reduction<T> {
        public T reduce(Map<String, Object> row, T initial);
    }

    public interface Filter extends Mapping<Boolean> {
    }

    public static <T> void foreach(final Transaction txn,
            final TableDef tableDef, final Map<String, Object> firstKey,
            final Filter primaryFilter, final Filter filter, final Mapping<T> r) {
        Traversal<T> iter = map(txn, tableDef, firstKey, primaryFilter, filter,
                r);
        iter.traverseAll();
    }

    public static <T> Traversal<T> map(final Transaction txn,
            final TableDef tableDef, final Map<String, Object> firstKey,
            final Filter primaryFilter, final Filter filter, final Mapping<T> r) {

        return new Traversal<T>() {
            Cursor c = txn.openTable(tableDef);
            IndexDef primary = tableDef.getPrimaryIndex();

            {
                TupleBuilder tpl = KeyHelper.createTupleBuilder(tableDef,
                        primary.getColumns(), firstKey);

                Tuple tuple = c.createClusteredIndexSearchTuple(tpl);
                c.find(tuple, SearchMode.GE);
            }

            Map<String, Object> nextItem = advance();

            private Map<String, Object> advance() {
                Map<String, Object> toReturn = null;
                while (c.isPositioned() && c.hasNext()) {
                    Tuple read = c.createClusteredIndexReadTuple();
                    try {
                        c.readRow(read);
                        Map<String, Object> row = read.valueMap();

                        if (!primaryFilter.map(row)) {
                            close();
                            break;
                        }

                        if (filter == null || filter.map(row)) {
                            toReturn = row;
                            break;
                        }
                    } finally {
                        read.delete();
                        if (c != null) {
                            c.next();
                        }
                    }
                }

                return toReturn;
            }

            @Override
            public boolean hasNext() {
                return nextItem != null;
            }

            public T next() {
                Map<String, Object> orig = nextItem;

                nextItem = advance();

                return r.map(orig);
            }

            @Override
            public void close() {
                if (c != null) {
                    c.close();
                    c = null;
                }
            }

            @Override
            public void traverseAll() {
                while (hasNext()) {
                    next();
                }

                close();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static <T> T reduce(final Transaction txn, final TableDef tableDef,
            final Map<String, Object> firstKey, final Filter primaryFilter,
            final Filter filter, final Reduction<T> r, final T initial) {

        MapReduction<T> mr = new MapReduction<T>(initial, r);
        Traversal<T> iter = map(txn, tableDef, firstKey, primaryFilter, filter,
                mr);
        try {
            while (iter.hasNext()) {
                iter.next();
            }

            return mr.getAccum();

        } finally {
            iter.close();
        }
    }

    public static Traversal<Mutation> apply(final Transaction txn,
            final TableDef tableDef, final DatabaseTemplate dbt,
            final Map<String, Object> firstKey, final Filter primaryFilter,
            final Filter filter, final Mapping<Mutation> r) {

        return new Traversal<Mutation>() {
            IndexDef primary = tableDef.getPrimaryIndex();
            Cursor c = txn.openTable(tableDef);

            {
                c.setLockMode(LockMode.INTENTION_EXCLUSIVE);
                c.lock(LockMode.LOCK_EXCLUSIVE);

                TupleBuilder tpl = KeyHelper.createTupleBuilder(tableDef,
                        primary.getColumns(), firstKey);

                Tuple tuple = c.createClusteredIndexSearchTuple(tpl);
                c.find(tuple, SearchMode.GE);
            }

            Map<String, Object> nextItem = advance();

            private Map<String, Object> advance() {
                Map<String, Object> toReturn = null;
                while (c.isPositioned() && c.hasNext()) {
                    Tuple read = c.createClusteredIndexReadTuple();
                    try {
                        c.readRow(read);
                        Map<String, Object> row = read.valueMap();

                        if (!primaryFilter.map(row)) {
                            try {
                                close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }

                            break;
                        }

                        if (filter == null || filter.map(row)) {
                            toReturn = row;
                            break;
                        }
                    } finally {
                        read.delete();
                        if (c != null) {
                            c.next();
                        }
                    }
                }

                return toReturn;
            }

            @Override
            public boolean hasNext() {
                return nextItem != null;
            }

            @Override
            public Mutation next() {
                Map<String, Object> orig = nextItem;

                nextItem = advance();

                Mutation m = r.map(orig);

                switch (m.getType()) {
                case NONE:
                    break;
                case INSERT_OR_UPDATE:
                    dbt.insertOrUpdate(txn, tableDef, m.getInstance());
                    break;
                case DELETE:
                    dbt.delete(txn, tableDef, m.getInstance());
                    break;
                default:
                    throw new IllegalArgumentException();
                }

                return m;
            }

            @Override
            public void close() {
                if (c != null) {
                    c.close();
                    c = null;
                }
            }

            @Override
            public void traverseAll() {
                while (hasNext()) {
                    next();
                }

                close();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class MapReduction<T> implements Mapping<T> {
        T accum;
        Reduction<T> r;

        public MapReduction(T initial, Reduction<T> r) {
            accum = initial;
            this.r = r;
        }

        public T map(Map<String, Object> row) {
            accum = r.reduce(row, accum);

            return accum;
        }

        public T getAccum() {
            return accum;
        }
    }
}
