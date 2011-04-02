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

    public enum TraversalMode {
        READ_ONLY, READ_WRITE;
    }

    public enum MutationType {
        NONE, INSERT_OR_UPDATE, DELETE;
    }

    public static class Target {
        private final TableDef tableDef;
        private final String indexDef;

        public Target(TableDef tableDef) {
            this(tableDef, null);
        }

        public Target(TableDef tableDef, String indexDef) {
            this.tableDef = tableDef;
            this.indexDef = indexDef;
        }

        public TableDef getTableDef() {
            return tableDef;
        }

        public String getIndexDef() {
            return indexDef;
        }
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

    public static <T> void foreach(final Transaction txn, final Target target,
            final Map<String, Object> firstKey, final Filter primaryFilter,
            final Filter filter, final Mapping<T> r) {
        map(txn, target, firstKey, primaryFilter, filter, r).traverseAll();
    }

    public static <T> Traversal<T> map(final Transaction txn,
            final Target target, final Map<String, Object> firstKey,
            final Filter primaryFilter, final Filter filter,
            final Mapping<T> mapping) {
        return new TraversalImpl<T>(txn, target, TraversalMode.READ_ONLY,
                firstKey, primaryFilter, filter, mapping);
    }

    public static <T> T reduce(final Transaction txn, final Target target,
            final Map<String, Object> firstKey, final Filter primaryFilter,
            final Filter filter, final Reduction<T> reduction, final T initial) {

        MapReduction<T> mr = new MapReduction<T>(initial, reduction);
        Traversal<T> iter = map(txn, target, firstKey, primaryFilter, filter,
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
            final Target target, final DatabaseTemplate dbt,
            final Map<String, Object> firstKey, final Filter primaryFilter,
            final Filter filter, final Mapping<Mutation> mutation) {

        final Mapping<Mutation> mapping = new Mapping<Functional.Mutation>() {
            @Override
            public Mutation map(Map<String, Object> row) {
                Mutation m = mutation.map(row);

                switch (m.getType()) {
                case NONE:
                    break;
                case INSERT_OR_UPDATE:
                    dbt.insertOrUpdate(txn, target.getTableDef(),
                            m.getInstance());
                    break;
                case DELETE:
                    dbt.delete(txn, target.getTableDef(), m.getInstance());
                    break;
                default:
                    throw new IllegalArgumentException();
                }

                return m;
            }
        };

        return new TraversalImpl<Mutation>(txn, target,
                TraversalMode.READ_WRITE, firstKey, primaryFilter, filter,
                mapping);
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

    private static class TraversalImpl<T> implements Traversal<T> {
        private final boolean isSecondary;
        private final boolean isReadOnly;
        private final TableDef tableDef;
        private final IndexDef indexDef;
        private final Filter primaryFilter;
        private final Filter filter;
        private final Mapping<T> mapping;

        private Cursor c0;
        private Cursor c1;
        private Map<String, Object> nextItem;

        public TraversalImpl(Transaction txn, Target target,
                TraversalMode traversalMode, Map<String, Object> firstKey,
                Filter primaryFilter, Filter filter, Mapping<T> mapping) {
            this.primaryFilter = primaryFilter;
            this.filter = filter;
            this.mapping = mapping;

            this.isReadOnly = traversalMode.equals(TraversalMode.READ_ONLY);
            this.isSecondary = target.getIndexDef() != null;
            this.tableDef = target.getTableDef();
            this.c0 = txn.openTable(tableDef);

            if (this.isSecondary) {
                this.c1 = c0.openIndex(target.getIndexDef());
                this.c1.setClusterAccess();
                this.indexDef = tableDef.getIndexDef(target.getIndexDef());
            } else {
                this.c1 = this.c0;
                this.indexDef = target.getTableDef().getPrimaryIndex();
            }

            if (!this.isReadOnly) {
                try {
                    this.c1.setLockMode(LockMode.INTENTION_EXCLUSIVE);
                    this.c1.lock(LockMode.LOCK_EXCLUSIVE);
                } catch (Exception e) {
                    this.close();

                    throw new RuntimeException(e);
                }
            }

            TupleBuilder tpl = KeyHelper.createTupleBuilder(tableDef,
                    indexDef.getColumns(), firstKey);

            Tuple tuple = isSecondary ? c1.createSecondaryIndexSearchTuple(tpl)
                    : c1.createClusteredIndexSearchTuple(tpl);

            c1.find(tuple, SearchMode.GE);
            nextItem = advance();
        }

        private Map<String, Object> advance() {
            Map<String, Object> toReturn = null;
            while (c1.isPositioned() && c1.hasNext()) {
                Tuple read = c1.createClusteredIndexReadTuple();
                try {
                    c1.readRow(read);
                    Map<String, Object> row = read.valueMap();

                    if (!primaryFilter.map(row)) {
                        close();
                        break;
                    }

                    if (filter == null || filter.map(row)) {
                        toReturn = row;
                        break;
                    }
                } catch (Exception e) {
                    close();

                    throw new RuntimeException(e);
                } finally {
                    read.delete();
                    if (c1 != null) {
                        c1.next();
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

            try {
                return mapping.map(orig);
            } catch (Exception e) {
                close();

                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (c1 != null) {
                c1.close();
                c1 = null;
            }

            if (isSecondary && c0 != null) {
                c0.close();
                c0 = null;
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
    }
}
