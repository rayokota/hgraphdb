/*
 * This file is licensed to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package org.apache.hadoop.hbase.client.mock;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.mockito.AdditionalAnswers.delegatesTo;

/**
 * MockHTable.
 *
 * original MockHTable (by agaoglu) : https://gist.github.com/agaoglu/613217#file_mock_h_table.java
 *
 * Modifications
 *
 * <ul>
 *     <li>fix filter (by k-mack) : https://gist.github.com/k-mack/4600133</li>
 *     <li>fix batch() : implement all mutate operation and fix result[] count.</li>
 *     <li>fix exists()</li>
 *     <li>fix increment() : wrong return value</li>
 *     <li>check columnFamily</li>
 *     <li>implement mutateRow()</li>
 *     <li>implement getTableName()</li>
 *     <li>implement getTableDescriptor()</li>
 *     <li>throws RuntimeException when unimplemented method was called.</li>
 *     <li>remove some methods for loading data, checking values ...</li>
 * </ul>
 */
public class MockHTable implements Table {
    private static final Logger LOG = LoggerFactory.getLogger(MockHTable.class);

    private final TableName tableName;
    private final List<String> columnFamilies = new ArrayList<>();
    private Configuration config;

    private final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> data =
        new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

    @SuppressWarnings("WeakerAccess")
    public MockHTable(TableName tableName) {
        this.tableName = tableName;
    }

    public MockHTable(TableName tableName, String... columnFamilies) {
        this.tableName = tableName;
        this.columnFamilies.addAll(Arrays.asList(columnFamilies));
    }

    @SuppressWarnings("WeakerAccess")
    public MockHTable(TableName tableName, List<String> columnFamilies) {
        this.tableName = tableName;
        this.columnFamilies.addAll(columnFamilies);
    }

    public void clear() {
        data.clear();
    }

    public byte[] getTableName() {
        return getName().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableName getName() {
        return tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration getConfiguration() {
        return config;
    }

    public MockHTable setConfiguration(Configuration config) {
        this.config = config;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDescriptor getDescriptor() throws IOException {
        HTableDescriptor table = new HTableDescriptor(tableName);
        for (String columnFamily : columnFamilies) {
            table.addFamily(new HColumnDescriptor(columnFamily));
        }
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result mutateRow(RowMutations rm) throws IOException {
        // currently only support Put and Delete
        long maxTs = System.currentTimeMillis();
        for (Mutation mutation : rm.getMutations()) {
            if (mutation instanceof Put) {
                put((Put) mutation);
            } else if (mutation instanceof Delete) {
                delete((Delete) mutation);
            }
            long ts = mutation.getTimestamp();
            if (ts != HConstants.LATEST_TIMESTAMP && ts > maxTs) maxTs = ts;
        }
        long now = System.currentTimeMillis();
        if (now <= maxTs) {
            // we have intentionally set the ts in the future, so wait
            try {
                Thread.sleep(maxTs - now + 1);
            } catch (InterruptedException ignored) { }
        }
        // results of Increment/Append operations, which are currently not supported
        return Result.EMPTY_RESULT;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result append(Append append) throws IOException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    private static List<Cell> toKeyValue(byte[] row, NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowdata, TimeRange timeRange, int maxVersions) {
        List<Cell> ret = new ArrayList<>();
        for (byte[] family : rowdata.keySet()) {
            for (byte[] qualifier : rowdata.get(family).keySet()) {
                int versionsAdded = 0;
                for (Map.Entry<Long, byte[]> tsToVal : rowdata.get(family).get(qualifier).descendingMap().entrySet()) {
                    if (versionsAdded == maxVersions)
                        break;
                    Long timestamp = tsToVal.getKey();
                    if (!timeRange.withinTimeRange(timestamp))
                        continue;
                    byte[] value = tsToVal.getValue();
                    ret.add(new KeyValue(row, family, qualifier, timestamp, value));
                    versionsAdded++;
                }
            }
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) throws IOException {
        Result result = get(get);
        return result != null && !result.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean[] exists(List<Get> gets) throws IOException {
        boolean[] result = new boolean[gets.size()];
        for (int i = 0; i < gets.size(); i++) {
            result[i] = exists(gets.get(i));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        Object[] rows = batch(actions);
        if (results != null) {
            System.arraycopy(rows, 0, results, 0, rows.length);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        Object[] results = new Object[actions.size()]; // same size.
        for (int i = 0; i < actions.size(); i++) {
            Row r = actions.get(i);
            if (r instanceof Delete) {
                delete((Delete) r);
                results[i] = new Result();
            }
            if (r instanceof Put) {
                put((Put) r);
                results[i] = new Result();
            }
            if (r instanceof Get) {
                Result result = get((Get) r);
                results[i] = result;
            }
            if (r instanceof Increment) {
                Result result = increment((Increment) r);
                results[i] = result;
            }
            if (r instanceof Append) {
                Result result = append((Append) r);
                results[i] = result;
            }
        }
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> void batchCallback(
            final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
            throws IOException, InterruptedException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result get(Get get) throws IOException {
        byte[] row = get.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (rowData == null) {
            return new Result();
        }
        List<Cell> kvs = new ArrayList<>();
        Filter filter = get.getFilter();
        int maxResults = get.getMaxResultsPerColumnFamily();

        if (!get.hasFamilies()) {
            kvs = toKeyValue(row, rowData, get.getTimeRange(), get.getMaxVersions());
            if (filter != null) {
                kvs = filter(filter, kvs);
            }
            if (maxResults >= 0 && kvs.size() > maxResults) {
                kvs = kvs.subList(0, maxResults);
            }
        } else {
            for (byte[] family : get.getFamilyMap().keySet()) {
                if (rowData.get(family) == null)
                    continue;
                NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
                if (qualifiers == null || qualifiers.isEmpty())
                    qualifiers = rowData.get(family).navigableKeySet();
                List<Cell> familyKvs = new ArrayList<>();
                for (byte[] qualifier : qualifiers) {
                    if (qualifier == null)
                        qualifier = "".getBytes();
                    if (!rowData.containsKey(family) ||
                            !rowData.get(family).containsKey(qualifier) ||
                            rowData.get(family).get(qualifier).isEmpty())
                        continue;
                    Map.Entry<Long, byte[]> timestampAndValue = rowData.get(family).get(qualifier).lastEntry();
                    familyKvs.add(new KeyValue(row, family, qualifier, timestampAndValue.getKey(), timestampAndValue.getValue()));
                }
                if (filter != null) {
                    familyKvs = filter(filter, familyKvs);
                }
                if (maxResults >= 0 && familyKvs.size() > maxResults) {
                    familyKvs = familyKvs.subList(0, maxResults);
                }
                kvs.addAll(familyKvs);
            }
        }
        return Result.create(kvs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result[] get(List<Get> gets) throws IOException {
        List<Result> results = new ArrayList<>();
        for (Get g : gets) {
            results.add(get(g));
        }
        return results.toArray(new Result[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        final List<Result> ret = new ArrayList<>();
        byte[] st = scan.getStartRow();
        byte[] sp = scan.getStopRow();
        Filter filter = scan.getFilter();
        int maxResults = scan.getMaxResultsPerColumnFamily();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> subData =
            scan.isReversed() ? data.descendingMap() : data;

        if (st == null || st.length == 0) {
            if (sp != null && sp.length > 0) {
                subData = subData.headMap(sp, scan.includeStopRow());
            }
        } else if (sp == null || sp.length == 0) {
            subData = subData.tailMap(st, scan.includeStartRow());
        } else {
            boolean includeStopRow = scan.includeStopRow();
            if (Arrays.equals(st, sp)) {
                includeStopRow = true;
            }
            subData = subData.subMap(st, scan.includeStartRow(), sp, includeStopRow);
        }

        for (byte[] row : subData.keySet()) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = subData.get(row);
            List<Cell> kvs;
            if (!scan.hasFamilies()) {
                kvs = toKeyValue(row, rowData, scan.getTimeRange(), scan.getMaxVersions());
                if (filter != null) {
                    kvs = filter(filter, kvs);
                }
                if (maxResults >= 0 && kvs.size() > maxResults) {
                    kvs = kvs.subList(0, maxResults);
                }
            } else {
                kvs = new ArrayList<>();
                for (byte[] family : scan.getFamilyMap().keySet()) {
                    if (rowData.get(family) == null)
                        continue;
                    NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(family);
                    if (qualifiers == null || qualifiers.isEmpty())
                        qualifiers = rowData.get(family).navigableKeySet();
                    List<Cell> familyKvs = new ArrayList<>();
                    for (byte[] qualifier : qualifiers) {
                        if (rowData.get(family).get(qualifier) == null)
                            continue;
                        List<KeyValue> tsKvs = new ArrayList<>();
                        for (Long timestamp : rowData.get(family).get(qualifier).descendingKeySet()) {
                            if (timestamp < scan.getTimeRange().getMin() || timestamp > scan.getTimeRange().getMax())
                                continue;
                            byte[] value = rowData.get(family).get(qualifier).get(timestamp);
                            tsKvs.add(new KeyValue(row, family, qualifier, timestamp, value));
                            if (tsKvs.size() == scan.getMaxVersions()) {
                                break;
                            }
                        }
                        familyKvs.addAll(tsKvs);
                    }
                    if (filter != null) {
                        familyKvs = filter(filter, familyKvs);
                    }
                    if (maxResults >= 0 && familyKvs.size() > maxResults) {
                        familyKvs = familyKvs.subList(0, maxResults);
                    }
                    kvs.addAll(familyKvs);
                }
            }
            if (!kvs.isEmpty()) {
                ret.add(Result.create(kvs));
            }
            // Check for early out optimization
            if (filter != null && filter.filterAllRemaining()) {
                break;
            }
        }

        return new ResultScanner() {
            private final Iterator<Result> iterator = ret.iterator();

            public Iterator<Result> iterator() {
                return iterator;
            }

            public Result[] next(int nbRows) throws IOException {
                ArrayList<Result> resultSets = new ArrayList<>(nbRows);
                for (int i = 0; i < nbRows; i++) {
                    Result next = next();
                    if (next != null) {
                        resultSets.add(next);
                    } else {
                        break;
                    }
                }
                return resultSets.toArray(new Result[0]);
            }

            public Result next() throws IOException {
                try {
                    return iterator().next();
                } catch (NoSuchElementException e) {
                    return null;
                }
            }

            public void close() {
            }

            public ScanMetrics getScanMetrics() {
                return null;
            }

            public boolean renewLease() {
                return false;
            }
        };
    }

    /**
     * Follows the logical flow through the filter methods for a single row.
     *
     * @param filter HBase filter.
     * @param kvs    List of a row's KeyValues
     * @return List of KeyValues that were not filtered.
     */
    private List<Cell> filter(Filter filter, List<Cell> kvs) throws IOException {
        filter.reset();

        List<Cell> tmp = new ArrayList<>(kvs.size());
        tmp.addAll(kvs);

      /*
       * Note. Filter flow for a single row. Adapted from
       * "HBase: The Definitive Guide" (p. 163) by Lars George, 2011.
       * See Figure 4-2 on p. 163.
       */
        boolean filteredOnRowKey = false;
        List<Cell> nkvs = new ArrayList<>(tmp.size());
        for (Cell kv : tmp) {
            if (filter.filterRowKey(kv)) {
                filteredOnRowKey = true;
                break;
            }
            Filter.ReturnCode filterResult = filter.filterCell(kv);
            if (filterResult == Filter.ReturnCode.INCLUDE || filterResult == Filter.ReturnCode.INCLUDE_AND_NEXT_COL) {
                nkvs.add(filter.transformCell(kv));
            } else if (filterResult == Filter.ReturnCode.NEXT_ROW) {
                break;
            } else if (filterResult == Filter.ReturnCode.NEXT_COL || filterResult == Filter.ReturnCode.SKIP) {
                //noinspection UnnecessaryContinue
                continue;
            }
          /*
           * Ignoring next key hint which is a optimization to reduce file
           * system IO
           */
        }
        if (filter.hasFilterRow() && !filteredOnRowKey) {
            filter.filterRowCells(nkvs);
        }
        if (filter.filterRow() || filteredOnRowKey) {
            nkvs.clear();
        }
        tmp = nkvs;
        return tmp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(scan);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(scan);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Put put) throws IOException {
        byte[] row = put.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData =
            data.computeIfAbsent(row, k -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
        for (byte[] family : put.getFamilyCellMap().keySet()) {
            if (!columnFamilies.contains(new String(family))) {
                throw new RuntimeException("Not Exists columnFamily : " + new String(family));
            }
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData =
                rowData.computeIfAbsent(family, k -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
            for (Cell kv : put.getFamilyCellMap().get(family)) {
                long ts = kv.getTimestamp();
                if (ts == HConstants.LATEST_TIMESTAMP) {
                    ts = put.getTimestamp();
                }
                if (ts == HConstants.LATEST_TIMESTAMP) {
                    ts = System.currentTimeMillis();
                }
                byte[] qualifier = CellUtil.cloneQualifier(kv);
                NavigableMap<Long, byte[]> qualifierData =
                    familyData.computeIfAbsent(qualifier, k -> new ConcurrentSkipListMap<>());
                qualifierData.put(ts, CellUtil.cloneValue(kv));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(put);
        }
    }

    private boolean check(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (value == null)
            return rowData == null ||
                    !rowData.containsKey(family) ||
                    !rowData.get(family).containsKey(qualifier);
        else if (rowData != null &&
                rowData.containsKey(family) &&
                rowData.get(family).containsKey(qualifier) &&
                !rowData.get(family).get(qualifier).isEmpty()) {

            byte[] oldValue = rowData.get(family).get(qualifier).lastEntry().getValue();
            int compareResult = Bytes.compareTo(value, oldValue);
            switch (compareOp) {
                case LESS:
                    return compareResult < 0;
                case LESS_OR_EQUAL:
                    return compareResult <= 0;
                case EQUAL:
                    return compareResult == 0;
                case NOT_EQUAL:
                    return compareResult != 0;
                case GREATER_OR_EQUAL:
                    return compareResult >= 0;
                case GREATER:
                    return compareResult > 0;
                default:
                    throw new RuntimeException("Unknown Compare op " + compareOp.name());
            }
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return checkAndPut(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, put);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Put put) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            put(put);
            return true;
        }
        return false;
    }

    @Override
    public void delete(Delete delete) throws IOException {
        byte[] row = delete.getRow();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        if (rowData == null)
            return;
        if (delete.getFamilyCellMap().isEmpty()) {
            data.remove(row);
            return;
        }
        for (byte[] family : delete.getFamilyCellMap().keySet()) {
            if (rowData.get(family) == null)
                continue;
            if (delete.getFamilyCellMap().get(family).isEmpty()) {
                rowData.remove(family);
                continue;
            }
            for (Cell kv : delete.getFamilyCellMap().get(family)) {
                long ts = kv.getTimestamp();
                if (kv.getType() == Cell.Type.DeleteColumn) {
                    if (ts == HConstants.LATEST_TIMESTAMP) {
                        rowData.get(CellUtil.cloneFamily(kv)).remove(CellUtil.cloneQualifier(kv));
                    } else {
                        rowData.get(CellUtil.cloneFamily(kv)).get(CellUtil.cloneQualifier(kv)).subMap(0L, true, ts, true).clear();
                    }
                } else {
                    if (ts == HConstants.LATEST_TIMESTAMP) {
                        rowData.get(CellUtil.cloneFamily(kv)).get(CellUtil.cloneQualifier(kv)).pollLastEntry();
                    } else {
                        rowData.get(CellUtil.cloneFamily(kv)).get(CellUtil.cloneQualifier(kv)).remove(ts);
                    }
                }
            }
            if (rowData.get(family).isEmpty()) {
                rowData.remove(family);
            }
        }
        if (rowData.isEmpty()) {
            data.remove(row);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(delete);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return checkAndDelete(row, family, qualifier, CompareFilter.CompareOp.EQUAL, value, delete);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, Delete delete) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            delete(delete);
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations rm) throws IOException {
        if (check(row, family, qualifier, compareOp, value)) {
            mutateRow(rm);
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result increment(Increment increment) throws IOException {
        List<Cell> kvs = new ArrayList<>();
        Map<byte[], NavigableMap<byte[], Long>> famToVal = increment.getFamilyMapOfLongs();
        for (Map.Entry<byte[], NavigableMap<byte[], Long>> ef : famToVal.entrySet()) {
            byte[] family = ef.getKey();
            NavigableMap<byte[], Long> qToVal = ef.getValue();
            for (Map.Entry<byte[], Long> eq : qToVal.entrySet()) {
                long newValue = incrementColumnValue(increment.getRow(), family, eq.getKey(), eq.getValue());
                Map.Entry<Long, byte[]> timestampAndValue = data.get(increment.getRow()).get(family).get(eq.getKey()).lastEntry();
                kvs.add(new KeyValue(increment.getRow(), family, eq.getKey(), timestampAndValue.getKey(), timestampAndValue.getValue()));
            }
        }
        return Result.create(kvs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        if (check(row, family, qualifier, CompareFilter.CompareOp.EQUAL, null)) {
            Put put = new Put(row);
            put.addColumn(family, qualifier, Bytes.toBytes(amount));
            put(put);
            return amount;
        }
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = data.get(row);
        long newValue = Bytes.toLong(rowData.get(family).get(qualifier).lastEntry().getValue()) + amount;
        rowData.get(family).get(qualifier).put(System.currentTimeMillis(), Bytes.toBytes(newValue));
        return newValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
                                     long amount, Durability durability) throws IOException {
        return incrementColumnValue(row, family, qualifier, amount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(final Class<T> service,
                                                                    byte[] startKey, byte[] endKey, final Batch.Call<T, R> callable)
            throws ServiceException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends Service, R> void coprocessorService(final Class<T> service,
                                                          byte[] startKey, byte[] endKey, final Batch.Call<T, R> callable,
                                                          final Batch.Callback<R> callback) throws ServiceException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            Descriptors.MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                            Message request, byte[] startKey, byte[] endKey, R responsePrototype,
                                                            Batch.Callback<R> callback) throws ServiceException {
        throw new RuntimeException(this.getClass() + " does NOT implement this method.");
    }

    public RegionLocator getRegionLocator() {
        return new RegionLocator() {
            @Override
            public HRegionLocation getRegionLocation(byte[] bytes) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public HRegionLocation getRegionLocation(byte[] bytes, boolean b) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public HRegionLocation getRegionLocation(byte[] bytes, int regionId, boolean b) throws IOException {
                return new HRegionLocation(null, ServerName.valueOf("localhost:0", 0));
            }

            @Override
            public List<HRegionLocation> getRegionLocations(byte[] bytes, boolean b) throws IOException {
                return Collections.singletonList(getRegionLocation(bytes, b));
            }

            @Override
            public void clearRegionLocationCache() {
            }

            @Override
            public List<HRegionLocation> getAllRegionLocations() throws IOException {
                return null;
            }

            @Override
            public byte[][] getStartKeys() throws IOException {
                return getStartEndKeys().getFirst();
            }

            @Override
            public byte[][] getEndKeys() throws IOException {
                return getStartEndKeys().getSecond();
            }

            @Override
            public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
                final byte[][] startKeyList = new byte[1][];
                final byte[][] endKeyList = new byte[1][];

                startKeyList[0] = new byte[0];
                endKeyList[0] = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};

                return new Pair<>(startKeyList, endKeyList);
            }

            @Override
            public TableName getName() {
                return MockHTable.this.getName();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    public HTable asHTable() {
        return Mockito.mock(HTable.class, delegatesTo(this));
    }
}
