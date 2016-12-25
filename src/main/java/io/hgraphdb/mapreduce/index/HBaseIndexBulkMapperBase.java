package io.hgraphdb.mapreduce.index;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public abstract class HBaseIndexBulkMapperBase extends HBaseIndexMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexBulkMapperBase.class);

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context)
            throws IOException, InterruptedException {

        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        final Iterator<? extends Mutation> mutations = constructMutations(result);
        List<KeyValue> keyValueList = toKeyValues(IteratorUtils.list(mutations));
        for (KeyValue kv : keyValueList) {
            outputKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
            context.write(outputKey, kv);
        }
    }

    @SuppressWarnings("deprecation")
    public static List<KeyValue> toKeyValues(List<? extends Mutation> mutations) {
        List<KeyValue> keyValues = Lists.newArrayListWithExpectedSize(mutations.size() * 5); // Guess-timate 5 key values per row
        for (Mutation mutation : mutations) {
            for (List<Cell> keyValueList : mutation.getFamilyCellMap().values()) {
                for (Cell keyValue : keyValueList) {
                    keyValues.add(org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(keyValue));
                }
            }
        }
        Collections.sort(keyValues, KeyValue.COMPARATOR);
        return keyValues;
    }
}