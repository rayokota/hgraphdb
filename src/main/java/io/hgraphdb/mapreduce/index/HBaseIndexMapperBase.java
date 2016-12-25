package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Abstract mapper that hands over rows from data table to the index table.
 */
public abstract class HBaseIndexMapperBase extends TableMapper<ImmutableBytesWritable, KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexMapperBase.class);

    private HBaseGraph graph;
    private IndexMetadata index;

    public HBaseGraph getGraph() {
        return graph;
    }

    public IndexMetadata getIndex() {
        return index;
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration configuration = context.getConfiguration();

        ElementType indexType = ElementType.valueOf(configuration.get(Constants.MAPREDUCE_INDEX_TYPE));
        String label = configuration.get(Constants.MAPREDUCE_INDEX_LABEL);
        String propertyKey = configuration.get(Constants.MAPREDUCE_INDEX_PROPERTY_KEY);

        graph = new HBaseGraph(new HBaseGraphConfiguration(configuration));
        index = graph.getIndex(OperationType.REMOVE, indexType, label, propertyKey);
    }

    protected abstract Iterator<? extends Mutation> constructMutations(Result result)
            throws IOException, InterruptedException;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        graph.close();
    }
}