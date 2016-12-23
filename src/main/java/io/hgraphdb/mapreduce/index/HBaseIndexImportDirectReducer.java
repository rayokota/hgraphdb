package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class HBaseIndexImportDirectReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexImportDirectReducer.class);

    private ElementType indexType;
    private String label;
    private String propertyKey;
    private HBaseGraph graph;

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration configuration = context.getConfiguration();

        indexType = ElementType.valueOf(configuration.get(Constants.POPULATE_INDEX_TYPE));
        label = configuration.get(Constants.POPULATE_INDEX_LABEL);
        propertyKey = configuration.get(Constants.POPULATE_INDEX_PROPERTY_KEY);

        graph = new HBaseGraph(new HBaseGraphConfiguration(configuration));
    }

    @Override
    protected void reduce(ImmutableBytesWritable arg0, Iterable<IntWritable> arg1,
                          Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable>.Context arg2)
            throws IOException, InterruptedException {
        try {
            graph.updateIndex(new IndexMetadata.Key(indexType, label, propertyKey), IndexMetadata.State.ACTIVE);
        } catch (Exception e) {
            LOG.error(" Failed to update the status to Active");
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        graph.close();
    }
}
