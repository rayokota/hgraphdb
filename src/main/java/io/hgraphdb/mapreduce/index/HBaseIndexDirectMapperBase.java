package io.hgraphdb.mapreduce.index;

import io.hgraphdb.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public abstract class HBaseIndexDirectMapperBase extends HBaseIndexMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexDirectMapperBase.class);

    private boolean skipWAL;
    private BufferedMutator mutator;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration configuration = context.getConfiguration();

        skipWAL = configuration.getBoolean(Constants.MAPREDUCE_INDEX_SKIP_WAL, false);

        TableName outputTable = TableName.valueOf(configuration.get(TableOutputFormat.OUTPUT_TABLE));
        BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                LOG.warn("Failed to send put: " + e.getRow(i));
            }
        };
        BufferedMutatorParams mutatorParms = new BufferedMutatorParams(outputTable).listener(listener);
        mutator = getGraph().connection().getBufferedMutator(mutatorParms);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context)
            throws IOException, InterruptedException {

        mutator.mutate(getMutationList(constructMutations(result)));
    }

    private List<? extends Mutation> getMutationList(Iterator<? extends Mutation> mutations) {
        return IteratorUtils.list(IteratorUtils.consume(mutations,
                m -> m.setDurability(skipWAL ? Durability.SKIP_WAL : Durability.USE_DEFAULT)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mutator.close();
        super.cleanup(context);
    }
}
