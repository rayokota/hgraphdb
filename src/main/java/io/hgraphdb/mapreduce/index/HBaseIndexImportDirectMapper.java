package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.readers.EdgeReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class HBaseIndexImportDirectMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexImportDirectMapper.class);

    private boolean skipWAL;
    private HBaseGraph graph;
    private IndexMetadata index;
    private ElementReader<?> reader;
    private BufferedMutator mutator;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration configuration = context.getConfiguration();

        ElementType indexType = ElementType.valueOf(configuration.get(Constants.POPULATE_INDEX_TYPE));
        String label = configuration.get(Constants.POPULATE_INDEX_LABEL);
        String propertyKey = configuration.get(Constants.POPULATE_INDEX_PROPERTY_KEY);
        skipWAL = configuration.getBoolean(Constants.POPULATE_INDEX_SKIP_WAL, false);

        graph = new HBaseGraph(new HBaseGraphConfiguration(configuration));
        index = graph.getIndex(OperationType.WRITE, indexType, label, propertyKey);
        reader = indexType == ElementType.EDGE ? new EdgeReader(graph) : new VertexReader(graph);
        TableName outputTable = TableName.valueOf(configuration.get(TableOutputFormat.OUTPUT_TABLE));

        BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                LOG.warn("Failed to send put: " + e.getRow(i));
            }
        };
        BufferedMutatorParams mutatorParms = new BufferedMutatorParams(outputTable).listener(listener);
        mutator = graph.connection().getBufferedMutator(mutatorParms);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context)
            throws IOException, InterruptedException {

        Element element = reader.parse(result);
        if (element.label().equals(index.label()) && element.keys().contains(index.propertyKey())) {
            Creator writer = index.type() == ElementType.EDGE
                    ? new EdgeIndexWriter(graph, (Edge) element, index.propertyKey())
                    : new VertexIndexWriter(graph, (Vertex) element, index.propertyKey());
            mutator.mutate(getMutationList(writer.constructInsertions()));
        }
    }

    private List<? extends Mutation> getMutationList(Iterator<? extends Mutation> mutations) {
        return IteratorUtils.list(IteratorUtils.consume(mutations,
                m -> m.setDurability(skipWAL ? Durability.SKIP_WAL : Durability.USE_DEFAULT)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mutator.close();
        graph.close();
    }
}
