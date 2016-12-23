package io.hgraphdb.mapreduce.index;

import com.google.common.collect.Lists;
import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.readers.EdgeReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class HBaseIndexImportMapper extends TableMapper<ImmutableBytesWritable, KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexImportMapper.class);

    private HBaseGraph graph;
    private IndexMetadata index;
    private ElementReader<?> reader;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration configuration = context.getConfiguration();

        ElementType indexType = ElementType.valueOf(configuration.get(Constants.POPULATE_INDEX_TYPE));
        String label = configuration.get(Constants.POPULATE_INDEX_LABEL);
        String propertyKey = configuration.get(Constants.POPULATE_INDEX_PROPERTY_KEY);

        graph = new HBaseGraph(new HBaseGraphConfiguration(configuration));
        index = graph.getIndex(OperationType.WRITE, indexType, label, propertyKey);
        reader = indexType == ElementType.EDGE ? new EdgeReader(graph) : new VertexReader(graph);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context)
            throws IOException, InterruptedException {

        final ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        Element element = reader.parse(result);
        if (element.label().equals(index.label()) && element.keys().contains(index.propertyKey())) {
            Creator writer = index.type() == ElementType.EDGE
                    ? new EdgeIndexWriter(graph, (Edge) element, index.propertyKey())
                    : new VertexIndexWriter(graph, (Vertex) element, index.propertyKey());
            List<? extends Mutation> mutations = IteratorUtils.list(writer.constructInsertions());
            List<KeyValue> keyValueList = toKeyValues(mutations);
            for (KeyValue kv : keyValueList) {
                outputKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                context.write(outputKey, kv);
            }
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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        graph.close();
    }
}