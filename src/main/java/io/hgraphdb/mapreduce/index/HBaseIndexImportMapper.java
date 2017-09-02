package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.readers.EdgeReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class HBaseIndexImportMapper extends HBaseIndexBulkMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexImportMapper.class);

    private ElementReader<?> reader;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        reader = getIndex().type() == ElementType.EDGE ? new EdgeReader(getGraph()) : new VertexReader(getGraph());
    }

    @Override
    protected Iterator<? extends Mutation> constructMutations(Result result)
            throws IOException, InterruptedException {
        return constructMutations(getGraph(), getIndex(), reader, result);
    }

    protected static Iterator<? extends Mutation> constructMutations(
            HBaseGraph graph, IndexMetadata index, ElementReader<?> reader, Result result)
            throws IOException, InterruptedException {

        Element element = reader.parse(result);
        if (element.label().equals(index.label()) && ((HBaseElement) element).hasProperty(index.propertyKey())) {
            Creator writer = index.type() == ElementType.EDGE
                    ? new EdgeIndexWriter(graph, (Edge) element, index.propertyKey())
                    : new VertexIndexWriter(graph, (Vertex) element, index.propertyKey());
            return writer.constructInsertions();
        }
        return Collections.emptyIterator();
    }
}