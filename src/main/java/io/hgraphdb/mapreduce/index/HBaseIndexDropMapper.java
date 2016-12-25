package io.hgraphdb.mapreduce.index;

import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseElement;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import io.hgraphdb.mutators.EdgeIndexRemover;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.VertexIndexRemover;
import io.hgraphdb.readers.EdgeIndexReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexIndexReader;
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
 * Mapper that drops index rows.
 */
public class HBaseIndexDropMapper extends HBaseIndexBulkMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexDropMapper.class);

    private ElementReader<?> reader;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        reader = getIndex().type() == ElementType.EDGE ? new EdgeIndexReader(getGraph()) : new VertexIndexReader(getGraph());
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
            Mutator remover = index.type() == ElementType.EDGE
                    ? new EdgeIndexRemover(graph, (Edge) element, index.propertyKey(), null)
                    : new VertexIndexRemover(graph, (Vertex) element, index.propertyKey(), null);
            return remover.constructMutations();
        }
        return Collections.emptyIterator();
    }
}
