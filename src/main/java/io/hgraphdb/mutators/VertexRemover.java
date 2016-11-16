package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public final class VertexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;

    public VertexRemover(HBaseGraph graph, Vertex vertex) {
        this.graph = graph;
        this.vertex = vertex;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(ValueUtils.serializeWithSalt(vertex.id()));
        return IteratorUtils.of(delete);
    }
}
