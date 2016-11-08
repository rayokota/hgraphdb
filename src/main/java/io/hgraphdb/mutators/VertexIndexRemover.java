package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class VertexIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final String key;
    private final Long ts;

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, String key, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.key = key;
        this.ts = ts;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getVertexIndexModel().serializeForWrite(vertex, key));
        if (ts != null) delete.setTimestamp(ts);
        return IteratorUtils.of(delete);
    }
}
