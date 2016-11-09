package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class VertexIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final Iterator<String> keys;
    private final Long ts;

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, String key, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.of(key);
        this.ts = ts;
    }

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, Iterator<IndexMetadata> indices) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.map(indices, IndexMetadata::propertyKey);
        this.ts = null;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return IteratorUtils.map(keys, key -> {
            Delete delete = new Delete(graph.getVertexIndexModel().serializeForWrite(vertex, key));
            if (ts != null) delete.setTimestamp(ts);
            return delete;
        });
    }
}
