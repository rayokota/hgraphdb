package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

public class VertexIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final Map<String, Boolean> keys;
    private final Long ts;

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, String key, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = ImmutableMap.of(key, false);
        this.ts = ts;
    }

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.collectMap(indices, IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return keys.entrySet().stream().map(entry -> {
            Mutation delete = new Delete(graph.getVertexIndexModel().serializeForWrite(vertex, entry.getValue(), entry.getKey()));
            if (ts != null) ((Delete) delete).setTimestamp(ts);
            return delete;
        }).iterator();
    }
}
