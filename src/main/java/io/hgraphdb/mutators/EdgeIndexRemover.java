package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Edge edge;
    private final Iterator<String> keys;
    private final Long ts;

    public EdgeIndexRemover(HBaseGraph graph, Edge edge, String key, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = IteratorUtils.of(key);
        this.ts = ts;
    }

    public EdgeIndexRemover(HBaseGraph graph, Edge edge, Iterator<IndexMetadata> indices) {
        this.graph = graph;
        this.edge = edge;
        this.keys = IteratorUtils.map(indices, IndexMetadata::propertyKey);
        this.ts = null;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return IteratorUtils.flatMap(keys, key -> {
            Delete in = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.IN, key));
            Delete out = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.OUT, key));
            if (ts != null) {
                in.setTimestamp(ts);
                out.setTimestamp(ts);
            }
            return IteratorUtils.of(in, out);
        });
    }
}
