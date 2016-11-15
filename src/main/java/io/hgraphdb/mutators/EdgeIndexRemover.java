package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class EdgeIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Edge edge;
    private final Map<String, Boolean> keys;
    private final Long ts;

    public EdgeIndexRemover(HBaseGraph graph, Edge edge, String key, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = ImmutableMap.of(key, false);
        this.ts = ts;
    }

    public EdgeIndexRemover(HBaseGraph graph, Edge edge, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = IteratorUtils.collectMap(indices, IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return keys.entrySet().stream().flatMap(entry -> {
            boolean isUnique = entry.getValue();
            Mutation in = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.IN, isUnique, entry.getKey()));
            Mutation out = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.OUT, isUnique, entry.getKey()));
            if (ts != null) {
                ((Delete) in).setTimestamp(ts);
                ((Delete) out).setTimestamp(ts);
            }
            return Stream.of(in, out);
        }).iterator();
    }
}
