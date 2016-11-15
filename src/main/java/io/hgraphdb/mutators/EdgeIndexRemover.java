package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.Constants;
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
            Mutation in = constructDelete(Direction.IN, entry);
            Mutation out = constructDelete(Direction.OUT, entry);
            return Stream.of(in, out);
        }).iterator();
    }

    private Delete constructDelete(Direction direction, Map.Entry<String, Boolean> entry) {
        boolean isUnique = entry.getValue();
        Delete delete = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, direction, isUnique, entry.getKey()));
        if (ts != null) {
            delete.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES, ts);
        } else {
            delete.addColumns(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES);
        }
        if (isUnique) {
            if (ts != null) {
                delete.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES, ts);
                delete.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.EDGE_ID_BYTES, ts);
            } else {
                delete.addColumns(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES);
                delete.addColumns(Constants.DEFAULT_FAMILY_BYTES, Constants.EDGE_ID_BYTES);
            }
        }
        return delete;
    }
}
