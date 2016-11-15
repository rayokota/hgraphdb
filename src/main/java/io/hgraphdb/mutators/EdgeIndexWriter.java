package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import com.sun.tools.internal.jxc.ap.Const;
import io.hgraphdb.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class EdgeIndexWriter implements Creator {

    private final HBaseGraph graph;
    private final Edge edge;
    private final Map<String, Boolean> keys;
    private final Long ts;

    public EdgeIndexWriter(HBaseGraph graph, Edge edge, String key) {
        this.graph = graph;
        this.edge = edge;
        this.keys = ImmutableMap.of(key, false);
        this.ts = null;
    }

    public EdgeIndexWriter(HBaseGraph graph, Edge edge, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = IteratorUtils.collectMap(indices, IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Edge getElement() {
        return edge;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        return keys.entrySet().stream().flatMap(entry ->
                    Stream.of(constructPut(Direction.IN, entry), constructPut(Direction.OUT, entry)))
                .iterator();
    }

    private Put constructPut(Direction direction, Map.Entry<String, Boolean> entry) {
        boolean isUnique = entry.getValue();
        Put put = ts != null
                ? new Put(graph.getEdgeIndexModel().serializeForWrite(edge, direction, isUnique, entry.getKey()), ts)
                : new Put(graph.getEdgeIndexModel().serializeForWrite(edge, direction, isUnique, entry.getKey()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                Serializer.serialize(((HBaseEdge)edge).createdAt()));
        if (isUnique) {
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.ID_BYTES, Serializer.serialize(edge.id()));
        }
        put.setAttribute(Mutators.IS_UNIQUE, Bytes.toBytes(isUnique));
        return put;
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphNotUniqueException("Edge index already exists");
    }
}
