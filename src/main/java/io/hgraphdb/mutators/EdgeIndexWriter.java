package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.*;
import org.apache.hadoop.hbase.HConstants;
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
        this(graph, edge, key, null);
    }

    public EdgeIndexWriter(HBaseGraph graph, Edge edge, String key, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = ImmutableMap.of(key, false);
        this.ts = ts;
    }

    public EdgeIndexWriter(HBaseGraph graph, Edge edge, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.keys = IteratorUtils.collectMap(IteratorUtils.filter(indices, i -> i.label().equals(edge.label())),
                IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Edge getElement() {
        return edge;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        return keys.entrySet().stream()
                .filter(entry -> entry.getKey().equals(Constants.CREATED_AT) || ((HBaseEdge) edge).hasProperty(entry.getKey()))
                .flatMap(entry -> Stream.of(constructPut(Direction.IN, entry), constructPut(Direction.OUT, entry)))
                .iterator();
    }

    private Put constructPut(Direction direction, Map.Entry<String, Boolean> entry) {
        long timestamp = ts != null ? ts : HConstants.LATEST_TIMESTAMP;
        boolean isUnique = entry.getValue();
        Put put = new Put(graph.getEdgeIndexModel().serializeForWrite(edge, direction, isUnique, entry.getKey()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                timestamp, ValueUtils.serialize(((HBaseEdge) edge).createdAt()));
        if (isUnique) {
            Object vertexId = direction == Direction.IN ? edge.outVertex().id() : edge.inVertex().id();
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES, timestamp, ValueUtils.serialize(vertexId));
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.EDGE_ID_BYTES, timestamp, ValueUtils.serialize(edge.id()));
        }
        put.setAttribute(Mutators.IS_UNIQUE, Bytes.toBytes(isUnique));
        return put;
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphNotUniqueException("Edge index already exists");
    }
}
