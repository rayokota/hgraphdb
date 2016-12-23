package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

public class VertexIndexWriter implements Creator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final Map<String, Boolean> keys;
    private final Long ts;

    public VertexIndexWriter(HBaseGraph graph, Vertex vertex, String key) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = ImmutableMap.of(key, false);
        this.ts = null;
    }

    public VertexIndexWriter(HBaseGraph graph, Vertex vertex, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.collectMap(indices, IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Vertex getElement() {
        return vertex;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        return keys.entrySet().stream().filter(entry -> vertex.keys().contains(entry.getKey()))
                .map(this::constructPut).iterator();
    }

    private Put constructPut(Map.Entry<String, Boolean> entry) {
        long timestamp = ts != null ? ts : HConstants.LATEST_TIMESTAMP;
        boolean isUnique = entry.getValue();
        Put put = new Put(graph.getVertexIndexModel().serializeForWrite(vertex, isUnique, entry.getKey()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                timestamp, ValueUtils.serialize(((HBaseVertex) vertex).createdAt()));
        if (isUnique) {
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES, timestamp, ValueUtils.serialize(vertex.id()));
        }
        put.setAttribute(Mutators.IS_UNIQUE, Bytes.toBytes(isUnique));
        return put;
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphNotUniqueException("Vertex index already exists");
    }
}
