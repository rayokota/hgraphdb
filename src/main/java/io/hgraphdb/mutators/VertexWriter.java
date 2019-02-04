package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public final class VertexWriter implements Creator {

    private final HBaseGraph graph;
    private final Vertex vertex;

    public VertexWriter(HBaseGraph graph, Vertex vertex) {
        this.graph = graph;
        this.vertex = vertex;
    }

    @Override
    public Vertex getElement() {
        return vertex;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        final String label = vertex.label() != null ? vertex.label() : Vertex.DEFAULT_LABEL;
        Put put = new Put(ValueUtils.serializeWithSalt(vertex.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                ValueUtils.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize(((HBaseVertex) vertex).createdAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(((HBaseVertex) vertex).updatedAt()));
        ((HBaseVertex) vertex).propertyEntriesStream()
                .forEach(entry -> {
                    byte[] bytes = ValueUtils.serializePropertyValue(graph, ElementType.VERTEX, label, entry.getKey(), entry.getValue());
                    put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(entry.getKey()), bytes);
                });

        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
    }
}
