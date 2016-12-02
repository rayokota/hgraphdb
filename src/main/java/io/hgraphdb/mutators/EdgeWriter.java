package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeWriter implements Creator {

    private final HBaseGraph graph;
    private final Edge edge;

    public EdgeWriter(HBaseGraph graph, Edge edge) {
        this.graph = graph;
        this.edge = edge;
    }

    @Override
    public Edge getElement() {
        return edge;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        final String label = edge.label() != null ? edge.label() : Edge.DEFAULT_LABEL;
        Put put = new Put(ValueUtils.serializeWithSalt(edge.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.TO_BYTES,
                ValueUtils.serialize(edge.inVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.FROM_BYTES,
                ValueUtils.serialize(edge.outVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                ValueUtils.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize(((HBaseEdge)edge).createdAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(((HBaseEdge)edge).updatedAt()));
        ((HBaseEdge) edge).getProperties().entrySet().stream()
                .forEach(entry -> {
                    byte[] bytes = ValueUtils.serializePropertyValue(graph, ElementType.EDGE, label, entry.getKey(), entry.getValue());
                    put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(entry.getKey()), bytes);
                });
        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
    }
}
