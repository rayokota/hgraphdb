package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.Serializer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class EdgeWriter implements Creator {

    private final HBaseGraph graph;
    private final Edge edge;

    public EdgeWriter(HBaseGraph graph, Edge edge) {
        this.graph = graph;
        this.edge = edge;
    }

    @Override
    public Put constructPut() {
        String label = edge.label();
        if (label == null) label = Edge.DEFAULT_LABEL;
        Put put = new Put(Serializer.serializeWithSalt(edge.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.TO_BYTES,
                Serializer.serialize(edge.inVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.FROM_BYTES,
                Serializer.serialize(edge.outVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                Serializer.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                Serializer.serialize(((HBaseEdge)edge).createdAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                Serializer.serialize(((HBaseEdge)edge).updatedAt()));
        ((HBaseEdge) edge).getProperties().entrySet().stream()
                .forEach(entry -> {
                    byte[] bytes = Serializer.serialize(entry.getValue());
                    put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(entry.getKey()), bytes);
                });
        return put;
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
    }
}
