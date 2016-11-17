package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import io.hgraphdb.LabelConnection;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class LabelConnectionWriter implements Creator {

    private final HBaseGraph graph;
    private final LabelConnection labelConnection;

    public LabelConnectionWriter(HBaseGraph graph, LabelConnection labelConnection) {
        this.graph = graph;
        this.labelConnection = labelConnection;
    }

    @Override
    public Element getElement() {
        return null;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        Put put = new Put(graph.getLabelConnectionModel().serialize(
                labelConnection.outVertexLabel(), labelConnection.edgeLabel(), labelConnection.inVertexLabel()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize(labelConnection.createdAt()));
        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphException(labelConnection.toString() + " already exists");
    }
}
