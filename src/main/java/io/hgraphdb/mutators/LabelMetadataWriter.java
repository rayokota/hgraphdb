package io.hgraphdb.mutators;

import io.hgraphdb.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class LabelMetadataWriter implements Creator {

    private final HBaseGraph graph;
    private final LabelMetadata label;

    public LabelMetadataWriter(HBaseGraph graph, LabelMetadata label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Element getElement() {
        return null;
    }

    @Override
    public Iterator<Put> constructInsertions() {
        Put put = new Put(graph.getLabelMetadataModel().serialize(label.key()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize(label.createdAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES,
                ValueUtils.serialize(label.idType().getCode()));
        label.propertyTypes().entrySet().forEach(entry -> {
                    put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(entry.getKey()),
                            ValueUtils.serialize(entry.getValue().getCode()));
                }
        );
        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphException(label.toString() + " already exists");
    }
}
