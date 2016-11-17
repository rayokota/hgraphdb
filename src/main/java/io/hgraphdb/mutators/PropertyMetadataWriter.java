package io.hgraphdb.mutators;

import io.hgraphdb.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyMetadataWriter implements Creator {

    private final HBaseGraph graph;
    private final LabelMetadata label;
    private final String propertyKey;
    private final ValueType type;

    public PropertyMetadataWriter(HBaseGraph graph, LabelMetadata label, String propertyKey, ValueType type) {
        this.graph = graph;
        this.label = label;
        this.propertyKey = propertyKey;
        this.type = type;
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
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(label.updatedAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(propertyKey),
                ValueUtils.serialize(type.getCode()));
        return IteratorUtils.of(put);
    }

    @Override
    public byte[] getQualifierToCheck() {
        return Bytes.toBytes(propertyKey);
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphException("Property '" + propertyKey + "' for "
                + label.toString() + " already exists");
    }
}
