package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.*;
import io.hgraphdb.readers.LabelMetadataReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.*;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LabelMetadataModel extends BaseModel {

    public LabelMetadataModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void createLabelMetadata(LabelMetadata label) {
        Creator creator = new LabelMetadataWriter(graph, label);
        Mutators.create(table, creator);
    }

    public void deleteLabelMetadata(LabelMetadata label) {
        Mutator writer = new LabelMetadataRemover(graph, label);
        Mutators.write(table, writer);
    }

    public void addPropertyMetadata(LabelMetadata label, Map<String, ValueType> propertyTypes) {
        propertyTypes.entrySet().forEach(entry -> {
            Creator creator = new PropertyMetadataWriter(graph, label, entry.getKey(), entry.getValue());
            Mutators.create(table, creator);
        });
    }

    public LabelMetadata label(LabelMetadata.Key labelKey) {
        final LabelMetadataReader parser = new LabelMetadataReader(graph);
        Get get = new Get(serialize(labelKey));
        try {
            Result result = table.get(get);
            return parser.parse(result);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<LabelMetadata> labels() {
        final LabelMetadataReader parser = new LabelMetadataReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public byte[] serialize(LabelMetadata.Key label) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, label.label(), Order.ASCENDING);
        OrderedBytes.encodeInt8(buffer, label.type() == ElementType.VERTEX ? (byte) 1 : (byte) 0, Order.ASCENDING);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public LabelMetadata deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        String label = OrderedBytes.decodeString(buffer);
        ElementType type = OrderedBytes.decodeInt8(buffer) == 1 ? ElementType.VERTEX : ElementType.EDGE;

        ValueType idType = null;
        Long createdAt = null;
        Long updatedAt = null;
        Map<String, ValueType> props = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                ValueType propType = ValueType.valueOf(((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
                props.put(key, propType);
            } else if (key.equals(Constants.ELEMENT_ID)) {
                idType = ValueType.valueOf(((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        return new LabelMetadata(type, label, idType, createdAt, updatedAt, props);
    }
}
