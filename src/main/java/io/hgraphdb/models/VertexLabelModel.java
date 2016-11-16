package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.*;
import io.hgraphdb.readers.VertexLabelReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VertexLabelModel extends BaseModel {

    public VertexLabelModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void createLabel(VertexLabel label) {
        Creator creator = new VertexLabelWriter(graph, label);
        Mutators.create(table, creator);
    }

    public void deleteLabel(VertexLabel label) {
        Mutator writer = new VertexLabelRemover(graph, label);
        Mutators.write(table, writer);
    }

    public VertexLabel label(String label) {
        final VertexLabelReader parser = new VertexLabelReader(graph);
        Get get = new Get(serialize(label));
        try {
            Result result = table.get(get);
            return parser.parse(result);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<VertexLabel> labels() {
        final VertexLabelReader parser = new VertexLabelReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return IteratorUtils.<Result, VertexLabel>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public byte[] serialize(String label) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, label, Order.ASCENDING);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public VertexLabel deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        String label = OrderedBytes.decodeString(buffer);

        ValueType idType = null;
        Long createdAt = null;
        Map<String, ValueType> props = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                ValueType type = ValueType.valueOf(((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
                props.put(key, type);
            } else if (key.equals(Constants.VERTEX_ID)) {
                idType = ValueType.valueOf(((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        return new VertexLabel(label, idType, createdAt, props);
    }
}
