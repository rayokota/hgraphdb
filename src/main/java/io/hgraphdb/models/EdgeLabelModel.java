package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.mutators.EdgeLabelRemover;
import io.hgraphdb.mutators.EdgeLabelWriter;
import io.hgraphdb.readers.EdgeLabelReader;
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

public class EdgeLabelModel extends BaseModel {

    public EdgeLabelModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void createLabel(EdgeLabel label) {
        Creator creator = new EdgeLabelWriter(graph, label);
        Mutators.create(table, creator);
    }

    public void deleteLabel(EdgeLabel label) {
        Mutator writer = new EdgeLabelRemover(graph, label);
        Mutators.write(table, writer);
    }

    public EdgeLabel label(String label, String outVertexLabel, String inVertexLabel) {
        final EdgeLabelReader parser = new EdgeLabelReader(graph);
        Get get = new Get(serialize(label, outVertexLabel, inVertexLabel));
        try {
            Result result = table.get(get);
            return parser.parse(result);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<EdgeLabel> labels() {
        final EdgeLabelReader parser = new EdgeLabelReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return IteratorUtils.<Result, EdgeLabel>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public byte[] serialize(String label, String outVertexLabel, String inVertexLabel) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, label, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, outVertexLabel, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, inVertexLabel, Order.ASCENDING);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public EdgeLabel deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        String label = OrderedBytes.decodeString(buffer);
        String outVertexLabel = OrderedBytes.decodeString(buffer);
        String inVertexLabel = OrderedBytes.decodeString(buffer);

        ValueType idType = null;
        Long createdAt = null;
        Map<String, ValueType> props = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                ValueType type = ValueType.valueOf((int)((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
                props.put(key, type);
            } else if (key.equals(Constants.EDGE_ID)) {
                idType = ValueType.valueOf(((Byte)ValueUtils.deserialize(CellUtil.cloneValue(cell))).intValue());
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        return new EdgeLabel(label, outVertexLabel, inVertexLabel, idType, createdAt, props);
    }
}
