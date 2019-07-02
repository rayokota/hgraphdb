package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.LabelConnectionRemover;
import io.hgraphdb.mutators.LabelConnectionWriter;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.readers.LabelConnectionReader;
import io.hgraphdb.util.DynamicPositionedMutableByteRange;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

import java.io.IOException;
import java.util.Iterator;

public class LabelConnectionModel extends BaseModel {

    public LabelConnectionModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void createLabelConnection(LabelConnection label) {
        Creator creator = new LabelConnectionWriter(graph, label);
        Mutators.create(table, creator);
    }

    public void deleteLabelConnection(LabelConnection label) {
        Mutator writer = new LabelConnectionRemover(graph, label);
        Mutators.write(table, writer);
    }

    public LabelConnection labelConnection(String outVertexLabel, String edgeLabel, String inVertexLabel) {
        final LabelConnectionReader parser = new LabelConnectionReader(graph);
        Get get = new Get(serialize(outVertexLabel, edgeLabel, inVertexLabel));
        try {
            Result result = table.get(get);
            return parser.parse(result);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<LabelConnection> labelConnections() {
        final LabelConnectionReader parser = new LabelConnectionReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public byte[] serialize(String outVertexLabel, String edgeLabel, String inVertexLabel) {
        PositionedByteRange buffer = new DynamicPositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, outVertexLabel, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, edgeLabel, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, inVertexLabel, Order.ASCENDING);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public LabelConnection deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        String outVertexLabel = OrderedBytes.decodeString(buffer);
        String edgeLabel = OrderedBytes.decodeString(buffer);
        String inVertexLabel = OrderedBytes.decodeString(buffer);
        Cell createdAtCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES);
        Long createdAt = ValueUtils.deserialize(CellUtil.cloneValue(createdAtCell));
        return new LabelConnection(outVertexLabel, edgeLabel, inVertexLabel, createdAt);
    }
}
