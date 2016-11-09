package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.mutators.VertexIndexRemover;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.readers.VertexIndexReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Triplet;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VertexIndexModel extends BaseModel {

    public VertexIndexModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void writeVertexIndex(Vertex vertex) {
        Iterator<IndexMetadata> indices = graph.getIndices(
                OperationType.WRITE, IndexType.VERTEX, vertex.label(), ((HBaseVertex) vertex).getPropertyKeys());
        VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, indices);
        Mutators.write(table, writer);
    }

    public void writeVertexIndex(Vertex vertex, IndexMetadata index) {
        VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, IteratorUtils.of(index));
        Mutators.write(table, writer);
    }

    public void deleteVertexIndex(Vertex vertex) {
        Iterator<IndexMetadata> indices = graph.getIndices(
                OperationType.WRITE, IndexType.VERTEX, vertex.label(), ((HBaseVertex) vertex).getPropertyKeys());
        VertexIndexRemover writer = new VertexIndexRemover(graph, vertex, indices);
        Mutators.write(table, writer);
    }

    public void deleteVertexIndex(Vertex vertex, IndexMetadata.Key key, Long ts) {
        Mutator writer = new VertexIndexRemover(graph, vertex, key.propertyKey(), ts);
        Mutators.write(table, writer);
    }

    public Iterator<Vertex> vertices(String label, String key, Object value) {
        return vertices(getVertexIndexScan(label, key, value));
    }

    public Iterator<Vertex> vertices(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        return vertices(getVertexIndexScan(label, key, inclusiveFrom, exclusiveTo));
    }

    private Iterator<Vertex> vertices(Scan scan) {
        final VertexIndexReader parser = new VertexIndexReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Vertex>flatMap(scanner.iterator(), result -> {
                Vertex vertex = parser.parse(result);
                try {
                    if (!graph.isLazyLoading()) ((HBaseVertex) vertex).load();
                    return IteratorUtils.of(vertex);
                } catch (final HBaseGraphNotFoundException e) {
                    e.getElement().removeStaleIndex();
                    return Collections.emptyIterator();
                }
            });
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    private Scan getVertexIndexScan(String label, String key, Object value) {
        byte[] startRow = serializeForRead(label, key, value);
        Scan scan = new Scan(startRow);
        scan.setFilter(new PrefixFilter(startRow));
        return scan;
    }

    private Scan getVertexIndexScan(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        byte[] startRow = serializeForRead(label, key, inclusiveFrom);
        byte[] endRow = serializeForRead(label, key, exclusiveTo);
        return new Scan(startRow, endRow);
    }

    public byte[] serializeForRead(String label, String key, Object value) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, label, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
        Serializer.serialize(buffer, value);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public byte[] serializeForWrite(Vertex vertex, String key) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, vertex.label(), Order.ASCENDING);
        OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
        Serializer.serialize(buffer, vertex.value(key));
        Serializer.serialize(buffer, vertex.id());
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public Vertex deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        String label = OrderedBytes.decodeString(buffer);
        String key = OrderedBytes.decodeString(buffer);
        Object value = Serializer.deserialize(buffer);
        Object id = Serializer.deserialize(buffer);
        Cell createdAtCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES);
        Long createdAt = Serializer.deserialize(CellUtil.cloneValue(createdAtCell));
        Map<String, Object> properties = new HashMap<>();
        properties.put(key, value);
        HBaseVertex newVertex = new HBaseVertex(graph, id, label, createdAt, null, properties, false);
        HBaseVertex vertex = (HBaseVertex) graph.findOrCreateVertex(id);
        vertex.copyFrom(newVertex);
        vertex.setIndexKey(new IndexMetadata.Key(IndexType.VERTEX, label, key));
        vertex.setIndexTs(createdAtCell.getTimestamp());
        return vertex;
    }
}
