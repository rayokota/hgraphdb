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
import org.apache.hadoop.hbase.util.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

public class VertexIndexModel extends BaseModel {

    public VertexIndexModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void writeVertexIndex(Vertex vertex) {
        long now = System.currentTimeMillis();
        ((HBaseVertex) vertex).setIndexTs(now);
        Iterator<IndexMetadata> indices = ((HBaseVertex) vertex).getIndices(OperationType.WRITE);
        VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, indices, now);
        Mutators.create(table, writer);
    }

    public void writeVertexIndex(Vertex vertex, String key) {
        VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, key);
        Mutators.create(table, writer);
    }

    public void writeVertexIndex(Vertex vertex, IndexMetadata index) {
        VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, IteratorUtils.of(index), null);
        Mutators.create(table, writer);
    }

    public void deleteVertexIndex(Vertex vertex, Long ts) {
        Iterator<IndexMetadata> indices = ((HBaseVertex) vertex).getIndices(OperationType.WRITE);
        VertexIndexRemover writer = new VertexIndexRemover(graph, vertex, indices, ts);
        Mutators.write(table, writer);
    }

    public void deleteVertexIndex(Vertex vertex, String key, Long ts) {
        Mutator writer = new VertexIndexRemover(graph, vertex, key, ts);
        Mutators.write(table, writer);
    }

    public Iterator<Vertex> vertices(String label, boolean isUnique, String key, Object value) {
        byte[] valueBytes = ValueUtils.serialize(value);
        return vertices(getVertexIndexScan(label, isUnique, key, value), vertex -> {
            byte[] propValueBytes = ValueUtils.serialize(vertex.getProperty(key));
            return Bytes.compareTo(propValueBytes, valueBytes) == 0;
        });
    }

    public Iterator<Vertex> vertices(String label, boolean isUnique, String key, Object inclusiveFrom, Object exclusiveTo) {
        byte[] fromBytes = ValueUtils.serialize(inclusiveFrom);
        byte[] toBytes = ValueUtils.serialize(exclusiveTo);
        return vertices(getVertexIndexScan(label, isUnique, key, inclusiveFrom, exclusiveTo), vertex -> {
            byte[] propValueBytes = ValueUtils.serialize(vertex.getProperty(key));
            return Bytes.compareTo(propValueBytes, fromBytes) >= 0
                    && Bytes.compareTo(propValueBytes, toBytes) < 0;
        });
    }

    private Iterator<Vertex> vertices(Scan scan, Predicate<HBaseVertex> filter) {
        final VertexIndexReader parser = new VertexIndexReader(graph);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Vertex>flatMap(scanner.iterator(), result -> {
                HBaseVertex vertex = (HBaseVertex) parser.parse(result);
                try {
                    boolean isLazy = graph.isLazyLoading();
                    if (!isLazy) vertex.load();
                    boolean passesFilter = isLazy || filter.test(vertex);
                    if (passesFilter) {
                        return IteratorUtils.of(vertex);
                    } else {
                        vertex.removeStaleIndex();
                        return Collections.emptyIterator();
                    }
                } catch (final HBaseGraphNotFoundException e) {
                    vertex.removeStaleIndex();
                    return Collections.emptyIterator();
                }
            });
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    private Scan getVertexIndexScan(String label, boolean isUnique, String key, Object value) {
        byte[] startRow = serializeForRead(label, isUnique, key, value);
        Scan scan = new Scan(startRow);
        scan.setFilter(new PrefixFilter(startRow));
        return scan;
    }

    private Scan getVertexIndexScan(String label, boolean isUnique, String key, Object inclusiveFrom, Object exclusiveTo) {
        byte[] startRow = serializeForRead(label, isUnique, key, inclusiveFrom);
        byte[] endRow = serializeForRead(label, isUnique, key, exclusiveTo);
        return new Scan(startRow, endRow);
    }

    public byte[] serializeForRead(String label, boolean isUnique, String key, Object value) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, label, Order.ASCENDING);
        OrderedBytes.encodeInt8(buffer, isUnique ? (byte) 1 : (byte) 0, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
        ValueUtils.serialize(buffer, value);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public byte[] serializeForWrite(Vertex vertex, boolean isUnique, String key) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(buffer, vertex.label(), Order.ASCENDING);
        OrderedBytes.encodeInt8(buffer, isUnique ? (byte) 1 : (byte) 0, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
        ValueUtils.serialize(buffer, vertex.value(key));
        if (!isUnique) {
            ValueUtils.serialize(buffer, vertex.id());
        }
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
        boolean isUnique = OrderedBytes.decodeInt8(buffer) == 1;
        String key = OrderedBytes.decodeString(buffer);
        Object value = ValueUtils.deserialize(buffer);
        Object vertexId;
        if (isUnique) {
            Cell vertexIdCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES);
            vertexId = ValueUtils.deserialize(CellUtil.cloneValue(vertexIdCell));
        } else {
            vertexId = ValueUtils.deserialize(buffer);
        }
        Cell createdAtCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES);
        Long createdAt = ValueUtils.deserialize(CellUtil.cloneValue(createdAtCell));
        Map<String, Object> properties = new HashMap<>();
        properties.put(key, value);
        HBaseVertex newVertex = new HBaseVertex(graph, vertexId, label, createdAt, null, properties, false);
        HBaseVertex vertex = (HBaseVertex) graph.findOrCreateVertex(vertexId);
        vertex.copyFrom(newVertex);
        vertex.setIndexKey(new IndexMetadata.Key(IndexType.VERTEX, label, key));
        vertex.setIndexTs(createdAtCell.getTimestamp());
        return vertex;
    }
}
