package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.EdgeIndexRemover;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.readers.EdgeIndexReader;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class EdgeIndexModel extends BaseModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeIndexModel.class);

    public EdgeIndexModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public void writeEdgeEndpoints(Edge edge) {
        long now = System.currentTimeMillis();
        ((HBaseEdge) edge).setIndexTs(now);
        Iterator<IndexMetadata> indices = ((HBaseEdge) edge).getIndices(OperationType.WRITE);
        EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, edge, indices, now);
        EdgeIndexWriter writer = new EdgeIndexWriter(graph, edge, Constants.CREATED_AT, now);
        Mutators.create(table, indexWriter, writer);
    }

    public void writeEdgeIndex(Edge edge, String key) {
        EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, edge, key, null);
        Mutators.create(table, indexWriter);
    }

    public void writeEdgeIndex(Edge edge, IndexMetadata index) {
        EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, edge, IteratorUtils.of(index), null);
        Mutators.create(table, indexWriter);
    }

    public void deleteEdgeEndpoints(Edge edge, Long ts) {
        Iterator<IndexMetadata> indices = ((HBaseEdge) edge).getIndices(OperationType.WRITE);
        EdgeIndexRemover indexWriter = new EdgeIndexRemover(graph, edge, indices, ts);
        Mutator writer = new EdgeIndexRemover(graph, edge, Constants.CREATED_AT, ts);
        Mutators.write(table, writer, indexWriter);
    }

    public void deleteEdgeIndex(Edge edge, String key, Long ts) {
        Mutator writer = new EdgeIndexRemover(graph, edge, key, ts);
        Mutators.write(table, writer);
    }

    public Iterator<Edge> edges(HBaseVertex vertex, Direction direction, String... labels) {
        Tuple cacheKey = labels.length > 0
                ? new Pair<>(direction, Arrays.asList(labels)) : new Unit<>(direction);
        Iterator<Edge> edges = vertex.getEdgesFromCache(cacheKey);
        if (edges != null) {
            return edges;
        }
        Scan scan = getEdgesEndpointScan(vertex, direction, Constants.CREATED_AT, labels);
        return performEdgesScan(vertex, scan, cacheKey, false, edge -> true);
    }

    public Iterator<Edge> edges(HBaseVertex vertex, Direction direction, String label,
                                String key, Object value) {
        byte[] valueBytes = ValueUtils.serialize(value);
        Tuple cacheKey = new Quartet<>(direction, label, key, ByteBuffer.wrap(valueBytes));
        Iterator<Edge> edges = vertex.getEdgesFromCache(cacheKey);
        if (edges != null) {
            return edges;
        }
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.EDGE, label, key);
        final boolean useIndex = !key.equals(Constants.CREATED_AT) && index != null;
        if (useIndex) {
            LOGGER.debug("Using edge index for ({}, {})", label, key);
        }
        Scan scan = useIndex
                ? getEdgesScan(vertex, direction, index.isUnique(), key, label, value)
                : getEdgesEndpointScan(vertex, direction, Constants.CREATED_AT, label);
        return performEdgesScan(vertex, scan, cacheKey, useIndex, edge -> {
            byte[] propValueBytes = ValueUtils.serialize(edge.getProperty(key));
            return Bytes.compareTo(propValueBytes, valueBytes) == 0;
        });
    }

    public Iterator<Edge> edges(HBaseVertex vertex, Direction direction, String label,
                                String key, Object inclusiveFromValue, Object exclusiveToValue) {
        byte[] fromBytes = ValueUtils.serialize(inclusiveFromValue);
        byte[] toBytes = ValueUtils.serialize(exclusiveToValue);
        Tuple cacheKey = new Quintet<>(direction, label, key, ByteBuffer.wrap(fromBytes), ByteBuffer.wrap(toBytes));
        Iterator<Edge> edges = vertex.getEdgesFromCache(cacheKey);
        if (edges != null) {
            return edges;
        }
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.EDGE, label, key);
        final boolean useIndex = !key.equals(Constants.CREATED_AT) && index != null;
        if (useIndex) {
            LOGGER.debug("Using edge index for ({}, {})", label, key);
        }
        Scan scan = useIndex
                ? getEdgesScan(vertex, direction, index.isUnique(), key, label, inclusiveFromValue, exclusiveToValue)
                : getEdgesEndpointScan(vertex, direction, Constants.CREATED_AT, label);
        return performEdgesScan(vertex, scan, cacheKey, useIndex, edge -> {
            byte[] propValueBytes = ValueUtils.serialize(edge.getProperty(key));
            return Bytes.compareTo(propValueBytes, fromBytes) >= 0
                    && Bytes.compareTo(propValueBytes, toBytes) < 0;
        });
    }

    public Iterator<Edge> edgesWithLimit(HBaseVertex vertex, Direction direction, String label,
                                         String key, Object fromValue, int limit, boolean reversed) {
        byte[] fromBytes = fromValue != null ? ValueUtils.serialize(fromValue) : new byte[0];
        Tuple cacheKey = new Sextet<>(direction, label, key, ByteBuffer.wrap(fromBytes), limit, reversed);
        Iterator<Edge> edges = vertex.getEdgesFromCache(cacheKey);
        if (edges != null) {
            return edges;
        }
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.EDGE, label, key);
        final boolean useIndex = !key.equals(Constants.CREATED_AT) && index != null;
        if (useIndex) {
            LOGGER.debug("Using edge index for ({}, {})", label, key);
        } else {
            throw new HBaseGraphNotValidException("Method edgesWithLimit requires an index be defined");
        }
        Scan scan = getEdgesScanWithLimit(vertex, direction, index.isUnique(), key, label, fromValue, limit, reversed);
        return IteratorUtils.limit(performEdgesScan(vertex, scan, cacheKey, useIndex, edge -> {
            if (fromBytes.length == 0) return true;
            byte[] propValueBytes = ValueUtils.serialize(edge.getProperty(key));
            int compare = Bytes.compareTo(propValueBytes, fromBytes);
            return reversed ? compare <= 0 : compare >= 0;
        }), limit);
    }

    @SuppressWarnings("unchecked")
    private Iterator<Edge> performEdgesScan(HBaseVertex vertex, Scan scan, Tuple cacheKey,
                                            boolean useIndex, Predicate<HBaseEdge> filter) {
        List<Edge> cached = new ArrayList<>();
        final EdgeIndexReader parser = new EdgeIndexReader(graph);
        ResultScanner scanner;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Edge>flatMap(
                    IteratorUtils.concat(scanner.iterator(), IteratorUtils.of(Result.EMPTY_RESULT)),
                    result -> {
                        if (result == Result.EMPTY_RESULT) {
                            vertex.cacheEdges(cacheKey, cached);
                            scanner.close();
                            return Collections.emptyIterator();
                        }
                        HBaseEdge edge = (HBaseEdge) parser.parse(result);
                        try {
                            boolean isLazy = graph.isLazyLoading();
                            if (!isLazy) edge.load();
                            boolean passesFilter = (isLazy && useIndex) || filter == null || filter.test(edge);
                            if (passesFilter) {
                                cached.add(edge);
                                return IteratorUtils.of(edge);
                            } else {
                                if (useIndex) edge.removeStaleIndex();
                                return Collections.emptyIterator();
                            }
                        } catch (final HBaseGraphNotFoundException e) {
                            edge.removeStaleIndex();
                            return Collections.emptyIterator();
                        }
                    });
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(HBaseVertex vertex, Direction direction, String... labels) {
        return IteratorUtils.flatMap(edges(vertex, direction, labels), transformEdge(vertex));
    }

    public Iterator<Vertex> vertices(HBaseVertex vertex, Direction direction, String label,
                                     String edgeKey, Object edgeValue) {
        return IteratorUtils.flatMap(edges(vertex, direction, label, edgeKey, edgeValue), transformEdge(vertex));
    }

    public Iterator<Vertex> vertices(HBaseVertex vertex, Direction direction, String label,
                                     String edgeKey, Object inclusiveFromEdgeValue, Object exclusiveToEdgeValue) {
        return IteratorUtils.flatMap(edges(vertex, direction, label, edgeKey,
                inclusiveFromEdgeValue, exclusiveToEdgeValue), transformEdge(vertex));
    }

    public Iterator<Vertex> verticesWithLimit(HBaseVertex vertex, Direction direction, String label,
                                              String edgeKey, Object fromEdgeValue, int limit, boolean reversed) {
        return IteratorUtils.flatMap(edgesWithLimit(vertex, direction, label, edgeKey,
                fromEdgeValue, limit, reversed), transformEdge(vertex));
    }

    private Function<Edge, Iterator<Vertex>> transformEdge(HBaseVertex vertex) {
        return edge -> {
            Object inVertexId = edge.inVertex().id();
            Object outVertexId = edge.outVertex().id();
            Object vertexId = vertex.id().equals(inVertexId) ? outVertexId : inVertexId;
            try {
                HBaseVertex v = (HBaseVertex) graph.findOrCreateVertex(vertexId);
                if (!graph.isLazyLoading()) v.load();
                return IteratorUtils.of(v);
            } catch (final HBaseGraphNotFoundException e) {
                ((HBaseEdge) edge).removeStaleIndex();
                return Collections.emptyIterator();
            }
        };
    }

    private Scan getEdgesEndpointScan(Vertex vertex, Direction direction, String key, String... labels) {
        LOGGER.trace("Executing Scan, type: {}, id: {}", "key", vertex.id());

        Scan scan;
        PrefixFilter prefixFilter;
        if (direction == Direction.BOTH) {
            byte[] startRow = serializeForRead(vertex, null, null);
            byte[] prefix = serializeForRead(vertex, null, null);
            scan = new Scan(startRow);
            prefixFilter = new PrefixFilter(prefix);
        } else {
            byte[] startRow = serializeForRead(vertex, direction, null);
            scan = new Scan(startRow);
            prefixFilter = new PrefixFilter(startRow);
        }

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(prefixFilter);
        filterList.addFilter(applyEdgeLabelsRowFilter(scan, vertex, direction, key, labels));
        scan.setFilter(filterList);
        return scan;
    }

    private FilterList applyEdgeLabelsRowFilter(Scan scan, Vertex vertex, Direction direction, String key, String... labels) {
        FilterList rowFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        if (labels.length > 0) {
            Arrays.stream(labels).forEach(label -> {
                if (direction == Direction.BOTH) {
                    applyEdgeLabelRowFilter(rowFilters, vertex, Direction.IN, key, label);
                    applyEdgeLabelRowFilter(rowFilters, vertex, Direction.OUT, key, label);
                } else {
                    applyEdgeLabelRowFilter(rowFilters, vertex, direction, key, label);
                }
            });
        } else {
            if (direction == Direction.BOTH) {
                applyEdgeLabelRowFilter(rowFilters, vertex, Direction.IN, key, null);
                applyEdgeLabelRowFilter(rowFilters, vertex, Direction.OUT, key, null);
            } else {
                applyEdgeLabelRowFilter(rowFilters, vertex, direction, key, null);
            }
        }
        return rowFilters;
    }

    private void applyEdgeLabelRowFilter(FilterList filters, Vertex vertex, Direction direction, String key, String label) {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(serializeForRead(vertex, direction, false, key, label, null)));
        filters.addFilter(rowFilter);
    }

    private Scan getEdgesScan(Vertex vertex, Direction direction, boolean isUnique, String key, String label, Object value) {
        LOGGER.trace("Executing Scan, type: {}, id: {}", "key-value", vertex.id());

        byte[] startRow = serializeForRead(vertex, direction, isUnique, key, label, value);
        Scan scan = new Scan(startRow);
        scan.setFilter(new PrefixFilter(startRow));
        return scan;
    }

    private Scan getEdgesScan(Vertex vertex, Direction direction, boolean isUnique, String key, String label,
                              Object inclusiveFromValue, Object exclusiveToValue) {
        LOGGER.trace("Executing Scan, type: {}, id: {}", "key-range", vertex.id());

        byte[] startRow = serializeForRead(vertex, direction, isUnique, key, label, inclusiveFromValue);
        byte[] stopRow = serializeForRead(vertex, direction, isUnique, key, label, exclusiveToValue);
        return new Scan(startRow, stopRow);
    }

    private Scan getEdgesScanWithLimit(Vertex vertex, Direction direction, boolean isUnique, String key, String label,
                                       Object fromValue, int limit, boolean reversed) {
        LOGGER.trace("Executing Scan, type: {}, id: {}", "key-limit", vertex.id());

        byte[] prefix = serializeForRead(vertex, direction, isUnique, key, label, null);
        byte[] startRow = fromValue != null
                ? serializeForRead(vertex, direction, isUnique, key, label, fromValue)
                : (reversed ? HBaseGraphUtils.createClosestRowAfter(prefix) : prefix);
        Scan scan = new Scan(startRow);
        FilterList filterList = new FilterList();
        filterList.addFilter(new PrefixFilter(prefix));
        filterList.addFilter(new PageFilter(limit));
        scan.setFilter(filterList);
        scan.setReversed(reversed);
        return scan;
    }

    public byte[] serializeForRead(Vertex vertex, Direction direction, String label) {
        return serializeForRead(vertex, direction, false, Constants.CREATED_AT, label, null);
    }

    public byte[] serializeForRead(Vertex vertex, Direction direction, boolean isUnique, String key, String label, Object value) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        ValueUtils.serializeWithSalt(buffer, vertex.id());
        if (direction != null) {
            OrderedBytes.encodeInt8(buffer, direction == Direction.IN ? (byte) 1 : (byte) 0, Order.ASCENDING);
            OrderedBytes.encodeInt8(buffer, isUnique ? (byte) 1 : (byte) 0, Order.ASCENDING);
            if (key != null) {
                OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
                if (label != null) {
                    OrderedBytes.encodeString(buffer, label, Order.ASCENDING);
                    if (value != null) {
                        ValueUtils.serialize(buffer, value);
                    }
                }
            }
        }
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public byte[] serializeForWrite(Edge edge, Direction direction, boolean isUnique, String key) {
        Object inVertexId = edge.inVertex().id();
        Object outVertexId = edge.outVertex().id();
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        ValueUtils.serializeWithSalt(buffer, direction == Direction.IN ? inVertexId : outVertexId);
        OrderedBytes.encodeInt8(buffer, direction == Direction.IN ? (byte) 1 : (byte) 0, Order.ASCENDING);
        OrderedBytes.encodeInt8(buffer, isUnique ? (byte) 1 : (byte) 0, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, key, Order.ASCENDING);
        OrderedBytes.encodeString(buffer, edge.label(), Order.ASCENDING);
        if (key.equals(Constants.CREATED_AT)) {
            ValueUtils.serialize(buffer, ((HBaseEdge) edge).createdAt());
        } else {
            ValueUtils.serialize(buffer, edge.value(key));
        }
        if (!isUnique) {
            ValueUtils.serialize(buffer, direction == Direction.IN ? outVertexId : inVertexId);
            ValueUtils.serialize(buffer, edge.id());
        }
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public Edge deserialize(Result result) {
        byte[] bytes = result.getRow();
        PositionedByteRange buffer = new SimplePositionedByteRange(bytes);
        Object vertexId1 = ValueUtils.deserializeWithSalt(buffer);
        Direction direction = OrderedBytes.decodeInt8(buffer) == 1 ? Direction.IN : Direction.OUT;
        boolean isUnique = OrderedBytes.decodeInt8(buffer) == 1;
        String key = OrderedBytes.decodeString(buffer);
        String label = OrderedBytes.decodeString(buffer);
        Object value = ValueUtils.deserialize(buffer);
        Object vertexId2;
        Object edgeId;
        if (isUnique) {
            Cell vertexId2Cell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES);
            vertexId2 = ValueUtils.deserialize(CellUtil.cloneValue(vertexId2Cell));
            Cell edgeIdCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.EDGE_ID_BYTES);
            edgeId = ValueUtils.deserialize(CellUtil.cloneValue(edgeIdCell));
        } else {
            vertexId2 = ValueUtils.deserialize(buffer);
            edgeId = ValueUtils.deserialize(buffer);
        }
        Cell createdAttsCell = result.getColumnLatestCell(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES);
        Long createdAt = ValueUtils.deserialize(CellUtil.cloneValue(createdAttsCell));
        Map<String, Object> properties = new HashMap<>();
        properties.put(key, value);
        HBaseEdge newEdge;
        if (direction == Direction.IN) {
            newEdge = new HBaseEdge(graph, edgeId, label, createdAt, null, properties, false,
                    graph.findOrCreateVertex(vertexId1),
                    graph.findOrCreateVertex(vertexId2));
        } else {
            newEdge = new HBaseEdge(graph, edgeId, label, createdAt, null, properties, false,
                    graph.findOrCreateVertex(vertexId2),
                    graph.findOrCreateVertex(vertexId1));
        }
        HBaseEdge edge = (HBaseEdge) graph.findOrCreateEdge(edgeId);
        edge.copyFrom(newEdge);
        edge.setIndexKey(new IndexMetadata.Key(ElementType.EDGE, label, key));
        edge.setIndexTs(createdAttsCell.getTimestamp());
        return edge;
    }
}
