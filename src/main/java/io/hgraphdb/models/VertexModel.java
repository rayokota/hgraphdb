package io.hgraphdb.models;

import io.hgraphdb.*;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.mutators.VertexRemover;
import io.hgraphdb.mutators.VertexWriter;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class VertexModel extends ElementModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertexModel.class);

    public VertexModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public VertexReader getReader() {
        return new VertexReader(graph);
    }

    public void writeVertex(Vertex vertex) {
        Creator creator = new VertexWriter(graph, vertex);
        Mutators.create(table, creator);
    }

    public void deleteVertex(Vertex vertex) {
        Mutator writer = new VertexRemover(graph, vertex);
        Mutators.write(table, writer);
    }

    public Iterator<Vertex> vertices() {
        final VertexReader parser = new VertexReader(graph);

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(Object fromId, int limit) {
        final VertexReader parser = new VertexReader(graph);

        Scan scan = fromId != null ? new Scan(ValueUtils.serializeWithSalt(fromId)) : new Scan();
        scan.setFilter(new PageFilter(limit));
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.limit(HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse), limit);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String label) {
        final VertexReader parser = new VertexReader(graph);

        Scan scan = getPropertyScan(label);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String key, Object value) {
        ElementHelper.validateProperty(key, value);
        final VertexReader parser = new VertexReader(graph);

        // This method will not work with counters
        byte[] val = ValueUtils.serialize(value);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(keyBytes, val);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String label, String key, Object value) {
        ElementHelper.validateProperty(key, value);
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.VERTEX, label, key);
        if (index != null) {
            LOGGER.debug("Using vertex index for ({}, {})", label, key);
            return graph.getVertexIndexModel().vertices(label, index.isUnique(), key, value);
        }
        final VertexReader parser = new VertexReader(graph);

        byte[] val = ValueUtils.serializePropertyValue(graph, ElementType.VERTEX, label, key, value);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(label, keyBytes, val);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        ElementHelper.validateProperty(key, inclusiveFrom);
        ElementHelper.validateProperty(key, exclusiveTo);
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.VERTEX, label, key);
        if (index != null) {
            LOGGER.debug("Using vertex index for ({}, {})", label, key);
            return graph.getVertexIndexModel().vertices(label, index.isUnique(), key, inclusiveFrom, exclusiveTo);
        }
        final VertexReader parser = new VertexReader(graph);

        byte[] fromVal = ValueUtils.serializePropertyValue(graph, ElementType.VERTEX, label, key, inclusiveFrom);
        byte[] toVal = ValueUtils.serializePropertyValue(graph, ElementType.VERTEX, label, key, exclusiveTo);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(label, keyBytes, fromVal, toVal);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return HBaseGraphUtils.mapWithCloseAtEnd(scanner, parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object from, int limit, boolean reversed) {
        ElementHelper.validateProperty(key, from != null ? from : new Object());
        IndexMetadata index = graph.getIndex(OperationType.READ, ElementType.VERTEX, label, key);
        if (index != null) {
            LOGGER.debug("Using vertex index for ({}, {})", label, key);
            return graph.getVertexIndexModel().verticesWithLimit(label, index.isUnique(), key, from, limit, reversed);
        }
        throw new HBaseGraphNotValidException("Method verticesWithLimit requires an index be defined");
    }
}
