package io.hgraphdb.models;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import io.hgraphdb.IndexType;
import io.hgraphdb.OperationType;
import io.hgraphdb.Serializer;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.mutators.VertexRemover;
import io.hgraphdb.mutators.VertexWriter;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.Iterator;

public class VertexModel extends ElementModel {

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
            return IteratorUtils.<Result, Vertex>map(scanner.iterator(), parser::parse);
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
            return IteratorUtils.<Result, Vertex>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String key, Object value) {
        ElementHelper.validateProperty(key, value);
        final VertexReader parser = new VertexReader(graph);

        byte[] val = Serializer.serialize(value);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(keyBytes, val);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Vertex>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String label, String key, Object value) {
        ElementHelper.validateProperty(key, value);
        if (graph.hasIndex(OperationType.READ, IndexType.VERTEX, label, key)) {
            return graph.getVertexIndexModel().vertices(label, key, value);
        }
        final VertexReader parser = new VertexReader(graph);

        byte[] val = Serializer.serialize(value);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(label, keyBytes, val);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Vertex>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Vertex> vertices(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        ElementHelper.validateProperty(key, inclusiveFrom);
        ElementHelper.validateProperty(key, exclusiveTo);
        if (graph.hasIndex(OperationType.READ, IndexType.VERTEX, label, key)) {
            return graph.getVertexIndexModel().vertices(label, key, inclusiveFrom, exclusiveTo);
        }
        final VertexReader parser = new VertexReader(graph);

        byte[] fromVal = Serializer.serialize(inclusiveFrom);
        byte[] toVal = Serializer.serialize(exclusiveTo);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(label, keyBytes, fromVal, toVal);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Vertex>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }
}
