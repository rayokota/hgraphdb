package io.hgraphdb.models;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import io.hgraphdb.Serializer;
import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeRemover;
import io.hgraphdb.mutators.EdgeWriter;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import io.hgraphdb.readers.EdgeReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.Iterator;


public class EdgeModel extends ElementModel {

    public EdgeModel(HBaseGraph graph, Table table) {
        super(graph, table);
    }

    public EdgeReader getReader() {
        return new EdgeReader(graph);
    }

    public void writeEdge(Edge edge) {
        Creator creator = new EdgeWriter(graph, edge);
        Mutators.create(table, creator);
    }

    public void deleteEdge(Edge edge) {
        Mutator writer = new EdgeRemover(graph, edge);
        Mutators.write(table, writer);
    }

    public Iterator<Edge> edges() {
        final EdgeReader parser = new EdgeReader(graph);

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            return IteratorUtils.<Result, Edge>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Edge> edges(Object fromId, int limit) {
        final EdgeReader parser = new EdgeReader(graph);

        Scan scan = fromId != null ? new Scan(Serializer.serializeWithSalt(fromId)) : new Scan();
        scan.setFilter(new PageFilter(limit));
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.limit(IteratorUtils.<Result, Edge>map(scanner.iterator(), parser::parse), limit);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Iterator<Edge> edges(String key, Object value) {
        ElementHelper.validateProperty(key, value);
        final EdgeReader parser = new EdgeReader(graph);

        byte[] val = Serializer.serialize(value);
        final byte[] keyBytes = Bytes.toBytes(key);
        Scan scan = getPropertyScan(keyBytes, val);
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            return IteratorUtils.<Result, Edge>map(scanner.iterator(), parser::parse);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }
}
