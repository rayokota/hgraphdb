package io.hgraphdb.models;

import com.google.common.annotations.VisibleForTesting;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public abstract class BaseModel implements AutoCloseable {

    protected final HBaseGraph graph;
    protected final Table table;

    public BaseModel(HBaseGraph graph, Table table) {
        this.graph = graph;
        this.table = table;
    }

    public HBaseGraph getGraph() {
        return graph;
    }

    public Table getTable() {
        return table;
    }

    public void close() {
        close(false);
    }

    @VisibleForTesting
    public void close(boolean clear) {
        if (clear) clear();
        try {
            table.close();
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    private void clear() {
        try (ResultScanner scanner = table.getScanner(new Scan())) {
            scanner.iterator().forEachRemaining(result -> {
                try {
                    table.delete(new Delete(result.getRow()));
                } catch (IOException e) {
                    throw new HBaseGraphException(e);
                }
            });
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }
}
