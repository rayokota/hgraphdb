package io.hgraphdb.mapreduce.index;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.HBaseGraphTest;
import io.hgraphdb.HBaseGraphUtils;
import io.hgraphdb.testclassification.SlowTests;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;

@Category(SlowTests.class)
public class DropIndexTest extends HBaseGraphTest {
    private final static Log LOG = LogFactory.getLog(DropIndexTest.class);

    @Test
    public void testDropVertexIndex() throws Exception {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.createIndex(ElementType.VERTEX, "b", "key1");
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(12), T.label, "a", "key2", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(14), T.label, "b", "key1", 11);

        HBaseGraphConfiguration hconf = graph.configuration();
        Configuration conf = hconf.toHBaseConfiguration();
        Connection conn = graph.connection();
        Table table = conn.getTable(HBaseGraphUtils.getTableName(hconf, Constants.VERTEX_INDICES));

        verifyTableCount(table, 4);
        runDropIndex(conf, new String[] {"-t", "vertex", "-l", "a", "-p", "key1",
                "-d", "true", "-rf", "true", "-op", "/tmp"});
        verifyTableCount(table, 1);

        table.close();
    }

    @Test
    public void testDropEdgeIndex() throws Exception {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        graph.createIndex(ElementType.EDGE, "b", "key2");
        Vertex v0 = graph.addVertex(T.id, id(0));
        Vertex v1 = graph.addVertex(T.id, id(1));
        Vertex v2 = graph.addVertex(T.id, id(2));
        Vertex v3 = graph.addVertex(T.id, id(3));
        Vertex v4 = graph.addVertex(T.id, id(4));
        v0.addEdge("b", v1, "key1", 1);
        v0.addEdge("b", v2, "key1", 2);
        v0.addEdge("b", v3, "key2", 3);
        v0.addEdge("a", v1, "key1", 1);
        v0.addEdge("b", v4, "key1", 4);

        HBaseGraphConfiguration hconf = graph.configuration();
        Configuration conf = hconf.toHBaseConfiguration();
        Connection conn = graph.connection();
        Table table = conn.getTable(HBaseGraphUtils.getTableName(hconf, Constants.EDGE_INDICES));

        verifyTableCount(table, 5*2 + 4*2);  // 5 edge endpoints and 4 indices
        runDropIndex(conf, new String[] {"-t", "edge", "-l", "b", "-p", "key1",
                "-d", "true", "-rf", "true", "-op", "/tmp"});
        verifyTableCount(table, 5*2 + 1*2);  // 5 edge endpoints and 1 index

        table.close();
    }

    private boolean runDropIndex(Configuration conf, String[] args) throws Exception {
        int status = ToolRunner.run(conf, new DropIndex(), args);
        return status == 0;
    }

    private void verifyTableCount(final Table table, final int count) throws IOException {
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        ResultScanner scanner = table.getScanner(scan);
        int i = 0;
        for (Result r : scanner) {
            i++;
        }
        assertEquals(count, i);
        scanner.close();
    }
}

