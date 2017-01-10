package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import io.hgraphdb.testclassification.SlowTests;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;

@Category(SlowTests.class)
public class PopulateIndexTest extends HBaseGraphTest {
    private final static Log LOG = LogFactory.getLog(PopulateIndexTest.class);

    @Test
    public void testPopulateVertexIndex() throws Exception {
        assertEquals(0, count(graph.vertices()));
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(12), T.label, "a", "key2", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(14), T.label, "b", "key1", 11);

        graph.createIndex(ElementType.VERTEX, "a", "key1", false, true, true);

        HBaseGraphConfiguration hconf = graph.configuration();
        Configuration conf = hconf.toHBaseConfiguration();
        Connection conn = graph.connection();
        Table table = conn.getTable(HBaseGraphUtils.getTableName(hconf, Constants.VERTEX_INDICES));

        runPopulateIndex(conf, new String[] {"-t", "vertex", "-l", "a", "-p", "key1",
                "-d", "true", "-rf", "true", "-op", "/tmp"});
        verifyTableCount(table, 3);

        table.close();
    }

    @Test
    public void testPopulateEdgeIndex() throws Exception {
        assertEquals(0, count(graph.vertices()));
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

        graph.createIndex(ElementType.EDGE, "b", "key1", false, true, true);

        HBaseGraphConfiguration hconf = graph.configuration();
        Configuration conf = hconf.toHBaseConfiguration();
        Connection conn = graph.connection();
        Table table = conn.getTable(HBaseGraphUtils.getTableName(hconf, Constants.EDGE_INDICES));

        verifyTableCount(table, 5*2);  // 5 edge endpoints
        runPopulateIndex(conf, new String[] {"-t", "edge", "-l", "b", "-p", "key1",
                "-d", "true", "-rf", "true", "-op", "/tmp"});
        verifyTableCount(table, 5*2 + 3*2);  // 5 edge endpoints and 3 indices

        table.close();
    }

    private boolean runPopulateIndex(Configuration conf, String[] args) throws Exception {
        int status = ToolRunner.run(conf, new PopulateIndex(), args);
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

