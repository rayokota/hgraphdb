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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SlowTests.class)
public class PopulateIndexTest extends HBaseGraphTest {
    private final static Log LOG = LogFactory.getLog(PopulateIndexTest.class);

    @Test
    public void testPopulateIndex() throws Exception {
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(12), T.label, "a", "key2", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(14), T.label, "b", "key1", 11);

        graph.createIndex(ElementType.VERTEX, "a", "key1");

        HBaseGraphConfiguration hconf = graph.configuration();
        Configuration conf = hconf.toHBaseConfiguration();
        Connection conn = MockConnectionFactory.createConnection(conf);
        Table table = conn.getTable(HBaseGraphUtils.getTableName(hconf, Constants.VERTEX_INDICES));
        runPopulateIndex(conf, new String[] {"-t", "vertex", "-l", "a", "-p", "key1",
                "-d", "true", "-r", "true", "-op", "/tmp"});
        verify(table);
        table.close();
    }

    private boolean runPopulateIndex(Configuration conf, String[] args) throws Exception {
        int status = ToolRunner.run(conf, new PopulateIndex(), args);
        return status == 0;
    }

    private void verify(final Table table) throws IOException {
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result r : scanner) {
            count++;
        }
        org.junit.Assert.assertEquals(4, count);
        scanner.close();
    }
}

