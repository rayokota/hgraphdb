package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.After;
import org.junit.Before;

public class HBaseGraphTest {

    protected static final boolean useMock = true;

    protected HBaseGraph graph;

    @Before
    public void makeGraph() {
        graph = (HBaseGraph) GraphFactory.open(generateGraphConfig("testgraph"));
    }

    protected HBaseGraphConfiguration generateGraphConfig(String graphNamespace) {
        HBaseGraphConfiguration config = new HBaseGraphConfiguration()
                .setGraphNamespace(graphNamespace)
                .setCreateTables(true);
        if (useMock) {
            return config.setInstanceType(HBaseGraphConfiguration.InstanceType.MOCK);
        } else {
            config.set("hbase.zookeeper.quorum", "127.0.0.1");
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            return config.setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED);
        }
    }

    @After
    public void clearGraph() {
        graph.close(true);
    }

    protected static String id(int idNum) {
        return String.format("%08d", idNum);
    }
}
