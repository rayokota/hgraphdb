package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.After;
import org.junit.Before;

public class HBaseGraphTest {

    protected static final HBaseGraphConfiguration.InstanceType type = System.getenv("HGRAPHDB_INSTANCE_TYPE") != null
            ? HBaseGraphConfiguration.InstanceType.valueOf(System.getenv("HGRAPHDB_INSTANCE_TYPE"))
            : HBaseGraphConfiguration.InstanceType.MOCK;

    protected HBaseGraph graph;

    @Before
    public void makeGraph() {
        graph = (HBaseGraph) GraphFactory.open(generateGraphConfig("testgraph"));
    }

    protected HBaseGraphConfiguration generateGraphConfig(String graphNamespace) {
        HBaseGraphConfiguration config = new HBaseGraphConfiguration()
                .setGraphNamespace(graphNamespace)
                .setCreateTables(true)
                .setRegionCount(1);
        switch (type) {
            case MOCK:
                config.setInstanceType(HBaseGraphConfiguration.InstanceType.MOCK);
                break;
            case BIGTABLE:
                config.setInstanceType(HBaseGraphConfiguration.InstanceType.BIGTABLE);
                config.set("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_2.BigtableConnection");
                config.set("google.bigtable.instance.id", "hgraphdb-bigtable");
                config.set("google.bigtable.project.id", "rayokota2");
                break;
            case DISTRIBUTED:
                config.setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED);
                config.set("hbase.zookeeper.quorum", "127.0.0.1");
                config.set("zookeeper.znode.parent", "/hbase-unsecure");
                break;
        }
        return config;
    }

    @After
    public void clearGraph() {
        graph.close(true);
    }

    protected static String id(int idNum) {
        return String.format("%08d", idNum);
    }
}
