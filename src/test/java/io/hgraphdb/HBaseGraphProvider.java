package io.hgraphdb;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class HBaseGraphProvider extends AbstractGraphProvider {

    protected static final HBaseGraphConfiguration.InstanceType type = System.getenv("HGRAPHDB_INSTANCE_TYPE") != null
            ? HBaseGraphConfiguration.InstanceType.valueOf(System.getenv("HGRAPHDB_INSTANCE_TYPE"))
            : HBaseGraphConfiguration.InstanceType.MOCK;

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(HBaseEdge.class);
        add(HBaseElement.class);
        add(HBaseGraph.class);
        add(HBaseProperty.class);
        add(HBaseVertex.class);
        add(HBaseVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData graphData) {
        Map<String, Object> config = null;
        switch (type) {
            case MOCK:
                config = new HashMap<String, Object>() {{
                    put(Graph.GRAPH, HBaseGraph.class.getName());
                    put(HBaseGraphConfiguration.Keys.INSTANCE_TYPE, HBaseGraphConfiguration.InstanceType.MOCK.toString());
                    put(HBaseGraphConfiguration.Keys.GRAPH_NAMESPACE, graphName);
                    put(HBaseGraphConfiguration.Keys.CREATE_TABLES, true);
                    put(HBaseGraphConfiguration.Keys.USE_LONG_FOR_NUMBERS, false);
                }};
                break;
            case BIGTABLE:
                config = new HashMap<String, Object>() {{
                    put(Graph.GRAPH, HBaseGraph.class.getName());
                    put(HBaseGraphConfiguration.Keys.INSTANCE_TYPE, HBaseGraphConfiguration.InstanceType.BIGTABLE.toString());
                    put(HBaseGraphConfiguration.Keys.GRAPH_NAMESPACE, graphName);
                    put(HBaseGraphConfiguration.Keys.GRAPH_TABLE_PREFIX, graphName);
                    put(HBaseGraphConfiguration.Keys.CREATE_TABLES, true);
                    put(HBaseGraphConfiguration.Keys.USE_LONG_FOR_NUMBERS, false);
                    put("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_x.BigtableConnection");
                    put("google.bigtable.instance.id", "hgraphdb-bigtable");
                    put("google.bigtable.project.id", "rayokota2");
                }};
                break;
            case DISTRIBUTED:
                config = new HashMap<String, Object>() {{
                    put(Graph.GRAPH, HBaseGraph.class.getName());
                    put(HBaseGraphConfiguration.Keys.INSTANCE_TYPE, HBaseGraphConfiguration.InstanceType.DISTRIBUTED.toString());
                    put(HBaseGraphConfiguration.Keys.GRAPH_NAMESPACE, graphName);
                    put(HBaseGraphConfiguration.Keys.CREATE_TABLES, true);
                    put(HBaseGraphConfiguration.Keys.USE_LONG_FOR_NUMBERS, false);
                    put("hbase.zookeeper.quorum", "127.0.0.1");
                    put("zookeeper.znode.parent", "/hbase-unsecure");
                }};
                break;
        }
        return config;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (graph != null) {
            ((HBaseGraph) graph).close(true);
        }
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.createIndices((HBaseGraph) graph, loadGraphWith.value());
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    private void createIndices(final HBaseGraph graph, final LoadGraphWith.GraphData graphData) {
        final Random random = new Random();
        final boolean pick = random.nextBoolean();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            if (pick) {
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "artist", "name");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "song", "name");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "song", "songType");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "song", "performances");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "followedBy", "weight");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "sungBy", "weight");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "writtenBy", "weight");
            } // else no indices
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            if (pick) {
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "person", "name");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "person", "age");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "software", "name");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "software", "lang");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "created", "weight");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "knows", "weight");
            } // else no indices
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            if (pick) {
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "vertex", "name");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "vertex", "age");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.VERTEX, "vertex", "lang");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "created", "weight");
                if (random.nextBoolean())
                    graph.createIndex(ElementType.EDGE, "knows", "weight");
            } // else no indices
        } else {
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}
