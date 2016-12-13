package io.hgraphdb.giraph.examples;

import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.HBaseGraphTest;
import io.hgraphdb.giraph.HBaseEdgeInputFormat;
import io.hgraphdb.giraph.HBaseVertexInputFormat;
import io.hgraphdb.giraph.utils.InternalHBaseVertexRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for max computation
 */
public class MaxComputationWithGraphOutputTest extends HBaseGraphTest {
    @Ignore
    @Test
    public void testMax() throws Exception {
        HBaseGraphConfiguration hconf = graph.configuration();

        GiraphConfiguration conf = new GiraphConfiguration(hconf.toHBaseConfiguration());
        conf.setComputationClass(MaxComputation.class);
        conf.setEdgeInputFormatClass(HBaseEdgeInputFormat.class);
        conf.setVertexInputFormatClass(HBaseVertexInputFormat.class);
        conf.setEdgeOutputFormatClass(CreateEdgeOutputFormat.class);
        conf.setVertexOutputFormatClass(MaxPropertyVertexOutputFormat.class);

        Vertex v1 = graph.addVertex(T.id, 1, T.label, "hi");
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "world");
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "bye");
        v5.addEdge("e", v1);
        v1.addEdge("e", v5);
        v1.addEdge("e", v2);
        v2.addEdge("e", v5);

        InternalHBaseVertexRunner.run(conf);

        graph.vertices().forEachRemaining(v -> assertEquals(5, v.property("max").value()));
        assertEquals(4, IteratorUtils.count(IteratorUtils.filter(graph.edges(), e -> e.label().equals("e2"))));
    }
}
