package io.hgraphdb.giraph.examples;

import com.google.common.collect.Maps;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.HBaseGraphTest;
import io.hgraphdb.giraph.HBaseEdgeInputFormat;
import io.hgraphdb.giraph.HBaseVertexInputFormat;
import io.hgraphdb.giraph.utils.InternalHBaseVertexRunner;
import io.hgraphdb.testclassification.SlowTests;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for max computation
 */
@Category(SlowTests.class)
public class MaxComputationTest extends HBaseGraphTest {
    @Test
    public void testMax() throws Exception {

        Vertex v1 = graph.addVertex(T.id, 1, T.label, "hi");
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "world");
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "bye");
        v5.addEdge("e", v1);
        v1.addEdge("e", v5);
        v1.addEdge("e", v2);
        v2.addEdge("e", v5);

        HBaseGraphConfiguration hconf = graph.configuration();
        GiraphConfiguration conf = new GiraphConfiguration(hconf.toHBaseConfiguration());
        conf.setComputationClass(MaxComputation.class);
        conf.setEdgeInputFormatClass(HBaseEdgeInputFormat.class);
        conf.setVertexInputFormatClass(HBaseVertexInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        Iterable<String> results = InternalHBaseVertexRunner.run(conf);

        Map<Integer, Integer> values = parseResults(results);
        assertEquals(3, values.size());
        assertEquals(5, (int) values.get(1));
        assertEquals(5, (int) values.get(2));
        assertEquals(5, (int) values.get(5));
    }

    private static Map<Integer, Integer> parseResults(Iterable<String> results) {
        Map<Integer, Integer> values = Maps.newHashMap();
        for (String line : results) {
            String[] tokens = line.split("\\s+");
            int id = Integer.valueOf(tokens[0]);
            int value = Integer.valueOf(tokens[1]);
            values.put(id, value);
        }
        return values;
    }
}
