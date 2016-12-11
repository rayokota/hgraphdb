/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hgraphdb.giraph;

import com.google.common.collect.Maps;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for max computation
 */
public class MaxComputationTest {
    @Test
    public void testMax() throws Exception {
        String[] s = {
                "5 1",
                "1 5 2",
                "2 5",
        };

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(MaxComputation.class);
        conf.setEdgeInputFormatClass(HBaseEdgeInputFormat.class);
        conf.setVertexInputFormatClass(HBaseVertexInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        HBaseGraph graph = new HBaseGraph(new HBaseGraphConfiguration(conf), MockConnectionFactory.createConnection(conf));
        Vertex v1 = graph.addVertex(T.id, 1, T.label, "hi");
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "world");
        Vertex v5 = graph.addVertex(T.id, 5, T.label, "bye");
        v5.addEdge("e", v1);
        v1.addEdge("e", v5);
        v1.addEdge("e", v2);
        v2.addEdge("e", v5);

        Iterable<String> results = InternalHBaseVertexRunner.run(conf, s);

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
