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
package io.hgraphdb.giraph.examples;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.giraph.HBaseEdgeInputFormat;
import io.hgraphdb.giraph.HBaseVertexInputFormat;
import io.hgraphdb.giraph.InternalHBaseVertexRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for max computation
 */
public class MaxComputationWithGraphOutputTest {
    @Test
    public void testMax() throws Exception {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(MaxComputation.class);
        conf.setEdgeInputFormatClass(HBaseEdgeInputFormat.class);
        conf.setVertexInputFormatClass(HBaseVertexInputFormat.class);
        conf.setEdgeOutputFormatClass(CreateEdgeOutputFormat.class);
        conf.setVertexOutputFormatClass(MaxPropertyVertexOutputFormat.class);

        HBaseGraphConfiguration hconf = new HBaseGraphConfiguration(conf);
        hconf.setInstanceType(HBaseGraphConfiguration.InstanceType.MOCK);

        HBaseGraph graph = new HBaseGraph(hconf);
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
