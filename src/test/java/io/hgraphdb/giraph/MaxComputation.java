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

import io.hgraphdb.HBaseVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * Simple algorithm that computes the max value in the graph.
 */
public class MaxComputation extends HBaseComputation<IntWritable> {

    @Override
    public void compute(final Vertex<ObjectWritable, VertexValueWritable, EdgeValueWritable> vertex,
                        final Iterable<IntWritable> messages) throws IOException {
        VertexValueWritable vertexValue = vertex.getValue();
        HBaseVertex v = vertexValue.getVertex();
        if (!(vertexValue.getValue() instanceof IntWritable)) {
            vertexValue.setValue(new IntWritable(((Number) v.id()).intValue()));
        }
        int value = ((IntWritable) vertexValue.getValue()).get();
        boolean changed = false;
        for (IntWritable message : messages) {
            if (value < message.get()) {
                value = message.get();
                vertexValue.setValue(new IntWritable(value));
                changed = true;
            }
        }
        if (getSuperstep() == 0 || changed) {
            sendMessageToAllEdges(vertex, new IntWritable(value));
        }
        vertex.voteToHalt();
    }
}
