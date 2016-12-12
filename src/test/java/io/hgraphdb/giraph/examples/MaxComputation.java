package io.hgraphdb.giraph.examples;

import io.hgraphdb.HBaseVertex;
import io.hgraphdb.giraph.EdgeValueWritable;
import io.hgraphdb.giraph.HBaseComputation;
import io.hgraphdb.giraph.ObjectWritable;
import io.hgraphdb.giraph.VertexValueWritable;
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
