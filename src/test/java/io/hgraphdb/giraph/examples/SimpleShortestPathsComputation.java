package io.hgraphdb.giraph.examples;

import io.hgraphdb.giraph.EdgeValueWritable;
import io.hgraphdb.giraph.HBaseComputation;
import io.hgraphdb.giraph.ObjectWritable;
import io.hgraphdb.giraph.VertexValueWritable;
import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
        name = "Shortest paths",
        description = "Finds all shortest paths from a selected vertex"
)
public class SimpleShortestPathsComputation extends
        HBaseComputation<Long, DoubleWritable, FloatWritable, DoubleWritable> {
    /**
     * The shortest paths id
     */
    public static final LongConfOption SOURCE_ID =
            new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
                    "The shortest paths id");
    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(SimpleShortestPathsComputation.class);

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<ObjectWritable<Long>, ?, ?> vertex) {
        long id = vertex.getId().get();
        return id == SOURCE_ID.get(getConf());
    }

    @Override
    public void compute(
            Vertex<ObjectWritable<Long>, VertexValueWritable<DoubleWritable>, EdgeValueWritable<FloatWritable>> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        VertexValueWritable<DoubleWritable> vertexValue = vertex.getValue();
        if (getSuperstep() == 0) {
            vertexValue.setValue(new DoubleWritable(Double.MAX_VALUE));
        }
        double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
        for (DoubleWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
                    " vertex value = " + vertex.getValue());
        }
        if (minDist < vertexValue.getValue().get()) {
            vertexValue.setValue(new DoubleWritable(minDist));
            for (Edge<ObjectWritable<Long>, EdgeValueWritable<FloatWritable>> edge : vertex.getEdges()) {
                double distance = minDist + ((Number) edge.getValue().getEdge().property("weight").value()).doubleValue();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Vertex " + vertex.getId() + " sent to " +
                            edge.getTargetVertexId() + " = " + distance);
                }
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
            }
        }
        vertex.voteToHalt();
    }
}
