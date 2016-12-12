package io.hgraphdb.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public abstract class HBaseComputation<I, V extends Writable, E extends Writable, M extends Writable>
        extends BasicComputation<ObjectWritable<I>, VertexValueWritable<V>, EdgeValueWritable<E>, M> {

    @Override
    public abstract void compute(final Vertex<ObjectWritable<I>, VertexValueWritable<V>, EdgeValueWritable<E>> vertex,
                                 final Iterable<M> messages) throws IOException;
}
