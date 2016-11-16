package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.VertexLabel;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class VertexLabelRemover implements Mutator {

    private final HBaseGraph graph;
    private final VertexLabel label;

    public VertexLabelRemover(HBaseGraph graph, VertexLabel label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getVertexLabelModel().serialize(label.label()));
        return IteratorUtils.of(delete);
    }
}
