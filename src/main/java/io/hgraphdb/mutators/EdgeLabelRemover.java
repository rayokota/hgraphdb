package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.EdgeLabel;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeLabelRemover implements Mutator {

    private final HBaseGraph graph;
    private final EdgeLabel label;

    public EdgeLabelRemover(HBaseGraph graph, EdgeLabel label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getEdgeLabelModel().serialize(
                label.label(), label.outVertexLabel(), label.inVertexLabel()));
        return IteratorUtils.of(delete);
    }
}
