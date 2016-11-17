package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.LabelConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class LabelConnectionRemover implements Mutator {

    private final HBaseGraph graph;
    private final LabelConnection labelConnection;

    public LabelConnectionRemover(HBaseGraph graph, LabelConnection labelConnection) {
        this.graph = graph;
        this.labelConnection = labelConnection;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getLabelConnectionModel().serialize(
                labelConnection.outVertexLabel(), labelConnection.edgeLabel(), labelConnection.inVertexLabel()));
        return IteratorUtils.of(delete);
    }
}
