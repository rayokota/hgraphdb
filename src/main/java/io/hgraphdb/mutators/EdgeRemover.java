package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeRemover implements Mutator {

    private final HBaseGraph graph;
    private final Edge edge;

    public EdgeRemover(HBaseGraph graph, Edge edge) {
        this.graph = graph;
        this.edge = edge;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(ValueUtils.serializeWithSalt(edge.id()));
        return IteratorUtils.of(delete);
    }
}
