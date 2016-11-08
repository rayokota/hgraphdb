package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class EdgeIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Edge edge;
    private final String key;
    private final Long ts;

    public EdgeIndexRemover(HBaseGraph graph, Edge edge, String key, Long ts) {
        this.graph = graph;
        this.edge = edge;
        this.key = key;
        this.ts = ts;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete in = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.IN, key));
        Delete out = new Delete(graph.getEdgeIndexModel().serializeForWrite(edge, Direction.OUT, key));
        if (ts != null) {
            in.setTimestamp(ts);
            out.setTimestamp(ts);
        }
        return IteratorUtils.of(in, out);
    }
}
