package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class EdgeIndexReader implements Reader<Edge> {

    private final HBaseGraph graph;

    public EdgeIndexReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public Edge parse(Result result) {
        return makeEdge(result);
    }

    private Edge makeEdge(Result result) {
        if (result.isEmpty()) return null;
        return graph.getEdgeIndexModel().deserialize(result);
    }
}
