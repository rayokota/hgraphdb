package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class EdgeIndexReader extends ElementReader<Edge> {

    public EdgeIndexReader(HBaseGraph graph) {
        super(graph);
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
