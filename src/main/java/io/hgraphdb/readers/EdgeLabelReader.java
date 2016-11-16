package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.EdgeLabel;
import org.apache.hadoop.hbase.client.Result;

public class EdgeLabelReader implements Reader<EdgeLabel> {

    private final HBaseGraph graph;

    public EdgeLabelReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public EdgeLabel parse(Result result) {
        return makeLabel(result);
    }

    private EdgeLabel makeLabel(Result result) {
        if (result.isEmpty()) return null;
        return graph.getEdgeLabelModel().deserialize(result);
    }
}
