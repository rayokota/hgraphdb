package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.VertexLabel;
import org.apache.hadoop.hbase.client.Result;

public class VertexLabelReader implements Reader<VertexLabel> {

    private final HBaseGraph graph;

    public VertexLabelReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public VertexLabel parse(Result result) {
        return makeLabel(result);
    }

    private VertexLabel makeLabel(Result result) {
        if (result.isEmpty()) return null;
        return graph.getVertexLabelModel().deserialize(result);
    }
}
