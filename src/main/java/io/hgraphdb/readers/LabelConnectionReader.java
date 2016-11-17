package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.LabelConnection;
import org.apache.hadoop.hbase.client.Result;

public class LabelConnectionReader implements Reader<LabelConnection> {

    private final HBaseGraph graph;

    public LabelConnectionReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public LabelConnection parse(Result result) {
        return makeLabel(result);
    }

    private LabelConnection makeLabel(Result result) {
        if (result.isEmpty()) return null;
        return graph.getLabelConnectionModel().deserialize(result);
    }
}
