package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.LabelMetadata;
import org.apache.hadoop.hbase.client.Result;

public class LabelMetadataReader implements Reader<LabelMetadata> {

    private final HBaseGraph graph;

    public LabelMetadataReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public LabelMetadata parse(Result result) {
        return makeLabel(result);
    }

    private LabelMetadata makeLabel(Result result) {
        if (result.isEmpty()) return null;
        return graph.getLabelMetadataModel().deserialize(result);
    }
}
