package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Result;

public class IndexMetadataReader implements Reader<IndexMetadata> {

    private final HBaseGraph graph;

    public IndexMetadataReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public IndexMetadata parse(Result result) {
        return makeIndexMetadata(result);
    }

    private IndexMetadata makeIndexMetadata(Result result) {
        if (result.isEmpty()) return null;
        return graph.getIndexMetadataModel().deserialize(result);
    }
}
