package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.javatuples.Triplet;

public class EdgeIndexReader implements Reader<Triplet<IndexMetadata.Key, Edge, Long>> {

    private final HBaseGraph graph;

    public EdgeIndexReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public Triplet<IndexMetadata.Key, Edge, Long> parse(Result result) {
        return makeEdge(result);
    }

    private Triplet<IndexMetadata.Key, Edge, Long> makeEdge(Result result) {
        if (result.isEmpty()) return null;
        return graph.getEdgeIndexModel().deserialize(result);
    }
}
