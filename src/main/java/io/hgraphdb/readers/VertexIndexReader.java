package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Triplet;

public class VertexIndexReader implements Reader<Triplet<IndexMetadata.Key, Vertex, Long>> {

    private final HBaseGraph graph;

    public VertexIndexReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public Triplet<IndexMetadata.Key, Vertex, Long> parse(Result result) {
        return makeVertex(result);
    }

    private Triplet<IndexMetadata.Key, Vertex, Long> makeVertex(Result result) {
        if (result.isEmpty()) return null;
        return graph.getVertexIndexModel().deserialize(result);
    }
}
