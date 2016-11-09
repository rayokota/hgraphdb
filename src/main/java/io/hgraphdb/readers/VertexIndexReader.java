package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Triplet;

public class VertexIndexReader implements Reader<Vertex> {

    private final HBaseGraph graph;

    public VertexIndexReader(HBaseGraph graph) {
        this.graph = graph;
    }

    @Override
    public Vertex parse(Result result) {
        return makeVertex(result);
    }

    private Vertex makeVertex(Result result) {
        if (result.isEmpty()) return null;
        return graph.getVertexIndexModel().deserialize(result);
    }
}
