package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class VertexIndexReader extends ElementReader<Vertex> {

    public VertexIndexReader(HBaseGraph graph) {
        super(graph);
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
