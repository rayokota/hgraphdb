package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.IndexMetadata;
import io.hgraphdb.Serializer;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class VertexIndexWriter implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final Iterator<String> keys;

    public VertexIndexWriter(HBaseGraph graph, Vertex vertex, Iterator<IndexMetadata> indices) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.map(indices, IndexMetadata::propertyKey);
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return IteratorUtils.map(keys, key -> {
            Put put = new Put(graph.getVertexIndexModel().serializeForWrite(vertex, key));
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                    Serializer.serialize(((HBaseVertex) vertex).createdAt()));
            return put;
        });
    }
}
