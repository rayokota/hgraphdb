package io.hgraphdb.mutators;

import com.google.common.collect.ImmutableMap;
import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

public class VertexIndexRemover implements Mutator {

    private final HBaseGraph graph;
    private final Vertex vertex;
    private final Map<String, Boolean> keys;
    private final Long ts;

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, String key, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = ImmutableMap.of(key, false);
        this.ts = ts;
    }

    public VertexIndexRemover(HBaseGraph graph, Vertex vertex, Iterator<IndexMetadata> indices, Long ts) {
        this.graph = graph;
        this.vertex = vertex;
        this.keys = IteratorUtils.collectMap(indices, IndexMetadata::propertyKey, IndexMetadata::isUnique);
        this.ts = ts;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        return keys.entrySet().stream().map(entry -> (Mutation) constructDelete(entry)).iterator();
    }

    private Delete constructDelete(Map.Entry<String, Boolean> entry) {
        long timestamp = ts != null ? ts : HConstants.LATEST_TIMESTAMP;
        boolean isUnique = entry.getValue();
        Delete delete = new Delete(graph.getVertexIndexModel().serializeForWrite(vertex, entry.getValue(), entry.getKey()));
        delete.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES, timestamp);
        if (isUnique) {
            delete.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.VERTEX_ID_BYTES, timestamp);
        }
        return delete;
    }
}
