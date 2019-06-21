package io.hgraphdb;

import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeIndexRemover;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.EdgeWriter;
import io.hgraphdb.mutators.PropertyWriter;
import io.hgraphdb.mutators.VertexIndexRemover;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.mutators.VertexWriter;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public final class HBaseBulkLoader implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBulkLoader.class);

    private static final BufferedMutator.ExceptionListener LISTENER = (e, mutator) -> {
        for (int i = 0; i < e.getNumExceptions(); i++) {
            LOGGER.warn("Failed to send put: " + e.getRow(i));
        }
    };

    private final HBaseGraph graph;
    private final BufferedMutator edgesMutator;
    private final BufferedMutator edgeIndicesMutator;
    private final BufferedMutator verticesMutator;
    private final BufferedMutator vertexIndicesMutator;
    private final boolean skipWAL;

    public HBaseBulkLoader(HBaseGraphConfiguration config) {
        this(new HBaseGraph(config, HBaseGraphUtils.getConnection(config)));
    }

    public HBaseBulkLoader(HBaseGraph graph) {
        this(graph,
                getBufferedMutator(graph, Constants.EDGES),
                getBufferedMutator(graph, Constants.EDGE_INDICES),
                getBufferedMutator(graph, Constants.VERTICES),
                getBufferedMutator(graph, Constants.VERTEX_INDICES));
    }

    private static BufferedMutator getBufferedMutator(HBaseGraph graph, String tableName) {
        try {
            HBaseGraphConfiguration config = graph.configuration();
            TableName name = HBaseGraphUtils.getTableName(config, tableName);
            BufferedMutatorParams params = new BufferedMutatorParams(name).listener(LISTENER);
            return graph.connection().getBufferedMutator(params);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public HBaseBulkLoader(HBaseGraph graph,
                           BufferedMutator edgesMutator,
                           BufferedMutator edgeIndicesMutator,
                           BufferedMutator verticesMutator,
                           BufferedMutator vertexIndicesMutator) {
        this.graph = graph;
        this.edgesMutator = edgesMutator;
        this.edgeIndicesMutator = edgeIndicesMutator;
        this.verticesMutator = verticesMutator;
        this.vertexIndicesMutator = vertexIndicesMutator;
        this.skipWAL = graph.configuration().getBulkLoaderSkipWAL();
    }

    public HBaseGraph getGraph() {
        return graph;
    }

    public Vertex addVertex(final Object... keyValues) {
        try {
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
            final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

            idValue = HBaseGraphUtils.generateIdIfNeeded(idValue);
            long now = System.currentTimeMillis();
            HBaseVertex vertex = new HBaseVertex(graph, idValue, label, now, now,
                    HBaseGraphUtils.propertiesToMultimap(keyValues));
            vertex.validate();

            Iterator<IndexMetadata> indices = vertex.getIndices(OperationType.WRITE);
            indexVertex(vertex, indices);

            Creator creator = new VertexWriter(graph, vertex);
            if (verticesMutator != null) verticesMutator.mutate(getMutationList(creator.constructInsertions()));

            return vertex;
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public void indexVertex(Vertex vertex, Iterator<IndexMetadata> indices) {
        try {
            VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, indices, null);
            if (vertexIndicesMutator != null) vertexIndicesMutator.mutate(getMutationList(writer.constructInsertions()));
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        try {
            if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
            ElementHelper.validateLabel(label);
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

            idValue = HBaseGraphUtils.generateIdIfNeeded(idValue);
            long now = System.currentTimeMillis();
            HBaseEdge edge = new HBaseEdge(graph, idValue, label, now, now,
                    HBaseGraphUtils.propertiesToMap(keyValues), inVertex, outVertex);
            edge.validate();

            Iterator<IndexMetadata> indices = edge.getIndices(OperationType.WRITE);
            indexEdge(edge, indices);

            EdgeIndexWriter writer = new EdgeIndexWriter(graph, edge, Constants.CREATED_AT);
            if (edgeIndicesMutator != null) edgeIndicesMutator.mutate(getMutationList(writer.constructInsertions()));

            Creator creator = new EdgeWriter(graph, edge);
            if (edgesMutator != null) edgesMutator.mutate(getMutationList(creator.constructInsertions()));

            return edge;
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public void indexEdge(Edge edge, Iterator<IndexMetadata> indices) {
        try {
            EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, edge, indices, null);
            if (edgeIndicesMutator != null) edgeIndicesMutator.mutate(getMutationList(indexWriter.constructInsertions()));
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public void setProperty(Edge edge, String key, Object value) {
        try {
            HBaseEdge e = (HBaseEdge) edge;
            ElementHelper.validateProperty(key, value);

            graph.validateProperty(e.getElementType(), e.label(), key, value);

            // delete from index model before setting property
            Object oldValue = null;
            boolean hasIndex = e.hasIndex(OperationType.WRITE, key);
            if (hasIndex) {
                // only load old value if using index
                oldValue = e.getProperty(key);
                if (oldValue != null && !oldValue.equals(value)) {
                    EdgeIndexRemover indexRemover = new EdgeIndexRemover(graph, e, key, null);
                    if (edgeIndicesMutator != null) edgeIndicesMutator.mutate(getMutationList(indexRemover.constructMutations()));
                }
            }

            e.getProperties().put(key, value);
            e.updatedAt(System.currentTimeMillis());

            if (hasIndex) {
                if (oldValue == null || !oldValue.equals(value)) {
                    EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, e, key);
                    if (edgeIndicesMutator != null) edgeIndicesMutator.mutate(getMutationList(indexWriter.constructInsertions()));
                }
            }
            PropertyWriter propertyWriter = new PropertyWriter(graph, e, key, value);
            if (edgesMutator != null) edgesMutator.mutate(getMutationList(propertyWriter.constructMutations()));
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public void setProperty(Vertex vertex, String key, Object value) {
        try {
            HBaseVertex v = (HBaseVertex) vertex;
            ElementHelper.validateProperty(key, value);

            graph.validateProperty(v.getElementType(), v.label(), key, value);

            // delete from index model before setting property
            Object oldValue = null;
            boolean hasIndex = v.hasIndex(OperationType.WRITE, key);
            if (hasIndex) {
                // only load old value if using index
                oldValue = v.getSingleProperty(key).orElse(null);
                if (oldValue != null && !oldValue.equals(value)) {
                    VertexIndexRemover indexRemover = new VertexIndexRemover(graph, v, key, null);
                    if (vertexIndicesMutator != null) vertexIndicesMutator.mutate(getMutationList(indexRemover.constructMutations()));
                }
            }

            v.cacheProperty(key, value);
            v.updatedAt(System.currentTimeMillis());

            if (hasIndex) {
                if (oldValue == null || !oldValue.equals(value)) {
                    VertexIndexWriter indexWriter = new VertexIndexWriter(graph, v, key);
                    if (vertexIndicesMutator != null) vertexIndicesMutator.mutate(getMutationList(indexWriter.constructInsertions()));
                }
            }
            PropertyWriter propertyWriter = new PropertyWriter(graph, v, key, value);
            if (verticesMutator != null) verticesMutator.mutate(getMutationList(propertyWriter.constructMutations()));
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    private List<? extends Mutation> getMutationList(Iterator<? extends Mutation> mutations) {
        return IteratorUtils.list(IteratorUtils.consume(mutations,
                m -> m.setDurability(skipWAL ? Durability.SKIP_WAL : Durability.USE_DEFAULT)));
    }

    @Override
	public void close() {
        try {
            if (edgesMutator != null) edgesMutator.close();
            if (edgeIndicesMutator != null) edgeIndicesMutator.close();
            if (verticesMutator != null) verticesMutator.close();
            if (vertexIndicesMutator != null) vertexIndicesMutator.close();
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }
}
