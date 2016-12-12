package io.hgraphdb;

import io.hgraphdb.mutators.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public final class HBaseBulkLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBulkLoader.class);

    private HBaseGraph graph;
    private BufferedMutator edgesMutator;
    private BufferedMutator edgeIndicesMutator;
    private BufferedMutator verticesMutator;
    private BufferedMutator vertexIndicesMutator;

    public HBaseBulkLoader(HBaseGraphConfiguration config) throws IOException {
        this(new HBaseGraph(config, HBaseGraphUtils.getConnection(config)));
    }

    public HBaseBulkLoader(HBaseGraph graph) {
        try {
            this.graph = graph;

            BufferedMutator.ExceptionListener listener = (e, mutator) -> {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        LOGGER.warn("Failed to send put: " + e.getRow(i));
                    }
            };

            HBaseGraphConfiguration config = graph.configuration();

            BufferedMutatorParams edgesMutatorParams =
                    new BufferedMutatorParams(HBaseGraphUtils.getTableName(config, Constants.EDGES)).listener(listener);
            BufferedMutatorParams edgeIndicesMutatorParams =
                    new BufferedMutatorParams(HBaseGraphUtils.getTableName(config, Constants.EDGE_INDICES)).listener(listener);
            BufferedMutatorParams verticesMutatorParams =
                    new BufferedMutatorParams(HBaseGraphUtils.getTableName(config, Constants.VERTICES)).listener(listener);
            BufferedMutatorParams vertexIndicesMutatorParams =
                    new BufferedMutatorParams(HBaseGraphUtils.getTableName(config, Constants.VERTEX_INDICES)).listener(listener);

            edgesMutator = graph.connection().getBufferedMutator(edgesMutatorParams);
            edgeIndicesMutator = graph.connection().getBufferedMutator(edgeIndicesMutatorParams);
            verticesMutator = graph.connection().getBufferedMutator(verticesMutatorParams);
            vertexIndicesMutator = graph.connection().getBufferedMutator(vertexIndicesMutatorParams);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
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
                    HBaseGraphUtils.propertiesToMap(keyValues));

            Iterator<IndexMetadata> indices = vertex.getIndices(OperationType.WRITE);
            VertexIndexWriter writer = new VertexIndexWriter(graph, vertex, indices, null);
            vertexIndicesMutator.mutate(IteratorUtils.list(writer.constructInsertions()));

            Creator creator = new VertexWriter(graph, vertex);
            verticesMutator.mutate(IteratorUtils.list(creator.constructInsertions()));

            return vertex;
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

            Iterator<IndexMetadata> indices = edge.getIndices(OperationType.WRITE);
            EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, edge, indices, null);
            edgeIndicesMutator.mutate(IteratorUtils.list(indexWriter.constructInsertions()));

            EdgeIndexWriter writer = new EdgeIndexWriter(graph, edge, Constants.CREATED_AT, null);
            edgeIndicesMutator.mutate(IteratorUtils.list(writer.constructInsertions()));

            Creator creator = new EdgeWriter(graph, edge);
            edgesMutator.mutate(IteratorUtils.list(creator.constructInsertions()));

            return edge;
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
                    edgeIndicesMutator.mutate(IteratorUtils.list(indexRemover.constructMutations()));
                }
            }

            e.getProperties().put(key, value);
            e.updatedAt(System.currentTimeMillis());

            if (hasIndex) {
                if (oldValue == null || !oldValue.equals(value)) {
                    EdgeIndexWriter indexWriter = new EdgeIndexWriter(graph, e, key, null);
                    edgeIndicesMutator.mutate(IteratorUtils.list(indexWriter.constructInsertions()));
                }
            }
            PropertyWriter propertyWriter = new PropertyWriter(graph, e, key, value);
            edgesMutator.mutate(IteratorUtils.list(propertyWriter.constructMutations()));
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
                oldValue = v.getProperty(key);
                if (oldValue != null && !oldValue.equals(value)) {
                    VertexIndexRemover indexRemover = new VertexIndexRemover(graph, v, key, null);
                    vertexIndicesMutator.mutate(IteratorUtils.list(indexRemover.constructMutations()));
                }
            }

            v.getProperties().put(key, value);
            v.updatedAt(System.currentTimeMillis());

            if (hasIndex) {
                if (oldValue == null || !oldValue.equals(value)) {
                    VertexIndexWriter indexWriter = new VertexIndexWriter(graph, v, key);
                    vertexIndicesMutator.mutate(IteratorUtils.list(indexWriter.constructInsertions()));
                }
            }
            PropertyWriter propertyWriter = new PropertyWriter(graph, v, key, value);
            verticesMutator.mutate(IteratorUtils.list(propertyWriter.constructMutations()));
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public void close() {
        try {
            edgesMutator.close();
            edgeIndicesMutator.close();
            verticesMutator.close();
            vertexIndicesMutator.close();
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }
}
