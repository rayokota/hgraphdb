package io.hgraphdb;

import io.hgraphdb.mutators.Creator;
import io.hgraphdb.mutators.EdgeIndexWriter;
import io.hgraphdb.mutators.EdgeWriter;
import io.hgraphdb.mutators.VertexIndexWriter;
import io.hgraphdb.mutators.VertexWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
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

    public HBaseBulkLoader(HBaseGraph graph) {
        try {
            this.graph = graph;

            BufferedMutator.ExceptionListener listener = (e, mutator) -> {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        LOGGER.warn("Failed to send put: " + e.getRow(i));
                    }
            };

            String ns = graph.configuration().getGraphNamespace();

            BufferedMutatorParams edgesMutatorParams =
                    new BufferedMutatorParams(TableName.valueOf(ns, Constants.EDGES)).listener(listener);
            BufferedMutatorParams edgeIndicesMutatorParams =
                    new BufferedMutatorParams(TableName.valueOf(ns, Constants.EDGE_INDICES)).listener(listener);
            BufferedMutatorParams verticesMutatorParams =
                    new BufferedMutatorParams(TableName.valueOf(ns, Constants.VERTICES)).listener(listener);
            BufferedMutatorParams vertexIndicesMutatorParams =
                    new BufferedMutatorParams(TableName.valueOf(ns, Constants.VERTEX_INDICES)).listener(listener);

            edgesMutator = graph.connection().getBufferedMutator(edgesMutatorParams);
            edgeIndicesMutator = graph.connection().getBufferedMutator(edgeIndicesMutatorParams);
            verticesMutator = graph.connection().getBufferedMutator(verticesMutatorParams);
            vertexIndicesMutator = graph.connection().getBufferedMutator(vertexIndicesMutatorParams);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
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
