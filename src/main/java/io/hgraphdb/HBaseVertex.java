package io.hgraphdb;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.hgraphdb.models.VertexModel;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Tuple;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HBaseVertex extends HBaseElement implements Vertex {

    private Cache<Tuple, List<Edge>> edgeCache;
    private Cache<Tuple, List<Vertex>> vertexCache;

    public HBaseVertex(HBaseGraph graph, Object id) {
        this(graph, id, null, null, null, null, false);
    }

    public HBaseVertex(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public HBaseVertex(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                       Map<String, Object> properties, boolean propertiesFullyLoaded) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);

        this.edgeCache = CacheBuilder.<Tuple, List<Edge>>newBuilder()
                .maximumSize(graph.configuration().getRelationshipCacheMaxSize())
                .expireAfterAccess(graph.configuration().getRelationshipCacheTtlSecs(), TimeUnit.SECONDS)
                .build();
        this.vertexCache = CacheBuilder.<Tuple, List<Vertex>>newBuilder()
                .maximumSize(graph.configuration().getRelationshipCacheMaxSize())
                .expireAfterAccess(graph.configuration().getRelationshipCacheTtlSecs(), TimeUnit.SECONDS)
                .build();
    }

    @Override
    public VertexModel getModel() {
        return graph.getVertexModel();
    }

    public Iterator<Edge> getEdgesFromCache(Tuple cacheKey) {
        if (!isCached()) return null;
        List<Edge> edges = edgeCache.getIfPresent(cacheKey);
        return edges != null ? IteratorUtils.filter(edges.iterator(), edge -> !((HBaseEdge) edge).isDeleted()) : null;
    }

    public Iterator<Vertex> getVerticesFromCache(Tuple cacheKey) {
        if (!isCached()) return null;
        List<Vertex> vertices = vertexCache.getIfPresent(cacheKey);
        return vertices != null ? IteratorUtils.filter(vertices.iterator(), vertex -> !((HBaseVertex) vertex).isDeleted()) : null;
    }

    public void cacheEdges(Tuple cacheKey, List<Edge> edges) {
        if (!isCached()) return;
        edgeCache.put(cacheKey, edges);
    }

    public void cacheVertices(Tuple cacheKey, List<Vertex> vertices) {
        if (!isCached()) return;
        vertexCache.put(cacheKey, vertices);
    }

    protected void invalidateCaches() {
        edgeCache.invalidateAll();
        vertexCache.invalidateAll();
    }

    @Override
    public void load() {
        super.load();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        idValue = HBaseGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        HBaseEdge newEdge = new HBaseEdge(graph, idValue, label, now, now, HBaseGraphUtils.propertiesToMap(keyValues), inVertex, this);
        graph.getEdgeModel().writeEdge(newEdge);
        graph.getEdgeIndexModel().writeEdgeEndpoints(newEdge);

        invalidateCaches();
        if (!isCached()) {
            HBaseVertex cachedVertex = (HBaseVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.invalidateCaches();
        }
        ((HBaseVertex) inVertex).invalidateCaches();
        if (!((HBaseVertex) inVertex).isCached()) {
            HBaseVertex cachedInVertex = (HBaseVertex) graph.findVertex(inVertex.id(), false);
            if (cachedInVertex != null) cachedInVertex.invalidateCaches();
        }

        Edge edge = graph.findOrCreateEdge(idValue);
        ((HBaseEdge) edge).copyFrom(newEdge);
        return edge;
    }

    @Override
    public void remove() {
        // Remove edges incident to this vertex.
        edges(Direction.BOTH, OperationType.REMOVE).forEachRemaining(Edge::remove);

        // Get rid of the vertex.
        getModel().deleteVertex(this);

        setDeleted(true);
        if (!isCached()) {
            HBaseVertex cachedVertex = (HBaseVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.setDeleted(true);
        }
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        setProperty(key, value);
        return new HBaseVertexProperty<>(graph, this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        V value = getProperty(key);
        return value != null ? new HBaseVertexProperty<>(graph, this, key, value) : VertexProperty.empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Iterable<String> keys = getPropertyKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new HBaseVertexProperty<>(graph, this, key, getProperty(key)));
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeIndexModel().edges(this, direction, OperationType.READ, edgeLabels);
    }

    public Iterator<Edge> edges(final Direction direction, final OperationType op, final String... edgeLabels) {
        return graph.getEdgeIndexModel().edges(this, direction, op, edgeLabels);
    }

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeIndexModel().edges(this, direction, label, key, value);
    }

    public Iterator<Edge> edges(final Direction direction, final String label, final String key,
                                final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeIndexModel().edges(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeIndexModel().vertices(this, direction, edgeLabels);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
