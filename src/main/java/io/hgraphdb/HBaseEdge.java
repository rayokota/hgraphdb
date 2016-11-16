package io.hgraphdb;

import io.hgraphdb.models.EdgeIndexModel;
import io.hgraphdb.models.EdgeModel;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class HBaseEdge extends HBaseElement implements Edge {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseEdge.class);

    private Vertex inVertex;
    private Vertex outVertex;

    public HBaseEdge(HBaseGraph graph, Object id) {
        this(graph, id, null, null, null, null, null, null);
    }

    public HBaseEdge(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                     Vertex inVertex, Vertex outVertex) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null, inVertex, outVertex);
    }

    public HBaseEdge(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                     boolean propertiesFullyLoaded, Vertex inVertex, Vertex outVertex) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);

        graph.validateEdge(label, id, properties, inVertex, outVertex);

        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    @Override
    public void copyFrom(HBaseElement element) {
        super.copyFrom(element);
        if (element instanceof HBaseEdge) {
            HBaseEdge copy = (HBaseEdge) element;
            if (copy.inVertex != null) this.inVertex = copy.inVertex;
            if (copy.outVertex != null) this.outVertex = copy.outVertex;
        }
    }

    @Override
    public Vertex outVertex() {
        return getVertex(Direction.OUT);
    }

    @Override
    public Vertex inVertex() {
        return getVertex(Direction.IN);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return direction == Direction.BOTH
                ? IteratorUtils.of(getVertex(Direction.OUT), getVertex(Direction.IN))
                : IteratorUtils.of(getVertex(direction));
    }

    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (!Direction.IN.equals(direction) && !Direction.OUT.equals(direction)) {
            throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        if (inVertex == null || outVertex == null) load();

        return Direction.IN.equals(direction) ? inVertex : outVertex;
    }

    @Override
    public void remove() {
        // Get rid of the endpoints and edge themselves.
        deleteFromModel();
        deleteEdgeEndpoints();

        setDeleted(true);
        if (!isCached()) {
            HBaseEdge cachedEdge = (HBaseEdge) graph.findEdge(id, false);
            if (cachedEdge != null) cachedEdge.setDeleted(true);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterable<String> keys = getPropertyKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new HBaseProperty<>(graph, this, key, getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        V value = getProperty(key);
        return value != null ? new HBaseProperty<>(graph, this, key, value) : Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        setProperty(key, value);
        return new HBaseProperty<>(graph, this, key, value);
    }

    @Override
    public boolean hasIndex(OperationType op, String propertyKey) {
        return graph.hasIndex(op, IndexType.EDGE, label, propertyKey);
    }

    @Override
    public Iterator<IndexMetadata> getIndices(OperationType op) {
        return graph.getIndices(op, IndexType.EDGE, label, getPropertyKeys());
    }

    @Override
    public EdgeModel getModel() {
        return graph.getEdgeModel();
    }

    @Override
    public EdgeIndexModel getIndexModel() {
        return graph.getEdgeIndexModel();
    }

    @Override
    public void writeToModel() {
        getModel().writeEdge(this);
    }

    @Override
    public void deleteFromModel() {
        getModel().deleteEdge(this);
    }

    public void writeEdgeEndpoints() {
        getIndexModel().writeEdgeEndpoints(this);
    }

    public void deleteEdgeEndpoints() {
        getIndexModel().deleteEdgeEndpoints(this, null);
    }

    public void deleteEdgeEndpoints(Long ts) {
        getIndexModel().deleteEdgeEndpoints(this, ts);
    }

    @Override
    public void writeToIndexModel(String key) {
        getIndexModel().writeEdgeIndex(this, key);
    }

    @Override
    public void deleteFromIndexModel(String key, Long ts) {
        getIndexModel().deleteEdgeIndex(this, key, ts);
    }

    @Override
    public void removeStaleIndices() {
        deleteEdgeEndpoints(getIndexTs());
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
