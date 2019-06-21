package io.hgraphdb;

import io.hgraphdb.models.EdgeIndexModel;
import io.hgraphdb.models.EdgeModel;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class HBaseEdge extends HBaseElement implements Edge {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseEdge.class);

    private Vertex inVertex;
    private Vertex outVertex;
    protected Map<String, Object> properties;
    protected transient boolean propertiesFullyLoaded;

    public HBaseEdge(HBaseGraph graph, Object id) {
        this(graph, id, null, null, null, null, null, null);
    }

    public HBaseEdge(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null, null, null);
    }

    public HBaseEdge(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                     Vertex inVertex, Vertex outVertex) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null, inVertex, outVertex);
    }

    public HBaseEdge(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                     boolean propertiesFullyLoaded, Vertex inVertex, Vertex outVertex) {
        super(graph, id, label, createdAt, updatedAt);

        this.inVertex = inVertex;
        this.outVertex = outVertex;
        this.properties = properties;
        this.propertiesFullyLoaded = propertiesFullyLoaded;
    }

    @Override
    public void validate() {
        if (graph != null) {
            graph.validateEdge(label, id, properties, inVertex, outVertex);
        }
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }

    public Map<String, Object> getProperties() {
        if (properties == null || !propertiesFullyLoaded) {
            load();
            propertiesFullyLoaded = true;
        }
        return properties;
    }

    @Override
    public void copyFrom(HBaseElement element) {
        super.copyFrom(element);
        if (element instanceof HBaseEdge) {
            HBaseEdge copy = (HBaseEdge) element;
            if (copy.inVertex != null) this.inVertex = copy.inVertex;
            if (copy.outVertex != null) this.outVertex = copy.outVertex;
            if (copy.properties != null
                    && (copy.propertiesFullyLoaded || this.properties == null)) {
                this.properties = new ConcurrentHashMap<>(copy.properties);
                this.propertiesFullyLoaded = copy.propertiesFullyLoaded;
            }
        }
    }

    @Override
    public Vertex outVertex() {
        return getVertex(Direction.OUT);
    }

    protected void setOutVertex(HBaseVertex outVertex) {
        this.outVertex = outVertex;
    }

    @Override
    public Vertex inVertex() {
        return getVertex(Direction.IN);
    }

    protected void setInVertex(HBaseVertex inVertex) {
        this.inVertex = inVertex;
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

    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(key, value);

        graph.validateProperty(getElementType(), label, key, value);

        // delete from index model before setting property
        Object oldValue = null;
        boolean hasIndex = hasIndex(OperationType.WRITE, key);
        if (hasIndex) {
            // only load old value if using index
            oldValue = getProperty(key);
            if (oldValue != null && !oldValue.equals(value)) {
                deleteFromIndexModel(key, null);
            }
        }

        getProperties().put(key, value);
        updatedAt(System.currentTimeMillis());

        if (hasIndex) {
            if (oldValue == null || !oldValue.equals(value)) {
                writeToIndexModel(key);
            }
        }
        Mutator writer = getModel().writeProperty(this, key, value);
        Mutators.write(getTable(), writer);
    }

    public void incrementProperty(String key, long value) {
        if (!graph.configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        ElementHelper.validateProperty(key, value);

        graph.validateProperty(getElementType(), label, key, value);

        updatedAt(System.currentTimeMillis());

        Mutator writer = getModel().incrementProperty(this, key, value);
        long newValue = Mutators.increment(getTable(), writer, key);
        getProperties().put(key, newValue);
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    public void removeProperty(String key) {
        Object value = getProperty(key);
        if (value != null) {
            // delete from index model before removing property
            boolean hasIndex = hasIndex(OperationType.WRITE, key);
            if (hasIndex) {
                deleteFromIndexModel(key, null);
            }

            getProperties().remove(key);
            updatedAt(System.currentTimeMillis());

            Mutator writer = getModel().clearProperty(this, key);
            Mutators.write(getTable(), writer);
        }
    }

    @Override
    public Stream<Entry<String, Object>> propertyEntriesStream() {
        return properties.entrySet().stream();
    }

    @Override
    public int propertySize() {
        return this.properties.size();
    }

    @SuppressWarnings("unchecked")
    public <V> V getProperty(String key) {
        if (properties != null) {
            // optimization for partially loaded properties
            V val = (V) properties.get(key);
            if (val != null) return val;
        }
        return (V) getProperties().get(key);
    }

    @Override
	public boolean hasProperty(String key) {
        if (properties != null) {
            // optimization for partially loaded properties
            Object val = properties.get(key);
            if (val != null) return true;
        }
        return keys().contains(key);
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
        Iterable<String> keys = keys();
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
