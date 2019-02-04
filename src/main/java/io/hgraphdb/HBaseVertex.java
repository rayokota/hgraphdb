package io.hgraphdb;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;

import io.hgraphdb.models.VertexIndexModel;
import io.hgraphdb.models.VertexModel;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class HBaseVertex extends HBaseElement implements Vertex {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseVertex.class);

    private transient Cache<Tuple, List<Edge>> edgeCache;
    protected Map<String, Collection<Object>> properties;
    protected transient boolean propertiesFullyLoaded;

    public HBaseVertex(HBaseGraph graph, Object id) {
        this(graph, id, null, null, null, null, false);
    }

    public HBaseVertex(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Collection<Object>> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public HBaseVertex(HBaseGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                       Map<String, Collection<Object>> properties, boolean propertiesFullyLoaded) {
        super(graph, id, label, createdAt, updatedAt);

        if (graph != null) {
            this.edgeCache = CacheBuilder.newBuilder()
                    .maximumSize(graph.configuration().getRelationshipCacheMaxSize())
                    .expireAfterAccess(graph.configuration().getRelationshipCacheTtlSecs(), TimeUnit.SECONDS)
                    .build();
        }
        this.properties = properties;
        this.propertiesFullyLoaded = propertiesFullyLoaded;
    }

    @Override
    public void copyFrom(HBaseElement element) {
        super.copyFrom(element);
        if (element instanceof HBaseVertex) {
            HBaseVertex copy = (HBaseVertex) element;
            if (copy.properties != null && (copy.propertiesFullyLoaded || this.properties == null)) {
                this.properties = new ConcurrentHashMap<>(copy.properties);
                this.propertiesFullyLoaded = copy.propertiesFullyLoaded;
            }
        }
    }

    @Override
    public void validate() {
        if (graph != null) {
            graph.validateVertex(label, id, properties);
        }
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

    public Iterator<Edge> getEdgesFromCache(Tuple cacheKey) {
        if (edgeCache == null || !isCached()) return null;
        List<Edge> edges = edgeCache.getIfPresent(cacheKey);
        return edges != null ? IteratorUtils.filter(edges.iterator(), edge -> !((HBaseEdge) edge).isDeleted()) : null;
    }

    public void cacheEdges(Tuple cacheKey, List<Edge> edges) {
        if (edgeCache == null || !isCached()) return;
        edgeCache.put(cacheKey, edges);
    }

    protected void invalidateEdgeCache() {
        if (edgeCache != null) edgeCache.invalidateAll();
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
        newEdge.validate();
        newEdge.writeEdgeEndpoints();
        newEdge.writeToModel();

        invalidateEdgeCache();
        if (!isCached()) {
            HBaseVertex cachedVertex = (HBaseVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.invalidateEdgeCache();
        }
        ((HBaseVertex) inVertex).invalidateEdgeCache();
        if (!((HBaseVertex) inVertex).isCached()) {
            HBaseVertex cachedInVertex = (HBaseVertex) graph.findVertex(inVertex.id(), false);
            if (cachedInVertex != null) cachedInVertex.invalidateEdgeCache();
        }

        Edge edge = graph.findOrCreateEdge(idValue);
        ((HBaseEdge) edge).copyFrom(newEdge);
        return edge;
    }

    @Override
    public void remove() {
        // Remove edges incident to this vertex.
        edges(Direction.BOTH).forEachRemaining(edge -> {
            try {
                edge.remove();
            } catch (HBaseGraphNotFoundException e) {
                // ignore
            }
        });

        // Get rid of the vertex.
        deleteFromModel();
        deleteFromIndexModel();

        setDeleted(true);
        if (!isCached()) {
            HBaseVertex cachedVertex = (HBaseVertex) graph.findVertex(id, false);
            if (cachedVertex != null) cachedVertex.setDeleted(true);
        }
    }

    private Map<String, Collection<Object>> getProperties() {
        if (this.properties == null) {
            if (this.propertiesFullyLoaded) {
                //read-optimized concurrent map
                this.properties = new MapMaker().concurrencyLevel(1).makeMap();
            } else {
                if (properties == null || !propertiesFullyLoaded) {
                    load();
                    propertiesFullyLoaded = true;
                }
            }
        }
        return properties;
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();

        Object oldSingleValue = null;
        if (cardinality == VertexProperty.Cardinality.single) {
            Collection<Object> values = getProperties().get(key);
            if (values != null && !values.isEmpty()) {
                oldSingleValue = values.iterator().next();
            }
        }
        boolean hasIndex = hasIndex(OperationType.WRITE, key);
        if (hasIndex && cardinality != VertexProperty.Cardinality.single) {
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        }

        //this will mutate the graph
        VertexProperty<V> existingProperty = null;
        if (cardinality.equals(VertexProperty.Cardinality.single)) {
           this.properties.remove(key);
        } else if (cardinality.equals(VertexProperty.Cardinality.set)) {
           Iterator<VertexProperty<V>> itty = this.properties(key);
           while (itty.hasNext()) {
               final VertexProperty<V> property = itty.next();
               if (property.value().equals(value)) {
                   ElementHelper.attachProperties(property, keyValues);
                   existingProperty = property;
                   break;
               }
           }
        }
        if (existingProperty != null) {
            if (existingProperty instanceof HBaseVertexProperty) {
               return existingProperty;
            } else {
               return new HBaseVertexProperty<>(graph, this, existingProperty.key(), existingProperty.value());
            }
        }

        if (hasIndex) {
             // only load old value if using index
            if (oldSingleValue != null && !oldSingleValue.equals(value)) {
                 deleteFromIndexModel(key, null);
            }
        }
        Collection<Object> plist = getProperties().computeIfAbsent(key, k -> new LinkedList<>());
        plist.add(value);
        updatedAt(System.currentTimeMillis());

        if (hasIndex) {
            if (oldSingleValue == null || !oldSingleValue.equals(value)) {
                writeToIndexModel(key);
            }
        }
        //we only really want to write multi-properties when necessary
        Mutator writer = getModel().writeProperty(this, key, preparePropertyWriteValue(cardinality, value, plist));
        Mutators.write(getTable(), writer);
        return new HBaseVertexProperty<>(graph, this, key, value);
    }

   private <V> Object preparePropertyWriteValue(final VertexProperty.Cardinality cardinality, final V value, Collection<Object> plist) {
       return cardinality == VertexProperty.Cardinality.single ? value : new PropertyValueList(plist);
   }

    @SuppressWarnings("unchecked")
    public <V> Optional<V> getSingleProperty(String key) {
        if (this.properties != null && this.properties.containsKey(key)) {
            // optimization for partially loaded properties
            Collection<Object> values = this.properties.getOrDefault(key, Collections.emptyList());
            if (values.size() == 1) {
                return Optional.ofNullable((V)values.iterator().next());
            }
        }
        Collection<Object> values = getProperties().getOrDefault(key, Collections.emptyList());
        if (values.size() == 1) {
            return Optional.ofNullable((V)values.iterator().next());
        }
        return Optional.empty();
    }
    public boolean hasProperty(String key, Object value) {
        if (this.properties != null && this.properties.containsKey(key)) {
            // optimization for partially loaded properties
            if (this.properties.getOrDefault(key, Collections.emptyList()).contains(value)) {
                return true;
            }
        }
        if (getProperties().getOrDefault(key, Collections.emptyList()).contains(value)) {
            return true;
        }
        return false;
    }
    public void cacheProperty(String key, Object value) {
        getProperties().computeIfAbsent(key, k -> new LinkedHashSet<>()).add(value);
    }

    @Override
    public boolean hasProperty(String key) {
        if (this.properties != null && !properties.getOrDefault(key, Collections.emptyList()).isEmpty()) {
            // optimization for partially loaded properties
            return true;
        }
        return !getProperties().getOrDefault(key, Collections.emptyList()).isEmpty();
    }

    public boolean removeProperty(String key, Object value) {
        boolean removed = false;
        if (null != this.properties && this.properties.containsKey(key)) {
           Collection<Object> values = this.properties.get(key);
           removed = values.remove(value);
           if (removed) {
               updatedAt(System.currentTimeMillis());
               Mutator writer;
               if (values.isEmpty()) {
                  boolean hasIndex = hasIndex(OperationType.WRITE, key);
                  deleteFromIndexModel(key, null);
                   //clear
                  writer = getModel().clearProperty(this, key);
               } else {
                  //cardinality is non-single
                  writer = getModel().writeProperty(this, key, preparePropertyWriteValue(VertexProperty.Cardinality.list, value, values));
               }
               Mutators.write(getTable(), writer);
           }
        }
        return removed;
    }

    @Override
    public Stream<Entry<String, Object>> propertyEntriesStream() {
        return this.getProperties().entrySet().stream()
           .flatMap(e -> e.getValue().stream().map(o -> new AbstractMap.SimpleEntry<>(e.getKey(), o)));
    }

    @Override
    public int propertySize() {
        return this.getProperties().values().stream().filter(c -> c != null).mapToInt(c -> c.size()).sum();
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
        cacheProperty(key, newValue);
    }

    @Override @SuppressWarnings("unchecked")
    public <V> VertexProperty<V> property(final String key) {
        if (getProperties().containsKey(key)) {
            Collection<Object> values = getProperties().getOrDefault(key, Collections.emptyList());
            if (values.size() > 1) {
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            } else {
                return new HBaseVertexProperty<>(graph, this, key, (V)values.iterator().next());
            }
        } else {
            return VertexProperty.<V>empty();
        }
    }

    @Override @SuppressWarnings("unchecked")
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Set<String> desiredKeys = ImmutableSet.copyOf(propertyKeys);
        return getProperties().entrySet().stream()
           .filter(e -> propertyKeys.length == 0 || desiredKeys.contains(e.getKey()))
           .flatMap(e -> e.getValue().stream().map(o -> new AbstractMap.SimpleEntry<>(e.getKey(), o)))
           .map(e -> (VertexProperty<V>)new HBaseVertexProperty<>(graph, this, e.getKey(), (V)e.getValue()))
           .iterator();
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeIndexModel().edges(this, direction, edgeLabels);
    }

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeIndexModel().edges(this, direction, label, key, value);
    }

    public Iterator<Edge> edgesInRange(final Direction direction, final String label, final String key,
                                       final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeIndexModel().edgesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit) {
        return edgesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit, final boolean reversed) {
        return graph.getEdgeIndexModel().edgesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return graph.getEdgeIndexModel().vertices(this, direction, edgeLabels);
    }

    public Iterator<Vertex> vertices(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeIndexModel().vertices(this, direction, label, key, value);
    }

    public Iterator<Vertex> verticesInRange(final Direction direction, final String label, final String key,
                                            final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeIndexModel().verticesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit) {
        return verticesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit, final boolean reversed) {
        return graph.getEdgeIndexModel().verticesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }

    @Override
    public VertexModel getModel() {
        return graph.getVertexModel();
    }

    @Override
    public VertexIndexModel getIndexModel() {
        return graph.getVertexIndexModel();
    }

    @Override
    public void writeToModel() {
        getModel().writeVertex(this);
    }

    @Override
    public void deleteFromModel() {
        getModel().deleteVertex(this);
    }

    public void writeToIndexModel() {
        getIndexModel().writeVertexIndex(this);
    }

    public void deleteFromIndexModel() {
        getIndexModel().deleteVertexIndex(this, null);
    }

    public void deleteFromIndexModel(Long ts) {
        getIndexModel().deleteVertexIndex(this, ts);
    }

    @Override
    public void writeToIndexModel(String key) {
        getIndexModel().writeVertexIndex(this, key);
    }

    @Override
    public void deleteFromIndexModel(String key, Long ts) {
        getIndexModel().deleteVertexIndex(this, key, ts);
    }

    @Override
    public void removeStaleIndices() {
        deleteFromIndexModel(getIndexTs());
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
