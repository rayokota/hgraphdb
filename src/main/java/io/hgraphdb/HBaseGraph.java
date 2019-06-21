package io.hgraphdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.hgraphdb.HBaseGraphConfiguration.InstanceType;
import io.hgraphdb.IndexMetadata.State;
import io.hgraphdb.models.*;
import io.hgraphdb.process.strategy.optimization.HBaseGraphStepStrategy;
import io.hgraphdb.process.strategy.optimization.HBaseVertexStepStrategy;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("io.hgraphdb.StructureBasicSuite")
@Graph.OptIn("io.hgraphdb.CustomSuite")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals",
        method = "g_VX1AsStringX_out_hasXid_2AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals",
        method = "g_EX11X_outV_outE_hasXid_10AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals",
        method = "g_VX1AsStringX_outXknowsX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest$Traversals",
        method = "g_EX11AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Requires metaproperties")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Requires metaproperties")
public class HBaseGraph implements Graph {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(HBaseGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        HBaseVertexStepStrategy.instance(),
                        HBaseGraphStepStrategy.instance()
                ));
    }

    private final HBaseGraphConfiguration config;
    private final HBaseGraphFeatures features;
    private final Connection connection;
    private final EdgeModel edgeModel;
    private final EdgeIndexModel edgeIndexModel;
    private final VertexModel vertexModel;
    private final VertexIndexModel vertexIndexModel;
    private final IndexMetadataModel indexMetadataModel;
    private final LabelMetadataModel labelMetadataModel;
    private final LabelConnectionModel labelConnectionModel;
    private Cache<ByteBuffer, Edge> edgeCache;
    private Cache<ByteBuffer, Vertex> vertexCache;
    private Map<IndexMetadata.Key, IndexMetadata> indices = new ConcurrentHashMap<>();
    private Map<LabelMetadata.Key, LabelMetadata> labels = new ConcurrentHashMap<>();
    private Set<LabelConnection> labelConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public static HBaseGraph open(final Configuration properties) throws HBaseGraphException {
        return new HBaseGraph(properties);
    }

    public static HBaseGraph open(final String graphNamespace, final String zkHosts) throws HBaseGraphException {
        return new HBaseGraph(graphNamespace, zkHosts);
    }

    public static HBaseGraph open(final String graphNamespace, final String zkHosts, final String znodeParent) throws HBaseGraphException {
        return new HBaseGraph(graphNamespace, zkHosts, znodeParent);
    }

    public HBaseGraph(Configuration cfg) {
        this(new HBaseGraphConfiguration(cfg));
    }

    public HBaseGraph(String graphNamespace, String zkHosts) {
        this(new HBaseGraphConfiguration()
                .setInstanceType(InstanceType.DISTRIBUTED)
                .setGraphNamespace(graphNamespace)
                .setCreateTables(true)
                .set("hbase.zookeeper.quorum", zkHosts));
    }

    public HBaseGraph(String graphNamespace, String zkHosts, String znodeParent) {
        this(new HBaseGraphConfiguration()
                .setInstanceType(InstanceType.DISTRIBUTED)
                .setGraphNamespace(graphNamespace)
                .setCreateTables(true)
                .set("hbase.zookeeper.quorum", zkHosts)
                .set("zookeeper.znode.parent", znodeParent));
    }

    public HBaseGraph(HBaseGraphConfiguration config) {
        this(config, HBaseGraphUtils.getConnection(config));
    }

    public HBaseGraph(HBaseGraphConfiguration config, Connection connection) {
        try {
            this.config = config;
            this.connection = connection;
            this.features = new HBaseGraphFeatures(config.getInstanceType() != InstanceType.MOCK);

            if (config.getCreateTables()) {
                HBaseGraphUtils.createTables(config, connection);
            }

            this.edgeModel = new EdgeModel(this,
                    connection.getTable(HBaseGraphUtils.getTableName(config, Constants.EDGES)));
            this.vertexModel = new VertexModel(this,
                    connection.getTable(HBaseGraphUtils.getTableName(config, Constants.VERTICES)));
            this.edgeIndexModel = new EdgeIndexModel(this,
                    connection.getTable(HBaseGraphUtils.getTableName(config, Constants.EDGE_INDICES)));
            this.vertexIndexModel = new VertexIndexModel(this,
                    connection.getTable(HBaseGraphUtils.getTableName(config, Constants.VERTEX_INDICES)));
            this.indexMetadataModel = new IndexMetadataModel(this,
                    connection.getTable(HBaseGraphUtils.getTableName(config, Constants.INDEX_METADATA)));
            if (config.getUseSchema()) {
                this.labelMetadataModel = new LabelMetadataModel(this,
                        connection.getTable(HBaseGraphUtils.getTableName(config, Constants.LABEL_METADATA)));
                this.labelConnectionModel = new LabelConnectionModel(this,
                        connection.getTable(HBaseGraphUtils.getTableName(config, Constants.LABEL_CONNECTIONS)));
            } else {
                this.labelMetadataModel = null;
                this.labelConnectionModel = null;
            }

            this.edgeCache = CacheBuilder.<ByteBuffer, Edge>newBuilder()
                    .maximumSize(config.getElementCacheMaxSize())
                    .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                    .removalListener((RemovalListener<ByteBuffer, Edge>) notif -> ((HBaseEdge) notif.getValue()).setCached(false))
                    .build();
            this.vertexCache = CacheBuilder.<ByteBuffer, Vertex>newBuilder()
                    .maximumSize(config.getElementCacheMaxSize())
                    .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                    .removalListener((RemovalListener<ByteBuffer, Vertex>) notif -> ((HBaseVertex) notif.getValue()).setCached(false))
                    .build();

            refreshSchema();
            int schemaCacheRefreshSecs = config.getSchemaCacheRefreshSecs();
            if (schemaCacheRefreshSecs > 0) {
                executor.scheduleAtFixedRate(this::refreshSchema, schemaCacheRefreshSecs, schemaCacheRefreshSecs, TimeUnit.SECONDS);
            }
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    protected ScheduledExecutorService getExecutor() {
        return executor;
    }

    public EdgeModel getEdgeModel() {
        return edgeModel;
    }

    public EdgeIndexModel getEdgeIndexModel() {
        return edgeIndexModel;
    }

    public VertexModel getVertexModel() {
        return vertexModel;
    }

    public VertexIndexModel getVertexIndexModel() {
        return vertexIndexModel;
    }

    public IndexMetadataModel getIndexMetadataModel() {
        return indexMetadataModel;
    }

    public LabelMetadataModel getLabelMetadataModel() {
        return labelMetadataModel;
    }

    public LabelConnectionModel getLabelConnectionModel() {
        return labelConnectionModel;
    }

    public boolean isLazyLoading() {
        return configuration().isLazyLoading();
    }

    public boolean isParallelLoading() {
        return configuration().isParallelLoading();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        idValue = HBaseGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        HBaseVertex newVertex = new HBaseVertex(this, idValue, label, now, now, HBaseGraphUtils.propertiesToMultimap(keyValues));
        newVertex.validate();
        newVertex.writeToIndexModel();
        newVertex.writeToModel();

        Vertex vertex = findOrCreateVertex(idValue);
        ((HBaseVertex) vertex).copyFrom(newVertex);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds.length == 0) {
            return allVertices();
        } else {
            Stream<Object> stream = Stream.of(vertexIds);
            if (isParallelLoading() && vertexIds.length > 1) {
                stream = stream.parallel();
            }
            return stream
                    .map(id -> {
                        if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Vertex)
                            return ((Vertex) id).id();
                        else
                            return id;
                    })
                    .flatMap(id -> {
                        try {
                            return Stream.of(vertex(id));
                        } catch (final HBaseGraphNotFoundException e) {
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toList())
                    .iterator();
        }
    }

    public Vertex vertex(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Vertex v = findOrCreateVertex(id);
        ((HBaseVertex) v).load();
        return v;
    }

    public Vertex findOrCreateVertex(Object id) {
        return findVertex(id, true);
    }

    protected Vertex findVertex(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = HBaseGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Vertex cachedVertex = vertexCache.getIfPresent(key);
        if (cachedVertex != null && !((HBaseVertex) cachedVertex).isDeleted()) {
            return cachedVertex;
        }
        if (!createIfNotFound) return null;
        HBaseVertex vertex = new HBaseVertex(this, id);
        vertexCache.put(key, vertex);
        vertex.setCached(true);
        return vertex;
    }

    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    public Iterator<Vertex> allVertices() {
        return vertexModel.vertices();
    }

    public Iterator<Vertex> allVertices(Object fromId, int limit) {
        return vertexModel.vertices(fromId, limit);
    }

    public Iterator<Vertex> verticesByLabel(String label) {
        return vertexModel.vertices(label);
    }

    public Iterator<Vertex> verticesByLabel(String label, String key, Object value) {
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return vertexModel.verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit) {
        return verticesWithLimit(label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit, boolean reversed) {
        return vertexModel.verticesWithLimit(label, key, fromValue, limit, reversed);
    }

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        return outVertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds.length == 0) {
            return allEdges();
        } else {
            Stream<Object> stream = Stream.of(edgeIds);
            if (isParallelLoading() && edgeIds.length > 1) {
                stream = stream.parallel();
            }
            return stream
                    .map(id -> {
                        if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Edge)
                            return ((Edge) id).id();
                        else
                            return id;
                    })
                    .flatMap(id -> {
                        try {
                            return Stream.of(edge(id));
                        } catch (final HBaseGraphNotFoundException e) {
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toList())
                    .iterator();
        }
    }

    public Edge edge(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Edge edge = findOrCreateEdge(id);
        ((HBaseEdge) edge).load();
        return edge;
    }

    public Edge findOrCreateEdge(Object id) {
        return findEdge(id, true);
    }

    protected Edge findEdge(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = HBaseGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Edge cachedEdge = edgeCache.getIfPresent(key);
        if (cachedEdge != null && !((HBaseEdge) cachedEdge).isDeleted()) {
            return cachedEdge;
        }
        if (!createIfNotFound) {
            return null;
        }
        HBaseEdge edge = new HBaseEdge(this, id);
        edgeCache.put(key, edge);
        edge.setCached(true);
        return edge;
    }

    public void removeEdge(Edge edge) {
        edge.remove();
    }

    public Iterator<Edge> allEdges() {
        return edgeModel.edges();
    }

    public Iterator<Edge> allEdges(Object fromId, int limit) {
        return edgeModel.edges(fromId, limit);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }


    @Override
    public HBaseGraphConfiguration configuration() {
        return this.config;
    }

    public Connection connection() {
        return this.connection;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, HBaseGraphConfiguration.HBASE_GRAPH_CLASS.getSimpleName().toLowerCase());
    }

    @VisibleForTesting
    protected void refreshSchema() {
        Map<IndexMetadata.Key, IndexMetadata> newIndices = new ConcurrentHashMap<>();
        for (Iterator<IndexMetadata> it = getIndexMetadataModel().indices(); it.hasNext(); ) {
            final IndexMetadata index = it.next();
            newIndices.put(index.key(), index);
        }
        indices = newIndices;
        if (configuration().getUseSchema()) {
            Map<LabelMetadata.Key, LabelMetadata> newLabels = new ConcurrentHashMap<>();
            for (Iterator<LabelMetadata> it = getLabelMetadataModel().labels(); it.hasNext(); ) {
                final LabelMetadata label = it.next();
                newLabels.put(label.key(), label);
            }
            labels = newLabels;
            Set<LabelConnection> newLabelConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());
            for (Iterator<LabelConnection> it = getLabelConnectionModel().labelConnections(); it.hasNext(); ) {
                final LabelConnection labelConnection = it.next();
                newLabelConnections.add(labelConnection);
            }
            labelConnections = newLabelConnections;
        }
    }

    public void createIndex(ElementType type, String label, String propertyKey) {
        createIndex(type, label, propertyKey, false, false, false);
    }

    public void createIndex(ElementType type, String label, String propertyKey, boolean isUnique) {
        createIndex(type, label, propertyKey, isUnique, false, false);
    }

    public void createIndex(ElementType type, String label, String propertyKey, boolean isUnique, boolean populate, boolean async) {
        if (configuration().getUseSchema()) {
            getLabel(type, label);
            ValueType propertyType = validateProperty(type, label, propertyKey, null);
            if (propertyType == ValueType.COUNTER) {
                throw new HBaseGraphNotValidException("Index on COUNTER property '" + propertyKey + "' is not valid");
            }
        }
        IndexMetadata.Key indexKey = new IndexMetadata.Key(type, label, propertyKey);
        IndexMetadata oldIndex = indexMetadataModel.index(indexKey);
        if (oldIndex != null && oldIndex.state() != State.DROPPED) {
            throw new HBaseGraphNotUniqueException("Index for " + indexKey.toString() + " already exists");
        }
        long now = System.currentTimeMillis();
        IndexMetadata index = new IndexMetadata(type, label, propertyKey, isUnique, State.CREATED, now, now);
        if (oldIndex == null) {
            indexMetadataModel.createIndexMetadata(index);
        } else {
            indexMetadataModel.writeIndexMetadata(index);
        }
        indices.put(index.key(), index);
        if (populate) {
            if (async) {
                // PopulateIndex job must be run to activate
            } else {
                populateIndex(index);
            }
        } else {
            updateIndex(indexKey, State.ACTIVE);
        }
    }

    private void populateIndex(IndexMetadata index) {
        updateIndex(index.key(), State.BUILDING);

        try (HBaseBulkLoader bulkLoader = new HBaseBulkLoader(this)) {
            if (index.type() == ElementType.VERTEX) {
                allVertices().forEachRemaining(vertex -> bulkLoader.indexVertex(vertex, IteratorUtils.of(index)));
            } else {
                allEdges().forEachRemaining(edge -> bulkLoader.indexEdge(edge, IteratorUtils.of(index)));
            }
            updateIndex(index.key(), State.ACTIVE);
        }
    }

    public boolean hasIndex(OperationType op, ElementType type, String label, String propertyKey) {
        return getIndex(op, type, label, propertyKey) != null;
    }

    public IndexMetadata getIndex(OperationType op, ElementType type, String label, String propertyKey) {
        Iterator<IndexMetadata> indices = getIndices(op, type, label, propertyKey);
        return indices.hasNext() ? indices.next() : null;
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, ElementType type) {
        return indices.values().stream()
                .filter(index -> isIndexActive(op, index)
                        && index.type().equals(type)).iterator();
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, ElementType type, String label, String... propertyKeys) {
        return getIndices(op, type, label, Arrays.asList(propertyKeys));
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, ElementType type, String label, Collection<String> propertyKeys) {
        return indices.values().stream()
                .filter(index -> isIndexActive(op, index)
                        && index.type().equals(type)
                        && index.label().equals(label)
                        && propertyKeys.contains(index.propertyKey())).iterator();
    }

    private boolean isIndexActive(OperationType op, IndexMetadata index) {
        State state = index.state();
        switch (op) {
            case READ:
                return state == State.ACTIVE;
            case WRITE:
                return state == State.CREATED || state == State.BUILDING || state == State.ACTIVE;
            case REMOVE:
                return state != State.DROPPED;
        }
        return false;
    }

    public void updateIndex(IndexMetadata.Key indexKey, State newState) {
        IndexMetadata oldIndex = indexMetadataModel.index(indexKey);
        if (oldIndex == null) {
            throw new HBaseGraphNotValidException("Index for " + indexKey.toString() + " does not exist");
        }
        State oldState = oldIndex.state();

        boolean doTransition = true;
        switch (oldState) {
            case CREATED:
                break;
            case BUILDING:
                if (newState == State.CREATED) {
                    doTransition = false;
                }
                break;
            case ACTIVE:
                if (newState == State.CREATED) {
                    doTransition = false;
                }
                break;
            case INACTIVE:
                if (newState == State.CREATED || newState == State.BUILDING || newState == State.ACTIVE) {
                    doTransition = false;
                }
                break;
            case DROPPED:
                if (newState != State.CREATED) {
                    doTransition = false;
                }
                break;
        }
        if (!doTransition) {
            throw new HBaseGraphNotValidException("Invalid index state transition: " + oldState + " -> " + newState);
        }
        oldIndex.state(newState);
        oldIndex.updatedAt(System.currentTimeMillis());
        indexMetadataModel.writeIndexMetadata(oldIndex);
        indices.put(indexKey, oldIndex);
    }

    public void createLabel(ElementType type, String label, ValueType idType, Object... propertyKeysAndTypes) {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        long now = System.currentTimeMillis();
        LabelMetadata labelMetadata = new LabelMetadata(type, label, idType,
                now, now, HBaseGraphUtils.propertyKeysAndTypesToMap(propertyKeysAndTypes));
        labelMetadataModel.createLabelMetadata(labelMetadata);
        labels.put(labelMetadata.key(), labelMetadata);
    }

    public LabelMetadata getLabel(ElementType type, String label) {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        LabelMetadata labelMetadata = labels.get(new LabelMetadata.Key(type, label));
        if (labelMetadata == null) {
            throw new HBaseGraphNotValidException(type + " LABEL " + label + " does not exist");
        }
        return labelMetadata;
    }

    public Iterator<LabelMetadata> getLabels(ElementType type) {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        return labels.values().stream().filter(label -> label.type().equals(type)).iterator();
    }

    public Iterator<LabelConnection> getLabelConnections() {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        return labelConnections.iterator();
    }

    public void updateLabel(ElementType type, String label, Object... propertyKeysAndTypes) {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        refreshSchema();
        LabelMetadata labelMetadata = getLabel(type, label);
        Map<String, ValueType> propertyTypes = HBaseGraphUtils.propertyKeysAndTypesToMap(propertyKeysAndTypes);
        labelMetadataModel.addPropertyMetadata(labelMetadata, propertyTypes);
        labelMetadata.propertyTypes().putAll(propertyTypes);
    }

    public void connectLabels(String outVertexLabel, String edgeLabel, String inVertexLabel) {
        if (!configuration().getUseSchema()) {
            throw new HBaseGraphNoSchemaException("Schema not enabled");
        }
        refreshSchema();
        getLabel(ElementType.VERTEX, outVertexLabel);
        getLabel(ElementType.EDGE, edgeLabel);
        getLabel(ElementType.VERTEX, inVertexLabel);
        long now = System.currentTimeMillis();
        LabelConnection labelConnection = new LabelConnection(outVertexLabel, edgeLabel, inVertexLabel, now);
        labelConnectionModel.createLabelConnection(labelConnection);
        labelConnections.add(labelConnection);
    }

    public void validateEdge(String label, Object id, Map<String, Object> properties, Vertex inVertex, Vertex outVertex) {
        if (!configuration().getUseSchema() || label == null || inVertex == null || outVertex == null) return;
        LabelMetadata inVertexLabelMetadata = getLabel(ElementType.VERTEX, inVertex.label());
        LabelMetadata labelMetadata = getLabel(ElementType.EDGE, label);
        LabelMetadata outVertexLabelMetadata = getLabel(ElementType.VERTEX, outVertex.label());
        LabelConnection labelConnection = new LabelConnection(outVertex.label(), label, inVertex.label(), null);
        if (!labelConnections.contains(labelConnection)) {
            throw new HBaseGraphNotValidException("Edge label '" + label + "' has not been connected with inVertex '" + inVertex.label()
                    + "' and outVertex '" + outVertex.label() + "'");
        }
        validateTypes(labelMetadata, id, properties.entrySet().stream());
    }

    public void validateVertex(String label, Object id, Map<String, Collection<Object>> properties) {
        if (!configuration().getUseSchema() || label == null) return;
        LabelMetadata labelMetadata = getLabel(ElementType.VERTEX, label);
        validateTypes(labelMetadata, id, properties.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(o -> new AbstractMap.SimpleEntry<>(e.getKey(), o))));
    }

    private void validateTypes(LabelMetadata labelMetadata, Object id, Stream<Map.Entry<String, Object>> properties) {
        ValueType idType = labelMetadata.idType();
        if (idType != ValueType.ANY && idType != ValueUtils.getValueType(id)) {
            throw new HBaseGraphNotValidException("ID '" + id + "' not of type " + idType);
        }
        properties.forEach(entry ->
            getPropertyType(labelMetadata, entry.getKey(), entry.getValue(), true)
        );
    }

    public ValueType validateProperty(ElementType type, String label, String propertyKey, Object value) {
        if (!configuration().getUseSchema()) return null;
        return getPropertyType(getLabel(type, label), propertyKey, value, true);
    }

    public ValueType getPropertyType(ElementType type, String label, String propertyKey) {
        if (!configuration().getUseSchema()) return null;
        return getPropertyType(getLabel(type, label), propertyKey, null, false);
    }

    private ValueType getPropertyType(LabelMetadata labelMetadata, String propertyKey, Object value, boolean doValidate) {
        Map<String, ValueType> propertyTypes = labelMetadata.propertyTypes();
        if (!Graph.Hidden.isHidden(propertyKey)) {
            ValueType propertyType = propertyTypes.get(propertyKey);
            if (doValidate) {
                if (propertyType == null) {
                    throw new HBaseGraphNotValidException("Property '" + propertyKey + "' has not been defined");
                }
                ValueType valueType = ValueUtils.getValueType(value);
                if (value != null && propertyType != ValueType.ANY
                        && (!(propertyType == ValueType.COUNTER && valueType == ValueType.LONG))
                        && (propertyType != valueType)) {
                    throw new HBaseGraphNotValidException("Property '" + propertyKey + "' not of type " + propertyType);
                }
            }
            return propertyType;
        }
        return null;
    }

    @Override
    public void close() {
        close(false);
    }

    @VisibleForTesting
    protected void close(boolean clear) {
        executor.shutdown();
        this.edgeModel.close(clear);
        this.edgeIndexModel.close(clear);
        this.vertexModel.close(clear);
        this.vertexIndexModel.close(clear);
        this.indexMetadataModel.close(clear);
        if (this.labelMetadataModel != null) {
            this.labelMetadataModel.close(clear);
        }
        if (this.labelConnectionModel != null) {
            this.labelConnectionModel.close(clear);
        }
    }

    @VisibleForTesting
    public void drop() {
        HBaseGraphUtils.dropTables(config, connection);
    }

    public void dump() {
        System.out.println("Vertices:");
        vertices().forEachRemaining(System.out::println);
        System.out.println("Edges:");
        edges().forEachRemaining(System.out::println);
    }
}
