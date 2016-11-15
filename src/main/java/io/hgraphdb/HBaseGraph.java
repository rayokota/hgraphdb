package io.hgraphdb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.hgraphdb.HBaseGraphConfiguration.InstanceType;
import io.hgraphdb.IndexMetadata.Key;
import io.hgraphdb.IndexMetadata.State;
import io.hgraphdb.models.EdgeIndexModel;
import io.hgraphdb.models.EdgeModel;
import io.hgraphdb.models.IndexMetadataModel;
import io.hgraphdb.models.VertexIndexModel;
import io.hgraphdb.models.VertexModel;
import io.hgraphdb.process.strategy.optimization.HBaseGraphStepStrategy;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
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
        method = "g_VX1X_outXknowsAsStringIdX",
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyHasTest$Traversals",
        method = "g_VX1AsStringX_out_hasXid_2AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyHasTest$Traversals",
        method = "g_EX11X_outV_outE_hasXid_10AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyVertexTest$Traversals",
        method = "g_VX1X_outXknowsAsStringIdX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyVertexTest$Traversals",
        method = "g_EX11AsStringX",
        reason = "Attempts to retrieve an element that has a numeric id with a String")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Requires metaproperties")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySubgraphTest$Traversals",
        method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Requires metaproperties")
public class HBaseGraph implements Graph {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(HBaseGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(HBaseGraphStepStrategy.instance()));
    }

    private final HBaseGraphConfiguration config;
    private final HBaseGraphFeatures features;
    private final Connection connection;
    private final EdgeModel edgeModel;
    private final EdgeIndexModel edgeIndexModel;
    private final VertexModel vertexModel;
    private final VertexIndexModel vertexIndexModel;
    private final IndexMetadataModel indexMetadataModel;
    private Cache<ByteBuffer, Edge> edgeCache;
    private Cache<ByteBuffer, Vertex> vertexCache;
    private Map<Key, IndexMetadata> indices = new ConcurrentHashMap<>();
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
            this.features = new HBaseGraphFeatures(config.getInstanceType() == InstanceType.DISTRIBUTED);

            if (config.getCreateTables()) {
                HBaseGraphUtils.createTables(config, connection);
            }

            String ns = config.getGraphNamespace();
            this.edgeModel = new EdgeModel(this, connection.getTable(TableName.valueOf(ns, Constants.EDGES)));
            this.edgeIndexModel = new EdgeIndexModel(this, connection.getTable(TableName.valueOf(ns, Constants.EDGE_INDICES)));
            this.vertexModel = new VertexModel(this, connection.getTable(TableName.valueOf(ns, Constants.VERTICES)));
            this.vertexIndexModel = new VertexIndexModel(this, connection.getTable(TableName.valueOf(ns, Constants.VERTEX_INDICES)));
            this.indexMetadataModel = new IndexMetadataModel(this, connection.getTable(TableName.valueOf(ns, Constants.INDEX_METADATA)));

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

            refreshIndices();
            executor.scheduleAtFixedRate(this::refreshIndices,
                    config.getIndexCacheRefreshSecs(), config.getIndexCacheRefreshSecs(), TimeUnit.SECONDS);
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

    public EdgeIndexModel getEdgeIndexModel() { return edgeIndexModel; }

    public VertexModel getVertexModel() {
        return vertexModel;
    }

    public VertexIndexModel getVertexIndexModel() {
        return vertexIndexModel;
    }

    public IndexMetadataModel getIndexMetadataModel() {
        return indexMetadataModel;
    }

    public boolean isLazyLoading() {
        return config.isLazyLoading();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        idValue = HBaseGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        HBaseVertex newVertex = new HBaseVertex(this, idValue, label, now, now, HBaseGraphUtils.propertiesToMap(keyValues));
        newVertex.writeToIndexModel(newVertex.getIndexTs());
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
            return Stream.of(vertexIds)
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

    public Vertex findVertex(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        ByteBuffer key = ByteBuffer.wrap(Serializer.serialize(id));
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

    public Iterator<Vertex> allVertices(String label) {
        return vertexModel.vertices(label);
    }

    public Iterator<Vertex> allVertices(String key, Object value) {
        return vertexModel.vertices(key, value);
    }

    public Iterator<Vertex> allVertices(String label, String key, Object value) {
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> allVertices(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return vertexModel.vertices(label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        return outVertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds.length == 0) {
            return allEdges();
        } else {
            return Stream.of(edgeIds)
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

    public Edge findEdge(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        ByteBuffer key = ByteBuffer.wrap(Serializer.serialize(id));
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

    public Iterator<Edge> allEdges(String key, Object value) {
        return edgeModel.edges(key, value);
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

    protected Connection connection() {
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
    protected void refreshIndices() {
        Map<Key, IndexMetadata> map = new ConcurrentHashMap<>();
        for (Iterator<IndexMetadata> it = getIndexMetadataModel().indices(); it.hasNext(); ) {
            final IndexMetadata index = it.next();
            map.put(index.key(), index);
        }
        indices = map;
    }

    public void createIndex(IndexType type, String label, String propertyKey) {
        createIndex(type, label, propertyKey, false, false);
    }

    public void createIndex(IndexType type, String label, String propertyKey, boolean isUnique) {
        createIndex(type, label, propertyKey, isUnique, false);
    }

    public void createIndex(IndexType type, String label, String propertyKey, boolean isUnique, boolean populate) {
        IndexMetadata.Key indexKey = new IndexMetadata.Key(type, label, propertyKey);
        IndexMetadata oldIndex = indexMetadataModel.index(indexKey);
        if (oldIndex != null && oldIndex.state() != State.DROPPED) {
            throw new HBaseGraphException("Index for " + indexKey.toString() + " already exists");
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
            populateIndex(index);
        } else {
            updateIndex(indexKey, State.ACTIVE);
        }
    }

    private void populateIndex(IndexMetadata index) {
        updateIndex(index.key(), State.BUILDING);

        executor.schedule(
                () -> {
                    if (index.type() == IndexType.VERTEX) {
                        allVertices().forEachRemaining(vertex -> vertexIndexModel.writeVertexIndex(vertex, index));
                    } else {
                        allEdges().forEachRemaining(edge -> edgeIndexModel.writeEdgeIndex(edge, index));
                    }
                    updateIndex(index.key(), State.ACTIVE);
                },
                config.getIndexStateChangeDelaySecs(), TimeUnit.SECONDS);
    }

    public boolean hasIndex(OperationType op, IndexType type, String label, String propertyKey ) {
        return getIndex(op, type, label, propertyKey) != null;
    }

    public IndexMetadata getIndex(OperationType op, IndexType type, String label, String propertyKey) {
        Iterator<IndexMetadata> indices = getIndices(op, type, label, propertyKey);
        return indices.hasNext() ? indices.next() : null;
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, IndexType type, String label, String... propertyKeys) {
        return getIndices(op, type, label, Arrays.asList(propertyKeys));
    }

    public Iterator<IndexMetadata> getIndices(OperationType op, IndexType type, String label, Collection<String> propertyKeys) {
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
        }
        return false;
    }

    private void updateIndex(IndexMetadata.Key indexKey, State newState) {
        IndexMetadata oldIndex = indexMetadataModel.index(indexKey);
        if (oldIndex == null) {
            throw new HBaseGraphException("Index for " + indexKey.toString() + " does not exists");
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
            throw new HBaseGraphException("Invalid index state transition: " + oldState + " -> " + newState);
        }
        oldIndex.state(newState);
        oldIndex.updatedAt(System.currentTimeMillis());
        indexMetadataModel.writeIndexMetadata(oldIndex);
        indices.put(indexKey, oldIndex);
    }

    @Override
    public void close() {
        close(false);
    }

    @VisibleForTesting
    protected void close(boolean clear) {
        this.edgeModel.close(clear);
        this.edgeIndexModel.close(clear);
        this.vertexModel.close(clear);
        this.vertexIndexModel.close(clear);
        this.indexMetadataModel.close(clear);
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
