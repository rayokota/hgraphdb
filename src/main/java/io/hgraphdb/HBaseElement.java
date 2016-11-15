package io.hgraphdb;

import io.hgraphdb.models.BaseModel;
import io.hgraphdb.models.ElementModel;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import org.apache.hadoop.hbase.client.Table;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HBaseElement implements Element {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseElement.class);

    protected final HBaseGraph graph;
    protected final Object id;
    protected String label;
    protected Long createdAt;
    protected Long updatedAt;
    protected Map<String, Object> properties;
    protected boolean propertiesFullyLoaded;
    protected IndexMetadata.Key indexKey;
    protected long indexTs;
    protected boolean isCached;
    protected boolean isDeleted;

    protected HBaseElement(HBaseGraph graph,
                           Object id,
                           String label,
                           Long createdAt,
                           Long updatedAt,
                           Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    protected HBaseElement(HBaseGraph graph,
                           Object id,
                           String label,
                           Long createdAt,
                           Long updatedAt,
                           Map<String, Object> properties,
                           boolean propertiesFullyLoaded) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.properties = properties;
        this.propertiesFullyLoaded = propertiesFullyLoaded;
        this.indexTs = System.currentTimeMillis();
    }

    public Table getTable() {
        return getModel().getTable();
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public Object id() {
        return id;
    }

    public IndexMetadata.Key getIndexKey() {
        return indexKey;
    }

    public void setIndexKey(IndexMetadata.Key indexKey) {
        this.indexKey = indexKey;
    }

    public long getIndexTs() {
        return indexTs;
    }

    public void setIndexTs(long indexTs) {
        this.indexTs = indexTs;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean isCached) {
        this.isCached = isCached;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public Map<String, Object> getProperties() {
        if (properties == null || !propertiesFullyLoaded) {
            load();
            propertiesFullyLoaded = true;
        }
        return properties;
    }

    public void copyFrom(HBaseElement element) {
        if (element.label != null) this.label = element.label;
        if (element.createdAt != null) this.createdAt = element.createdAt;
        if (element.updatedAt != null) this.updatedAt = element.updatedAt;
        if (element.properties != null
                && (element.propertiesFullyLoaded || this.properties == null)) {
            this.properties = new ConcurrentHashMap<>(element.properties);
            this.propertiesFullyLoaded = element.propertiesFullyLoaded;
        }
    }

    public void load() {
        getModel().load(this);
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
    public Set<String> keys() {
        return getPropertyKeys();
    }

    public Set<String> getPropertyKeys() {
        return new HashSet<>(getProperties().keySet());
    }

    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(key, value);

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

    public <V> V removeProperty(String key) {
        V value = getProperty(key);
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
        return value;
    }

    @Override
    public String label() {
        if (label == null) load();
        return label;
    }

    public Long createdAt() {
        if (createdAt == null) load();
        return createdAt;
    }

    public Long updatedAt() {
        if (updatedAt == null) load();
        return updatedAt;
    }

    public void updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public abstract boolean hasIndex(OperationType op, String propertyKey);

    public abstract Iterator<IndexMetadata> getIndices(OperationType op);

    public abstract ElementModel getModel();

    public abstract BaseModel getIndexModel();

    public abstract void writeToModel();

    // Write all indices with the given ts
    public abstract void writeToIndexModel(Long ts);

    // Write one index
    public abstract void writeToIndexModel(String key);

    public abstract void deleteFromModel();

    // Delete all indices
    public abstract void deleteFromIndexModel();

    // Delete all indices with the given ts
    public abstract void deleteFromIndexModel(Long ts);

    // Delete one index with the given ts
    public abstract void deleteFromIndexModel(String key, Long ts);

    public void removeStaleIndex() {
        IndexMetadata.Key indexKey = getIndexKey();
        long ts = getIndexTs();
        // delete after some expiry due to timing issues between index creation and element creation
        if (indexKey != null && ts + graph.configuration().getStaleIndexExpiryMs() < System.currentTimeMillis()) {
            graph.getExecutor().submit(() -> {
                try {
                    deleteFromIndexModel(indexKey.propertyKey(), ts);
                } catch (Exception e) {
                    LOGGER.error("Could not delete stale index", e);
                }
            });
        }
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
