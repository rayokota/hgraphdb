package io.hgraphdb;

import io.hgraphdb.models.ElementModel;
import io.hgraphdb.mutators.Mutator;
import io.hgraphdb.mutators.Mutators;
import org.apache.hadoop.hbase.client.Table;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class HBaseElement implements Element {

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
    }

    public abstract ElementModel getModel();

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

    public abstract void removeStaleIndex();

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
        updatedAt(System.currentTimeMillis());
        Mutator writer = getModel().writeProperty(this, key, value);
        Mutators.write(getTable(), writer);
        if (!key.equals(Constants.LABEL)) {
            getProperties().put(key, value);
        }
    }

    public <V> V removeProperty(String key) {
        V value = getProperty(key);
        if (value != null) {
            updatedAt(System.currentTimeMillis());
            Mutator writer = getModel().clearProperty(this, key);
            Mutators.write(getTable(), writer);
            getProperties().remove(key);
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

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
