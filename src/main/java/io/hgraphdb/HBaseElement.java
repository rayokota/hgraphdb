package io.hgraphdb;

import io.hgraphdb.models.BaseModel;
import io.hgraphdb.models.ElementModel;

import org.apache.hadoop.hbase.client.Table;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public abstract class HBaseElement implements Element {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseElement.class);

    protected HBaseGraph graph;
    protected final Object id;
    protected String label;
    protected Long createdAt;
    protected Long updatedAt;
    protected transient IndexMetadata.Key indexKey;
    protected transient long indexTs;
    protected transient boolean isCached;
    protected transient boolean isDeleted;

    protected HBaseElement(HBaseGraph graph,
                           Object id,
                           String label,
                           Long createdAt,
                           Long updatedAt) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public abstract void validate();

    public abstract ElementType getElementType();

    public Table getTable() {
        return getModel().getTable();
    }

    @Override
    public Graph graph() {
        return graph;
    }

    public void setGraph(HBaseGraph graph) {
        this.graph = graph;
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

    public void copyFrom(HBaseElement element) {
        if (element.label != null) this.label = element.label;
        if (element.createdAt != null) this.createdAt = element.createdAt;
        if (element.updatedAt != null) this.updatedAt = element.updatedAt;
    }

    public void load() {
        getModel().load(this);
    }

    public abstract boolean hasProperty(String key);

    public abstract Stream<Map.Entry<String, Object>> propertyEntriesStream();

    public abstract int propertySize();

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

    public boolean hasIndex(OperationType op, String propertyKey) {
        return graph.hasIndex(op, getElementType(), label, propertyKey);
    }

    public Iterator<IndexMetadata> getIndices(OperationType op) {
        return graph.getIndices(op, getElementType(), label, keys());
    }

    public abstract ElementModel getModel();

    public abstract BaseModel getIndexModel();

    public abstract void writeToModel();

    public abstract void deleteFromModel();

    /*
     * Write one index.
     * Used when setting properties.
     */
    public abstract void writeToIndexModel(String key);

    /*
     * Remove one index with the given ts.
     * Used when removing properties.
     */
    public abstract void deleteFromIndexModel(String key, Long ts);

    /*
     * Remove one stale index using indexKey and indexTs.
     */
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

    /*
     * Remove all stale indices using indexTs.
     */
    public abstract void removeStaleIndices();

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
