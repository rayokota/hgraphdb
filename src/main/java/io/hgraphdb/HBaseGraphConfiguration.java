package io.hgraphdb;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;

public class HBaseGraphConfiguration extends AbstractConfiguration {

    private final PropertiesConfiguration conf;

    public static final Class<? extends Graph> HBASE_GRAPH_CLASS = HBaseGraph.class;

    public static final String HBASE_GRAPH_CLASSNAME =
            HBASE_GRAPH_CLASS.getCanonicalName();

    public enum InstanceType {
        DISTRIBUTED,
        MOCK
    }

    public static class Keys {
        public static final String GRAPH_CLASS                     = "gremlin.graph";
        public static final String INSTANCE_TYPE                   = "gremlin.hbase.instanceType";
        public static final String GRAPH_NAMESPACE                 = "gremlin.hbase.namespace";
        public static final String GRAPH_TABLE_PREFIX              = "gremlin.hbase.tablePrefix";
        public static final String CREATE_TABLES                   = "gremlin.hbase.createTables";
        public static final String EDGE_TABLE_TTL                  = "gremlin.hbase.edgeTableTTL";
        public static final String VERTEX_TABLE_TTL                = "gremlin.hbase.vertexTableTTL";
        public static final String REGION_COUNT                    = "gremlin.hbase.regionCount";
        public static final String COMPRESSION_ALGO                = "gremlin.hbase.compression";
        public static final String GLOBAL_CACHE_MAX_SIZE           = "gremlin.hbase.globalCacheMaxSize";
        public static final String GLOBAL_CACHE_TTL_SECS           = "gremlin.hbase.globalCacheTtlSecs";
        public static final String RELATIONSHIP_CACHE_MAX_SIZE     = "gremlin.hbase.relationshipCacheMaxSize";
        public static final String RELATIONSHIP_CACHE_TTL_SECS     = "gremlin.hbase.relationshipCacheTtlSecs";
        public static final String LAZY_LOADING                    = "gremlin.hbase.lazyLoading";
        public static final String USE_SCHEMA                      = "gremlin.hbase.useSchema";

        /* How often to refresh the schema cache */
        public static final String SCHEMA_CACHE_REFRESH_SECS       = "gremlin.hbase.schemaCacheRefreshSecs";
        /* How long to wait before a schema state change is propagated to other schema caches */
        public static final String SCHEMA_STATE_CHANGE_DELAY_SECS  = "gremlin.hbase.schemaStateChangeDelaySecs";
        /* How old stale indices have to be in order to delete */
        public static final String STALE_INDEX_EXPIRY_MS           = "gremlin.hbase.staleIndexExpiryMs";

        public static final String HBASE_SECURITY_AUTHENTICATION   = "hbase.security.authentication";
        public static final String HBASE_CLIENT_KERBEROS_PRINCIPAL = "hbase.client.kerberos.principal";
        public static final String HBASE_CLIENT_KEYTAB_FILE        = "hbase.client.keytab.file";
    }

    public HBaseGraphConfiguration() {
        conf = new PropertiesConfiguration();
        conf.setProperty(Keys.GRAPH_CLASS, HBASE_GRAPH_CLASSNAME);
    }

    public HBaseGraphConfiguration(Configuration config) {
        conf = new PropertiesConfiguration();
        conf.setDelimiterParsingDisabled(true);
        conf.setProperty(Keys.GRAPH_CLASS, HBASE_GRAPH_CLASSNAME);
        if (config != null) {
            config.getKeys().forEachRemaining(key ->
                    conf.setProperty(key.replace("..", "."), config.getProperty(key)));
        }
    }

    public HBaseGraphConfiguration(org.apache.hadoop.conf.Configuration config) {
        conf = new PropertiesConfiguration();
        conf.setProperty(Keys.GRAPH_CLASS, HBASE_GRAPH_CLASSNAME);
        if (config != null) {
            config.iterator().forEachRemaining(entry ->
                    conf.setProperty(entry.getKey(), entry.getValue()));
        }
    }

    public InstanceType getInstanceType() {
        return InstanceType.valueOf(conf.getString(Keys.INSTANCE_TYPE, InstanceType.DISTRIBUTED.toString()));
    }

    public HBaseGraphConfiguration setInstanceType(InstanceType type) {
        conf.setProperty(Keys.INSTANCE_TYPE, type.toString());
        return this;
    }

    public String getGraphNamespace() {
        return conf.getString(Keys.GRAPH_NAMESPACE, "default");
    }

    public HBaseGraphConfiguration setGraphNamespace(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph namespace."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.GRAPH_NAMESPACE, name);
        return this;
    }

    public String getGraphTablePrefix() {
        return conf.getString(Keys.GRAPH_TABLE_PREFIX, "");
    }

    public HBaseGraphConfiguration setGraphTablePrefix(String name) {
        if (!isValidGraphName(name)) {
            throw new IllegalArgumentException("Invalid graph table prefix."
                    + " Only alphanumerics and underscores are allowed");
        }

        conf.setProperty(Keys.GRAPH_TABLE_PREFIX, name);
        return this;
    }

    private static boolean isValidGraphName(String name) {
        return name.matches("^[A-Za-z0-9_]+$");
    }

    public boolean getCreateTables() {
        return conf.getBoolean(Keys.CREATE_TABLES, false);
    }

    public HBaseGraphConfiguration setCreateTables(boolean create) {
        conf.setProperty(Keys.CREATE_TABLES, create);
        return this;
    }

    public int getEdgeTableTTL() {
        return conf.getInt(Keys.EDGE_TABLE_TTL, HConstants.FOREVER);
    }

    public HBaseGraphConfiguration setEdgeTableTTL(int ttl) {
        conf.setProperty(Keys.EDGE_TABLE_TTL, ttl);
        return this;
    }

    public int getVertexTableTTL() {
        return conf.getInt(Keys.VERTEX_TABLE_TTL, HConstants.FOREVER);
    }

    public HBaseGraphConfiguration setVertexTableTTL(int ttl) {
        conf.setProperty(Keys.VERTEX_TABLE_TTL, ttl);
        return this;
    }

    public int getRegionCount() {
        return conf.getInt(Keys.REGION_COUNT, 1);
    }

    public HBaseGraphConfiguration setRegionCount(int regionCount) {
        conf.setProperty(Keys.REGION_COUNT, regionCount);
        return this;
    }

    public String getCompressionAlgorithm() {
        return conf.getString(Keys.COMPRESSION_ALGO, "gz");
    }

    public HBaseGraphConfiguration setCompressionAlgorithm(String compressionAlgorithm) {
        conf.setProperty(Keys.COMPRESSION_ALGO, compressionAlgorithm);
        return this;
    }

    public long getElementCacheMaxSize() {
        return conf.getLong(Keys.GLOBAL_CACHE_MAX_SIZE, 1000000);
    }

    public HBaseGraphConfiguration setElementCacheMaxSize(long maxSize) {
        conf.setProperty(Keys.GLOBAL_CACHE_MAX_SIZE, maxSize);
        return this;
    }

    public long getElementCacheTtlSecs() {
        return conf.getLong(Keys.GLOBAL_CACHE_TTL_SECS, 60);
    }

    public HBaseGraphConfiguration setElementCacheTtlSecs(long maxSize) {
        conf.setProperty(Keys.GLOBAL_CACHE_TTL_SECS, maxSize);
        return this;
    }

    public long getRelationshipCacheMaxSize() {
        return conf.getLong(Keys.RELATIONSHIP_CACHE_MAX_SIZE, 1000);
    }

    public HBaseGraphConfiguration setRelationshipCacheMaxSize(long maxSize) {
        conf.setProperty(Keys.RELATIONSHIP_CACHE_MAX_SIZE, maxSize);
        return this;
    }

    public long getRelationshipCacheTtlSecs() {
        return conf.getLong(Keys.RELATIONSHIP_CACHE_TTL_SECS, 60);
    }

    public HBaseGraphConfiguration setRelationshipCacheTtlSecs(long maxSize) {
        conf.setProperty(Keys.RELATIONSHIP_CACHE_TTL_SECS, maxSize);
        return this;
    }

    public boolean isLazyLoading() {
        return conf.getBoolean(Keys.LAZY_LOADING, false);
    }

    public HBaseGraphConfiguration setLazyLoading(boolean lazyLoading) {
        conf.setProperty(Keys.LAZY_LOADING, lazyLoading);
        return this;
    }

    public boolean getUseSchema() {
        return conf.getBoolean(Keys.USE_SCHEMA, false);
    }

    public HBaseGraphConfiguration setUseSchema(boolean useSchema) {
        conf.setProperty(Keys.USE_SCHEMA, useSchema);
        return this;
    }

    public int getSchemaCacheRefreshSecs() {
        return conf.getInt(Keys.SCHEMA_CACHE_REFRESH_SECS, 1);
    }

    public HBaseGraphConfiguration setSchemaCacheRefreshSecs(int indexCacheRefreshSecs) {
        conf.setProperty(Keys.SCHEMA_CACHE_REFRESH_SECS, indexCacheRefreshSecs);
        return this;
    }

    public int getSchemaStateChangeDelaySecs() {
        return conf.getInt(Keys.SCHEMA_STATE_CHANGE_DELAY_SECS, 2);
    }

    public HBaseGraphConfiguration setSchemaStateChangeDelaySecs(int indexStateChangeDelaySecs) {
        conf.setProperty(Keys.SCHEMA_STATE_CHANGE_DELAY_SECS, indexStateChangeDelaySecs);
        return this;
    }

    public int getStaleIndexExpiryMs() {
        return conf.getInt(Keys.STALE_INDEX_EXPIRY_MS, 50000);
    }

    public HBaseGraphConfiguration setStaleIndexExpiryMs(int staleIndexExpiryMs) {
        conf.setProperty(Keys.STALE_INDEX_EXPIRY_MS, staleIndexExpiryMs);
        return this;
    }

    @Override
    public boolean isEmpty() {
        return conf.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return conf.containsKey(key);
    }

    @Override
    public Object getProperty(String key) {
        return conf.getProperty(key);
    }

    public HBaseGraphConfiguration set(String key, Object value) {
        conf.setProperty(key, value);
        return this;
    }

    @Override
    public Iterator<String> getKeys() {
        return conf.getKeys();
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        conf.setProperty(key, value);
    }

    public org.apache.hadoop.conf.Configuration toHBaseConfiguration() {
        org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration();
        conf.getKeys().forEachRemaining(key -> c.set(key, conf.getProperty(key).toString()));
        return c;
    }
}
