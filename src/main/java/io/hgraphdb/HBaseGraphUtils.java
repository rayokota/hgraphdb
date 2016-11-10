package io.hgraphdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.mock.MockConnection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.hgraphdb.Constants.DEFAULT_FAMILY;
import static io.hgraphdb.HBaseGraphConfiguration.Keys.*;

public final class HBaseGraphUtils {

    private static Map<String, Connection> connections = new ConcurrentHashMap<>();

    public static Connection getConnection(HBaseGraphConfiguration config) {
        Connection conn = connections.get(config.getGraphNamespace());
        if (conn != null) return conn;
        Configuration hbaseConfig = config.toHBaseConfiguration();
        try {
            if (config.getInstanceType() == HBaseGraphConfiguration.InstanceType.MOCK) {
                return new MockConnection(hbaseConfig);
            }
            UserGroupInformation ugi = null;
            if ("kerberos".equals(hbaseConfig.get(HBASE_SECURITY_AUTHENTICATION))) {
                String principal = hbaseConfig.get(HBASE_CLIENT_KERBEROS_PRINCIPAL);
                String keytab = hbaseConfig.get(HBASE_CLIENT_KEYTAB_FILE);
                if (principal != null && keytab != null) {
                    UserGroupInformation.setConfiguration(hbaseConfig);
                    UserGroupInformation.loginUserFromKeytab(principal, keytab);
                    ugi = UserGroupInformation.getLoginUser();
                }
            }
            if (ugi != null) {
                conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                    @Override
                    public Connection run() throws Exception {
                        return ConnectionFactory.createConnection(hbaseConfig);
                    }
                });
            } else {
                conn = ConnectionFactory.createConnection(hbaseConfig);
            }
        } catch (Exception e) {
            throw new HBaseGraphException(e);
        }
        connections.put(config.getGraphNamespace(), conn);
        return conn;
    }

    public static void createTables(HBaseGraphConfiguration config, Connection conn) {
        if (config.getInstanceType() == HBaseGraphConfiguration.InstanceType.MOCK) return;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            createNamespace(config, admin);
            createTables(config, admin);
        } catch (Exception e) {
            throw new HBaseGraphException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException ignored) { }
        }
    }

    private static void createNamespace(HBaseGraphConfiguration config, Admin admin) throws IOException {
        String name = config.getGraphNamespace();
        try {
            NamespaceDescriptor ns = admin.getNamespaceDescriptor(name);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(name).build());
        }
    }

    private static void createTables(HBaseGraphConfiguration config, Admin admin) throws IOException {
        createTable(config, admin, Constants.EDGES);
        createTable(config, admin, Constants.EDGE_INDICES);
        createTable(config, admin, Constants.VERTICES);
        createTable(config, admin, Constants.VERTEX_INDICES);
        createTable(config, admin, Constants.INDEX_METADATA);
    }

    private static void createTable(HBaseGraphConfiguration config, Admin admin, String name) throws IOException {
        TableName tableName = TableName.valueOf(config.getGraphNamespace(), name);
        if (admin.tableExists(tableName)) return;
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY)
                .setCompressionType(Compression.Algorithm.valueOf(config.getCompressionAlgorithm().toUpperCase()))
                .setBloomFilterType(BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                .setMaxVersions(1)
                .setMinVersions(0)
                .setBlocksize(32768)
                .setBlockCacheEnabled(true);
        tableDescriptor.addFamily(columnDescriptor);
        int regionCount = config.getRegionCount();
        admin.createTable(tableDescriptor, getStartKey(regionCount), getEndKey(regionCount), regionCount);
    }

    public static void dropTables(HBaseGraphConfiguration config, Connection conn) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            dropTables(config, admin);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException ignored) { }
        }
    }

    private static void dropTables(HBaseGraphConfiguration config, Admin admin) throws IOException {
        dropTable(config, admin, Constants.EDGES);
        dropTable(config, admin, Constants.EDGE_INDICES);
        dropTable(config, admin, Constants.VERTICES);
        dropTable(config, admin, Constants.VERTEX_INDICES);
        dropTable(config, admin, Constants.INDEX_METADATA);
    }

    private static void dropTable(HBaseGraphConfiguration config, Admin admin, String name) throws IOException {
        TableName tableName = TableName.valueOf(config.getGraphNamespace(), name);
        if (!admin.tableExists(tableName)) return;
        if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
        }
        admin.truncateTable(tableName, true);
    }

    private static byte[] getStartKey(int regionCount) {
        return Bytes.toBytes((Integer.MAX_VALUE / regionCount));
    }

    private static byte[] getEndKey(int regionCount) {
        return Bytes.toBytes((Integer.MAX_VALUE / regionCount * (regionCount - 1)));
    }

    public static Object generateIdIfNeeded(Object id) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        } else if (id instanceof Long) {
            // noop
        } else if (id instanceof Number) {
            id = ((Number) id).longValue();
        }
        return id;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(keyStr, value);
            props.put(keyStr, value);
        }
        return props;
    }
}
