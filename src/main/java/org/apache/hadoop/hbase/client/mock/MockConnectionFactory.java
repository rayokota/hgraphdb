package org.apache.hadoop.hbase.client.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class MockConnectionFactory {

    private static Connection connection = null;

    protected MockConnectionFactory() {
    }

    public static Connection createConnection(Configuration conf) {
        if (connection == null) {
            connection = new MockConnection(conf);
        }
        return connection;
    }
}
