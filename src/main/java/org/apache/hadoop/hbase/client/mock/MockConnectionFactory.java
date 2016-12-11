package org.apache.hadoop.hbase.client.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;


public class MockConnectionFactory {

    private static Connection connection = null;

    protected MockConnectionFactory() {
    }

    public static Connection createConnection(Configuration conf) throws IOException {
        if (connection == null) {
            connection = new MockConnection(conf);
        }
        return connection;
    }
}
