package io.hgraphdb.giraph.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public class TableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {

    private Connection connection;

    @Override
    protected void initializeTable(Connection connection, TableName tableName) throws java.io.IOException {
        super.initializeTable(connection, tableName);
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }
}
