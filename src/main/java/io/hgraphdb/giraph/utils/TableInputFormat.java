package io.hgraphdb.giraph.utils;

import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {

    @SuppressWarnings("hiding")
    private static final Log LOG = LogFactory.getLog(TableInputFormat.class);

    private String tablePropertyKey;
    private Connection connection;
    private Table table;

    public TableInputFormat(String tablePropertyKey) {
        this.tablePropertyKey = tablePropertyKey;
    }

    public boolean isMock() {
        return HBaseGraphConfiguration.InstanceType.MOCK.toString().equals(
            getConf().get(HBaseGraphConfiguration.Keys.INSTANCE_TYPE));
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    protected Table getTable() {
        return isMock() ? table : super.getTable();
    }

    @Override
    protected void initialize(JobContext context) throws IOException {
        TableName tableName = TableName.valueOf(getConf().get(tablePropertyKey));
        try {
            if (isMock()) {
                initializeTable(MockConnectionFactory.createConnection(new Configuration(getConf())), tableName);
            } else {
                initializeTable(ConnectionFactory.createConnection(new Configuration(getConf())), tableName);
            }
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
    }

    @Override
    protected void initializeTable(Connection connection, TableName tableName) throws java.io.IOException {
        if (isMock()) {
            this.table = connection.getTable(tableName);
        } else {
            super.initializeTable(connection, tableName);
        }
        this.connection = connection;
    }

    @Override
    protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        if (isMock()) {
            return new Pair<>(new byte[][]{HConstants.EMPTY_START_ROW}, new byte[][]{HConstants.EMPTY_END_ROW});
        } else {
            return super.getStartEndKeys();
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        if (isMock()) {
            if (table == null) {
                initialize(context);
            }
            List<InputSplit> splits = new ArrayList<InputSplit>(1);
            TableSplit split = new TableSplit(getTable().getName(), getScan(),
                    HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, "", 0);
            splits.add(split);
            return splits;
        } else {
            return super.getSplits(context);
        }
    }
}
