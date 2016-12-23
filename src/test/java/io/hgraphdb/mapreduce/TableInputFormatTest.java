package io.hgraphdb.mapreduce;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraphTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TableInputFormatTest extends HBaseGraphTest {
    private final static Log log = LogFactory.getLog(TableInputFormatTest.class);

    private static final byte[] KEY = Bytes.toBytes("row1");
    private static final NavigableMap<Long, Boolean> TIMESTAMP =
            new TreeMap<Long, Boolean>();

    static {
        TIMESTAMP.put((long) 1245620000, false);
        TIMESTAMP.put((long) 1245620005, true); // include
        TIMESTAMP.put((long) 1245620010, true); // include
        TIMESTAMP.put((long) 1245620055, true); // include
        TIMESTAMP.put((long) 1245620100, true); // include
        TIMESTAMP.put((long) 1245620150, false);
        TIMESTAMP.put((long) 1245620250, false);
    }

    static final long MINSTAMP = 1245620005;
    static final long MAXSTAMP = 1245620100 + 1; // maxStamp itself is excluded. so increment it.

    static final TableName TABLE_NAME = TableName.valueOf("table123");
    static final byte[] FAMILY_NAME = Constants.DEFAULT_FAMILY_BYTES;
    static final byte[] COLUMN_NAME = Bytes.toBytes("input");

    private static class ProcessTimeRangeMapper
            extends TableMapper<ImmutableBytesWritable, MapWritable>
            implements Configurable {

        private Configuration conf = null;
        private Table table = null;

        @Override
        public void map(ImmutableBytesWritable key, Result result,
                        Context context)
                throws IOException {
            List<Long> tsList = new ArrayList<Long>();
            for (Cell kv : result.listCells()) {
                tsList.add(kv.getTimestamp());
            }

            List<Put> puts = new ArrayList<>();
            for (Long ts : tsList) {
                Put put = new Put(key.get());
                put.setDurability(Durability.SKIP_WAL);
                put.addColumn(FAMILY_NAME, COLUMN_NAME, ts, Bytes.toBytes(true));
                puts.add(put);
            }
            table.put(puts);
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void setConf(Configuration configuration) {
            this.conf = configuration;
            try {
                Connection connection = MockConnectionFactory.createConnection(conf);
                table = connection.getTable(TABLE_NAME);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testTimeRangeMapRed()
            throws IOException, InterruptedException, ClassNotFoundException {
        final HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
        final HColumnDescriptor col = new HColumnDescriptor(FAMILY_NAME);
        col.setMaxVersions(Integer.MAX_VALUE);
        desc.addFamily(col);
        List<Put> puts = new ArrayList<Put>();
        for (Map.Entry<Long, Boolean> entry : TIMESTAMP.entrySet()) {
            Put put = new Put(KEY);
            put.setDurability(Durability.SKIP_WAL);
            put.addColumn(FAMILY_NAME, COLUMN_NAME, entry.getKey(), Bytes.toBytes(false));
            puts.add(put);
        }
        Configuration conf = graph.configuration().toHBaseConfiguration();
        Connection conn = MockConnectionFactory.createConnection(conf);
        Table table = conn.getTable(desc.getTableName());
        table.put(puts);
        runTestOnTable();
        verify(table);
        table.close();
    }

    private void runTestOnTable()
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = null;
        try {
            Configuration conf = graph.configuration().toHBaseConfiguration();
            job = new Job(conf, "test123");
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
            Scan scan = new Scan();
            scan.addColumn(FAMILY_NAME, COLUMN_NAME);
            scan.setTimeRange(MINSTAMP, MAXSTAMP);
            scan.setMaxVersions();
            TableMapReduceUtil.initTableMapperJob(TABLE_NAME.getNameAsString(),
                    scan, ProcessTimeRangeMapper.class, Text.class, Text.class, job,
                    true, TableInputFormat.class);
            job.waitForCompletion(true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (job != null) {
                FileUtil.fullyDelete(
                        new File(job.getConfiguration().get("hadoop.tmp.dir")));
            }
        }
    }

    private void verify(final Table table) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(FAMILY_NAME, COLUMN_NAME);
        scan.setMaxVersions(1);
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            for (Cell kv : r.listCells()) {
                log.debug(Bytes.toString(r.getRow()) + "\t" + Bytes.toString(CellUtil.cloneFamily(kv))
                        + "\t" + Bytes.toString(CellUtil.cloneQualifier(kv))
                        + "\t" + kv.getTimestamp() + "\t" + Bytes.toBoolean(CellUtil.cloneValue(kv)));
                org.junit.Assert.assertEquals(TIMESTAMP.get(kv.getTimestamp()),
                        Bytes.toBoolean(CellUtil.cloneValue(kv)));
            }
        }
        scanner.close();
    }
}

