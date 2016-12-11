package io.hgraphdb.giraph;

import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.giraph.hbase.TableInputFormat;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Base class that wraps an HBase TableInputFormat and underlying Scan object
 * to help instantiate edges from an HBase table. All
 * the static TableInputFormat properties necessary to configure
 * an HBase job are available.
 * <p>
 * For example, setting conf.set(TableInputFormat.INPUT_TABLE, "in_table");
 * from the job setup routine will properly delegate to the
 * TableInputFormat instance. The Configurable interface prevents specific
 * wrapper methods from having to be called.
 * <p>
 * Works with {@link HBaseEdgeOutputFormat}
 *
 */
@SuppressWarnings("rawtypes")
public class HBaseEdgeInputFormat
        extends EdgeInputFormat<ObjectWritable, EdgeValueWritable> {


    public static final String EDGE_INPUT_TABLE = "hbase.mapreduce.edgetable";

    /**
     * delegate HBase table input format
     */
    protected static final TableInputFormat BASE_FORMAT =
            new TableInputFormat(EDGE_INPUT_TABLE);
    /**
     * logger
     */
    private static final Logger LOG =
            Logger.getLogger(HBaseEdgeInputFormat.class);

    @Override
    public void checkInputSpecs(final Configuration configuration) {
    }

    @Override
    public EdgeReader<ObjectWritable, EdgeValueWritable> createEdgeReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new HBaseEdgeReader(split, context);
    }

    /**
     * Takes an instance of RecordReader that supports
     * HBase row-key, result records.  Subclasses can focus on
     * edge instantiation details without worrying about connection
     * semantics. Subclasses are expected to implement nextEdge() and
     * getCurrentEdge()
     *
     */
    public static class HBaseEdgeReader
            extends EdgeReader<ObjectWritable, EdgeValueWritable> {

        /**
         * Reader instance
         */
        private final RecordReader<ImmutableBytesWritable, Result> reader;

        /**
         * HBase graph
         */
        private final HBaseGraph graph;

        /**
         * Context passed to initialize
         */
        private TaskAttemptContext context;

        /**
         * Sets the base TableInputFormat and creates a record reader.
         *
         * @param split   InputSplit
         * @param context Context
         * @throws IOException
         */
        public HBaseEdgeReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            BASE_FORMAT.setConf(context.getConfiguration());
            this.reader = BASE_FORMAT.createRecordReader(split, context);
            this.graph = new HBaseGraph(new HBaseGraphConfiguration(context.getConfiguration()), BASE_FORMAT.getConnection());
        }

        /**
         * initialize
         *
         * @param inputSplit Input split to be used for reading vertices.
         * @param context    Context from the task.
         * @throws IOException
         * @throws InterruptedException
         */
        public void initialize(InputSplit inputSplit,
                               TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(inputSplit, context);
            this.context = context;
        }

        /** Get last HBase edge. Generate it if missing.
         *
         * @return The last HBase edge
         * @throws IOException
         * @throws InterruptedException
         */
        public HBaseEdge getCurrentHBaseEdge() throws IOException, InterruptedException {
            return parseHBaseEdge(getRecordReader().getCurrentValue());
        }

        private HBaseEdge parseHBaseEdge(Result result) {
            io.hgraphdb.readers.EdgeReader edgeReader = new io.hgraphdb.readers.EdgeReader(graph);
            return (HBaseEdge) edgeReader.parse(result);
        }

        @Override
        public boolean nextEdge() throws IOException,
                InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public Edge<ObjectWritable, EdgeValueWritable> getCurrentEdge()
                throws IOException, InterruptedException {
            Edge<ObjectWritable, EdgeValueWritable> edge = EdgeFactory.create(
                    getCurrentTargetId(), new EdgeValueWritable(getCurrentHBaseEdge()));
            return edge;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ObjectWritable getCurrentSourceId()
                throws IOException, InterruptedException {
            return new ObjectWritable(getCurrentHBaseEdge().outVertex().id());
        }

        @SuppressWarnings("unchecked")
        public ObjectWritable getCurrentTargetId()
                throws IOException, InterruptedException {
            return new ObjectWritable(getCurrentHBaseEdge().inVertex().id());
        }

        /**
         * close
         *
         * @throws IOException
         */
        public void close() throws IOException {
            reader.close();
        }

        /**
         * getProgress
         *
         * @return progress
         * @throws IOException
         * @throws InterruptedException
         */
        public float getProgress() throws
                IOException, InterruptedException {
            return reader.getProgress();
        }

        /**
         * getRecordReader
         *
         * @return Record reader to be used for reading.
         */
        protected RecordReader<ImmutableBytesWritable,
                Result> getRecordReader() {
            return reader;
        }

        /**
         * getContext
         *
         * @return Context passed to initialize.
         */
        protected TaskAttemptContext getContext() {
            return context;
        }

    }

    @Override
    public List<InputSplit> getSplits(
            JobContext context, int minSplitCountHint)
            throws IOException, InterruptedException {
        BASE_FORMAT.setConf(getConf());
        return BASE_FORMAT.getSplits(context);
    }
}
