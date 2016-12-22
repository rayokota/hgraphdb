package io.hgraphdb.giraph;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.mapreduce.TableInputFormat;
import io.hgraphdb.readers.VertexReader;
import org.apache.giraph.io.VertexValueInputFormat;
import org.apache.giraph.io.VertexValueReader;
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
 * to help instantiate vertices from an HBase table. All
 * the static TableInputFormat properties necessary to configure
 * an HBase job are available.
 * <p>
 * Setting conf.set(HBaseVertexInputFormat.VERTEX_INPUT_TABLE, "in_table");
 * from the job setup routine will properly delegate to the
 * TableInputFormat instance. The Configurable interface prevents specific
 * wrapper methods from having to be called.
 * <p>
 * Works with {@link HBaseVertexOutputFormat}
 */
@SuppressWarnings("rawtypes")
public class HBaseVertexInputFormat
        extends VertexValueInputFormat<ObjectWritable, VertexValueWritable> {

    public static final String VERTEX_INPUT_TABLE = "hbase.mapreduce.vertextable";

    /**
     * delegate HBase table input format
     */
    protected static final TableInputFormat BASE_FORMAT =
            new TableInputFormat(VERTEX_INPUT_TABLE);
    /**
     * logger
     */
    private static final Logger LOG =
            Logger.getLogger(HBaseVertexInputFormat.class);

    @Override
    public void checkInputSpecs(final Configuration configuration) {
    }

    @Override
    public VertexValueReader<ObjectWritable, VertexValueWritable> createVertexValueReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new HBaseVertexReader(split, context);
    }

    /**
     * Takes an instance of RecordReader that supports
     * HBase row-key, result records.  Subclasses can focus on
     * vertex instantiation details without worrying about connection
     * semantics. Subclasses are expected to implement nextVertex() and
     * getCurrentVertex()
     */
    public static class HBaseVertexReader
            extends VertexValueReader<ObjectWritable, VertexValueWritable> {

        /**
         * Reader instance
         */
        private final RecordReader<ImmutableBytesWritable, Result> reader;

        /**
         * HBase graph
         */
        private final HBaseGraph graph;

        /**
         * HBase vertex
         */
        private HBaseVertex vertex;

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
        public HBaseVertexReader(InputSplit split, TaskAttemptContext context)
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

        /**
         * Get last HBase vertex. Generate it if missing.
         *
         * @return The last HBase vertex
         * @throws IOException
         * @throws InterruptedException
         */
        public HBaseVertex getCurrentHBaseVertex() throws IOException, InterruptedException {
            if (vertex == null) {
                vertex = parseHBaseVertex(getRecordReader().getCurrentValue());
            }
            return vertex;
        }

        private HBaseVertex parseHBaseVertex(Result result) {
            VertexReader vertexReader = new VertexReader(graph);
            return (HBaseVertex) vertexReader.parse(result);
        }

        @Override
        public boolean nextVertex() throws IOException,
                InterruptedException {
            vertex = null;
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public ObjectWritable getCurrentVertexId()
                throws IOException, InterruptedException {
            return new ObjectWritable(getCurrentHBaseVertex().id());
        }

        @Override
        public VertexValueWritable getCurrentVertexValue()
                throws IOException, InterruptedException {
            return new VertexValueWritable(getCurrentHBaseVertex());
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
        protected RecordReader<ImmutableBytesWritable, Result> getRecordReader() {
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
