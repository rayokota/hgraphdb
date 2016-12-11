package io.hgraphdb.giraph;

import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseVertex;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*
 Test subclass for HBaseVertexOutputFormat
 */
public class TestVertexOutputFormat extends HBaseVertexOutputFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestVertexOutputFormat.class);

    @Override
    public HBaseVertexWriter createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TestVertexWriter(context);
    }

    /*
     For each vertex, write back to the configured table using
     the vertex id as the row key bytes.
     */
    public static class TestVertexWriter extends HBaseVertexWriter {

        public TestVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            super(context);
        }

        @Override
        public void writeVertex(HBaseBulkLoader writer, HBaseVertex vertex) {
        }
    }
}
