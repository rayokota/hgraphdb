package io.hgraphdb.giraph.examples;


import io.hgraphdb.giraph.EdgeValueWritable;
import io.hgraphdb.giraph.ObjectWritable;
import io.hgraphdb.giraph.VertexValueWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Write out Vertices' IDs and values, but not their edges nor edges' values.
 * This is a useful output format when the final value of the vertex is
 * all that's needed. The boolean configuration parameter reverse.id.and.value
 * allows reversing the output of id and value.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class IdWithValueTextOutputFormat<I extends ObjectWritable,
        V extends VertexValueWritable, E extends EdgeValueWritable>
        extends TextVertexOutputFormat<I, V, E> {

    /**
     * Specify the output delimiter
     */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /**
     * Default output delimiter
     */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    /**
     * Reverse id and value order?
     */
    public static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";
    /**
     * Default is to not reverse id and value order.
     */
    public static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    /**
     * Vertex writer used with {@link org.apache.giraph.io.formats.IdWithValueTextOutputFormat}.
     */
    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
        /**
         * Saved delimiter
         */
        private String delimiter;
        /**
         * Cached reserve option
         */
        private boolean reverseOutput;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(
                    LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
            reverseOutput = getConf().getBoolean(
                    REVERSE_ID_AND_VALUE, REVERSE_ID_AND_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<I, V, E> vertex)
                throws IOException {

            StringBuilder str = new StringBuilder();
            if (reverseOutput) {
                str.append(vertex.getValue().getValue());
                str.append(delimiter);
                str.append(vertex.getId().toString());
            } else {
                str.append(vertex.getId().toString());
                str.append(delimiter);
                str.append(vertex.getValue().getValue());
            }
            return new Text(str.toString());
        }
    }
}
