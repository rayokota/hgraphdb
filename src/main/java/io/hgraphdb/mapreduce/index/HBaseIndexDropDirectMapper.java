package io.hgraphdb.mapreduce.index;

import io.hgraphdb.ElementType;
import io.hgraphdb.readers.EdgeIndexReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexIndexReader;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Mapper that drops index rows.
 */
public class HBaseIndexDropDirectMapper extends HBaseIndexDirectMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexDropDirectMapper.class);

    private ElementReader<?> reader;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        reader = getIndex().type() == ElementType.EDGE ? new EdgeIndexReader(getGraph()) : new VertexIndexReader(getGraph());
    }

    @Override
    protected Iterator<? extends Mutation> constructMutations(Result result)
            throws IOException, InterruptedException {
        return HBaseIndexDropMapper.constructMutations(getGraph(), getIndex(), reader, result);
    }
}
