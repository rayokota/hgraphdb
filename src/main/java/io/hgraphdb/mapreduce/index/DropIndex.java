package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An MR job to drop an index.
 */
public class DropIndex extends IndexTool {

    private static final Logger LOG = LoggerFactory.getLogger(DropIndex.class);

    @Override
    protected void setup(final HBaseGraph graph, final IndexMetadata index) {
        graph.updateIndex(index.key(), IndexMetadata.State.INACTIVE);
    }

    @Override
    protected void cleanup(final HBaseGraph graph, final IndexMetadata index) {
        graph.updateIndex(index.key(), IndexMetadata.State.DROPPED);
    }

    @Override
    protected Class<? extends Mapper> getDirectMapperClass() {
        return HBaseIndexDropDirectMapper.class;
    }

    @Override
    protected Class<? extends Reducer> getDirectReducerClass() {
        return HBaseIndexDropDirectReducer.class;
    }

    @Override
    protected Class<? extends Mapper> getBulkMapperClass() {
        return HBaseIndexDropMapper.class;
    }

    @Override
    protected TableName getInputTableName(final HBaseGraph graph, final IndexMetadata index) {
        return HBaseGraphUtils.getTableName(
                graph.configuration(), index.type() == ElementType.EDGE ? Constants.EDGE_INDICES : Constants.VERTEX_INDICES);
    }

    @Override
    protected TableName getOutputTableName(final HBaseGraph graph, final IndexMetadata index) {
        return HBaseGraphUtils.getTableName(
                graph.configuration(), index.type() == ElementType.EDGE ? Constants.EDGE_INDICES : Constants.VERTEX_INDICES);
    }

    @Override
    protected Scan getInputScan(final HBaseGraph graph, final IndexMetadata index) {
        if (index.type() == ElementType.EDGE) {
            return new Scan();
        } else {
            // optimization for vertex index scans
            byte[] startRow = graph.getVertexIndexModel().serializeForRead(
                    index.label(), index.isUnique(), index.propertyKey(), null);
            Scan scan = new Scan(startRow);
            scan.setRowPrefixFilter(startRow);
            return scan;
        }
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new DropIndex(), args);
        System.exit(result);
    }
}
