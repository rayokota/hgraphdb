package io.hgraphdb.mapreduce.index;

import io.hgraphdb.IndexMetadata;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class HBaseIndexDropDirectReducer extends HBaseIndexReducerBase {

    @Override
    protected IndexMetadata.State getUpdatedIndexState() {
        return IndexMetadata.State.DROPPED;
    }
}
