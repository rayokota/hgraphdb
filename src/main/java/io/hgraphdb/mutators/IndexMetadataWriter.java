package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphException;
import io.hgraphdb.IndexMetadata;
import io.hgraphdb.Serializer;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class IndexMetadataWriter implements Creator, Mutator {

    private final HBaseGraph graph;
    private final IndexMetadata index;

    public IndexMetadataWriter(HBaseGraph graph, IndexMetadata index) {
        this.graph = graph;
        this.index = index;
    }

    @Override
    public Put constructPut() {
        Put put = new Put(graph.getIndexMetadataModel().serialize(index.key()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.INDEX_STATE_BYTES,
                Serializer.serialize(index.state().toString()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                Serializer.serialize(index.createdAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                Serializer.serialize(index.updatedAt()));
        return put;
    }

    @Override
    public RuntimeException alreadyExists() {
        return new HBaseGraphException("Index for (" + index.label() + ", " + index.propertyKey() + ") of type "
                + index.type() + " already exists");
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Put put = new Put(graph.getIndexMetadataModel().serialize(index.key()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.INDEX_STATE_BYTES,
                Serializer.serialize(index.state().toString()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                Serializer.serialize(index.updatedAt()));
        return IteratorUtils.of(put);
    }
}
