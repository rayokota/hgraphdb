package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.IndexMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class IndexMetadataRemover implements Mutator {

    private final HBaseGraph graph;
    private final IndexMetadata index;

    public IndexMetadataRemover(HBaseGraph graph, IndexMetadata index) {
        this.graph = graph;
        this.index = index;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getIndexMetadataModel().serialize(index.key()));
        return IteratorUtils.of(delete);
    }
}
