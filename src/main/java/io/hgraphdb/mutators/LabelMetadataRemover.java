package io.hgraphdb.mutators;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.LabelMetadata;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class LabelMetadataRemover implements Mutator {

    private final HBaseGraph graph;
    private final LabelMetadata label;

    public LabelMetadataRemover(HBaseGraph graph, LabelMetadata label) {
        this.graph = graph;
        this.label = label;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(graph.getLabelMetadataModel().serialize(label.key()));
        return IteratorUtils.of(delete);
    }
}
