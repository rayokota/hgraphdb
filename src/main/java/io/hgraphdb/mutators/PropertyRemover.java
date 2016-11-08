package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.Serializer;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyRemover implements Mutator {

    private final HBaseGraph graph;
    private final Element element;
    private final String key;

    public PropertyRemover(HBaseGraph graph, Element element, String key) {
        this.graph = graph;
        this.element = element;
        this.key = key;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        Delete delete = new Delete(Serializer.serializeWithSalt(element.id()));
        delete.addColumns(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(key));
        return IteratorUtils.of(delete);
    }
}
