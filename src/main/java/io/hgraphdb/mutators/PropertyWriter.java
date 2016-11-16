package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseElement;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class PropertyWriter implements Mutator {

    private final HBaseGraph graph;
    private final Element element;
    private final String key;
    private final Object value;

    public PropertyWriter(HBaseGraph graph, Element element, String key, Object value) {
        this.graph = graph;
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public Iterator<Mutation> constructMutations() {
        byte[] bytes = ValueUtils.serialize(value);
        Put put = new Put(ValueUtils.serializeWithSalt(element.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(key), bytes);
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(((HBaseElement)element).updatedAt()));
        return IteratorUtils.of(put);
    }
}
