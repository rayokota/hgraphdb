package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseElement;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.Serializer;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

import static org.apache.hadoop.hbase.KeyValue.Type.Put;

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
        byte[] idBytes = Serializer.serializeWithSalt(element.id());
        Delete delete = new Delete(idBytes);
        delete.addColumns(Constants.DEFAULT_FAMILY_BYTES, Bytes.toBytes(key));
        Put put = new Put(idBytes);
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                Serializer.serialize(((HBaseElement)element).updatedAt()));
        return IteratorUtils.of(delete, put);
    }
}
