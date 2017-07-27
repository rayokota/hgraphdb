package io.hgraphdb.gelly;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.readers.VertexReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Result;

public class HBaseVertexInputFormat<K, V> extends HBaseElementInputFormat<Tuple2<K, V>> {

    private static final long serialVersionUID = 7319777577061741932L;

    public HBaseVertexInputFormat(HBaseGraphConfiguration hConf) {
        this(hConf, Constants.ELEMENT_ID);
    }

    public HBaseVertexInputFormat(HBaseGraphConfiguration hConf, String propertyName) {
        super(hConf, ElementType.VERTEX, propertyName);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple2<K, V> mapResultToTuple(Result r) {
        HBaseVertex vertex = parseHBaseVertex(r);
        return new Tuple2<>((K) vertex.id(), (V) property(vertex, getPropertyName()));
    }

    private HBaseVertex parseHBaseVertex(Result result) {
        VertexReader vertexReader = new VertexReader(getGraph());
        return (HBaseVertex) vertexReader.parse(result);
    }
}
