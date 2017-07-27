package io.hgraphdb.gelly;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseGraphConfiguration;
import io.hgraphdb.readers.EdgeReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.client.Result;

public class HBaseEdgeInputFormat<K, V> extends HBaseElementInputFormat<Tuple3<K, K, V>> {

    private static final long serialVersionUID = 9536634064600195L;

    public HBaseEdgeInputFormat(HBaseGraphConfiguration hConf) {
        this(hConf, Constants.ELEMENT_ID);
    }

    public HBaseEdgeInputFormat(HBaseGraphConfiguration hConf, String propertyName) {
        super(hConf, ElementType.EDGE, propertyName);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Tuple3<K, K, V> mapResultToTuple(Result r) {
        HBaseEdge edge = parseHBaseEdge(r);
        return new Tuple3<>((K) edge.outVertex().id(), (K) edge.inVertex().id(), (V) property(edge, getPropertyName()));
    }

    private HBaseEdge parseHBaseEdge(Result result) {
        EdgeReader edgeReader = new io.hgraphdb.readers.EdgeReader(getGraph());
        return (HBaseEdge) edgeReader.parse(result);
    }
}
