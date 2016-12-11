package io.hgraphdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class HBaseEdgeSerializer extends HBaseElementSerializer<HBaseEdge> {

    public void write(Kryo kryo, Output output, HBaseEdge edge) {
        super.write(kryo, output, edge);
        kryo.writeClassAndObject(output, edge.outVertex().id());
        kryo.writeClassAndObject(output, edge.inVertex().id());
    }

    public HBaseEdge read(Kryo kryo, Input input, Class<HBaseEdge> type) {
        HBaseEdge edge = super.read(kryo, input, type);
        Object outVId = kryo.readClassAndObject(input);
        Object inVId = kryo.readClassAndObject(input);
        edge.setOutVertex(new HBaseVertex(null, outVId));
        edge.setInVertex(new HBaseVertex(null, inVId));
        return edge;
    }
}
