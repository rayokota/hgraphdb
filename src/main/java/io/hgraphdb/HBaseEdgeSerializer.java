package io.hgraphdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class HBaseEdgeSerializer extends HBaseElementSerializer<HBaseEdge> {

    public void write(Kryo kryo, Output output, HBaseEdge edge) {
        super.write(kryo, output, edge);
        byte[] outVBytes = ValueUtils.serialize(edge.outVertex().id());
        output.writeInt(outVBytes.length);
        output.writeBytes(outVBytes);
        byte[] inVBytes = ValueUtils.serialize(edge.inVertex().id());
        output.writeInt(inVBytes.length);
        output.writeBytes(inVBytes);
    }

    public HBaseEdge read(Kryo kryo, Input input, Class<HBaseEdge> type) {
        HBaseEdge edge = super.read(kryo, input, type);
        int outVBytesLen = input.readInt();
        Object outVId = ValueUtils.deserialize(input.readBytes(outVBytesLen));
        int inVBytesLen = input.readInt();
        Object inVId = ValueUtils.deserialize(input.readBytes(inVBytesLen));
        edge.setOutVertex(new HBaseVertex(null, outVId));
        edge.setInVertex(new HBaseVertex(null, inVId));
        return edge;
    }
}
