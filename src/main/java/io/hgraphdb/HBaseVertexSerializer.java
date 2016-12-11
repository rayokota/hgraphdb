package io.hgraphdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class HBaseVertexSerializer extends HBaseElementSerializer<HBaseVertex> {

    public void write(Kryo kryo, Output output, HBaseVertex vertex) {
        super.write(kryo, output, vertex);
    }

    public HBaseVertex read(Kryo kryo, Input input, Class<HBaseVertex> type) {
        return super.read(kryo, input, type);
    }
}
