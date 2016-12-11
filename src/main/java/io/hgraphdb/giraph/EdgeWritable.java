package io.hgraphdb.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseEdgeSerializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.*;

public final class EdgeWritable implements Writable, Serializable {

    private HBaseEdge edge;

    public EdgeWritable() {
    }

    public EdgeWritable(final HBaseEdge edge) {
        this.edge = edge;
    }

    public HBaseEdge get() {
        return edge;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HBaseEdge.class, new HBaseEdgeSerializer());
        final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
        this.edge = kryo.readObject(new Input(bais), HBaseEdge.class);
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HBaseEdge.class, new HBaseEdgeSerializer());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output out = new Output(baos);
        kryo.writeObject(out, this.edge);
        out.close();
        final byte[] serialized = baos.toByteArray();
        WritableUtils.writeCompressedByteArray(output, serialized);
    }

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.write(outputStream);
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        this.readFields(inputStream);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EdgeWritable && ElementHelper.areEqual(this.edge, ((EdgeWritable) other).get());
    }

    @Override
    public int hashCode() {
        return this.edge.hashCode();
    }

    @Override
    public String toString() {
        return this.edge.toString();
    }
}
