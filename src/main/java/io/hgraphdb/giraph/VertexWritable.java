package io.hgraphdb.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.HBaseVertexSerializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.*;

public final class VertexWritable implements Writable, Serializable {

    private HBaseVertex vertex;

    public VertexWritable() {
    }

    public VertexWritable(final HBaseVertex vertex) {
        this.vertex = vertex;
    }

    public HBaseVertex get() {
        return this.vertex;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HBaseVertex.class, new HBaseVertexSerializer());
        final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
        this.vertex = kryo.readObject(new Input(bais), HBaseVertex.class);
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(HBaseVertex.class, new HBaseVertexSerializer());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output out = new Output(baos);
        kryo.writeObject(out, this.vertex);
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
        return other instanceof VertexWritable && ElementHelper.areEqual(this.vertex, ((VertexWritable) other).get());
    }

    @Override
    public int hashCode() {
        return this.vertex.hashCode();
    }

    @Override
    public String toString() {
        return this.vertex.toString();
    }
}
