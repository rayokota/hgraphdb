package io.hgraphdb.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.HBaseVertexSerializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.*;

public final class VertexValueWritable implements Writable, Serializable {

    private HBaseVertex vertex;
    private Writable value;

    public VertexValueWritable() {
    }

    public VertexValueWritable(final HBaseVertex vertex) {
        this.vertex = vertex;
        this.value = NullWritable.get();
    }

    public HBaseVertex getVertex() {
        return this.vertex;
    }

    public Writable getValue() {
        return this.value;
    }

    public void setValue(Writable value) {
        this.value = value;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        try {
            Kryo kryo = new Kryo();
            kryo.register(HBaseVertex.class, new HBaseVertexSerializer());
            final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
            this.vertex = kryo.readObject(new Input(bais), HBaseVertex.class);
            Class<? extends Writable> cls = Class.forName(Text.readString(input)).asSubclass(Writable.class);
            Writable writable;
            if (cls.equals(NullWritable.class)) {
                writable = NullWritable.get();
            } else {
                writable = cls.newInstance();
            }
            writable.readFields(input);
            this.value = writable;
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new IOException("Failed writable init", e);
        }
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
        Text.writeString(output, value.getClass().getName());
        value.write(output);
    }

    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        this.write(outputStream);
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        this.readFields(inputStream);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof VertexValueWritable && ElementHelper.areEqual(this.vertex, ((VertexValueWritable) other).getVertex());
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
