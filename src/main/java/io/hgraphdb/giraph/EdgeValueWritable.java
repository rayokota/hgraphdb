package io.hgraphdb.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseEdgeSerializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.*;

public final class EdgeValueWritable implements Writable, Serializable {

    private HBaseEdge edge;
    private Writable value;

    public EdgeValueWritable() {
    }

    public EdgeValueWritable(final HBaseEdge edge) {
        this.edge = edge;
        this.value = NullWritable.get();
    }

    public HBaseEdge getEdge() {
        return edge;
    }

    public Writable getValue() {
        return value;
    }

    public void setValue(Writable value) {
        this.value = value;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
        try {
            Kryo kryo = new Kryo();
            kryo.register(HBaseEdge.class, new HBaseEdgeSerializer());
            final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
            this.edge = kryo.readObject(new Input(bais), HBaseEdge.class);
            Class<? extends Writable> cls = Class.forName(Text.readString(input)).asSubclass(Writable.class);
            Writable writable;
            if (cls.equals(NullWritable.class)) {
                writable = NullWritable.get();
            } else {
                writable = cls.newInstance();
            }
            writable.readFields(input);
            this.value = writable;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed writable init", e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed writable init", e);
        } catch (InstantiationException e) {
            throw new IOException("Failed writable init", e);
        }
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
        return other instanceof EdgeValueWritable && ElementHelper.areEqual(this.edge, ((EdgeValueWritable) other).getEdge());
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
