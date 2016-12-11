package io.hgraphdb.giraph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

import java.io.*;

public final class ObjectWritable<T> implements WritableComparable<ObjectWritable>, Serializable {

    private static final String NULL = "null";
    private static final ObjectWritable<MapReduce.NullObject> NULL_OBJECT_WRITABLE = new ObjectWritable<>(MapReduce.NullObject.instance());

    T t;

    public ObjectWritable() {
    }

    public ObjectWritable(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    @Override
    public String toString() {
        return null == this.t ? NULL : this.t.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(final DataInput input) throws IOException {
        Kryo kryo = new Kryo();
        final ByteArrayInputStream bais = new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input));
        this.t = (T) kryo.readClassAndObject(new Input(bais));
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output out = new Output(baos);
        kryo.writeClassAndObject(out, this.t);
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

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(final ObjectWritable objectWritable) {
        if (null == this.t)
            return objectWritable.isEmpty() ? 0 : -1;
        else if (this.t instanceof Comparable && !objectWritable.isEmpty())
            return ((Comparable) this.t).compareTo(objectWritable.get());
        else if (this.t.equals(objectWritable.get()))
            return 0;
        else
            return -1;
    }

    public boolean isEmpty() {
        return null == this.t;
    }

    public static <A> ObjectWritable<A> empty() {
        return new ObjectWritable<>(null);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ObjectWritable))
            return false;
        else if (this.isEmpty())
            return ((ObjectWritable) other).isEmpty();
        else
            return this.t.equals(((ObjectWritable) other).get());
    }

    @Override
    public int hashCode() {
        return null == this.t ? 0 : this.t.hashCode();
    }

    public static ObjectWritable<MapReduce.NullObject> getNullObjectWritable() {
        return NULL_OBJECT_WRITABLE;
    }
}
