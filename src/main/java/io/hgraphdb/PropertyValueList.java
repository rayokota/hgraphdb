package io.hgraphdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.google.common.collect.ForwardingCollection;

import java.util.Collection;
import java.util.LinkedList;

public class PropertyValueList<T> extends ForwardingCollection<T> implements KryoSerializable {

    private static final int KRYO_ID = 225;
    private static final int KRYO_LL_ID = 226;
    static {
        new Kryo().register(PropertyValueList.class, KRYO_ID);
        new Kryo().register(LinkedList.class, KRYO_LL_ID);
    }

    private Collection<T> list;

    public PropertyValueList() {
        this(new LinkedList<>());
    }

    public PropertyValueList(Collection<T> list) {
        this.list = list;
    }

    @Override protected Collection<T> delegate() {
        return list;
    }

    @Override public void read(Kryo kryo, Input input) {
        Collection c = (Collection)kryo.readClassAndObject(input);
        this.list.addAll(c);
    }

    @Override public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, this.list);
    }

}
