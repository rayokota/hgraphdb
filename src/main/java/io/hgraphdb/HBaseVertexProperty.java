package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public final class HBaseVertexProperty<V> implements VertexProperty<V> {

    protected final HBaseGraph graph;
    protected final HBaseVertex vertex;
    protected final String key;
    protected final V value;

    public HBaseVertexProperty(final HBaseGraph graph, final HBaseVertex vertex, final String key, final V value) {
        this.graph = graph;
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public Object id() {
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this.key);
    }

    @Override
    public Set<String> keys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}