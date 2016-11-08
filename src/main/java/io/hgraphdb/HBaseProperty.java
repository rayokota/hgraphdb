package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public final class HBaseProperty<V> implements Property<V> {

    protected final HBaseGraph graph;
    protected final HBaseElement element;
    protected final String key;
    protected final V value;

    public HBaseProperty(final HBaseGraph graph, final HBaseElement element, final String key, final V value) {
        this.graph = graph;
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        element.removeProperty(this.key);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}