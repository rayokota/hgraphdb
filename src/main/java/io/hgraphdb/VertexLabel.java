package io.hgraphdb;

public class VertexLabel extends ElementLabel {

    public VertexLabel(String label, ValueType idType, Integer ttl, Object... propertyKeysAndTypes) {
        super(label, idType, ttl, propertyKeysAndTypes);
    }

    @Override
    public String toString() {
        return "VertexLabel{" +
                "label='" + label() + '\'' +
                ", idType=" + idType() +
                ", ttl=" + ttl() +
                ", propertyKeys=" + propertyKeys() +
                '}';
    }
}
