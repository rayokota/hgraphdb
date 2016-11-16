package io.hgraphdb;

public class VertexLabel extends ElementLabel {

    public VertexLabel(String label, ValueType idType, Object... propertyKeysAndTypes) {
        super(label, idType, propertyKeysAndTypes);
    }

    @Override
    public String toString() {
        return "vertexLabel{" +
                "label='" + label() + '\'' +
                ", idType=" + idType() +
                ", propertyTypes=" + propertyTypes() +
                '}';
    }
}
