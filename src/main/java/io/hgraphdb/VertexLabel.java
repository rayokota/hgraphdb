package io.hgraphdb;

import java.util.Map;

public class VertexLabel extends ElementLabel {

    public VertexLabel(String label, ValueType idType, Long createdAt, Map<String, ValueType> propertyTypes) {
        super(label, idType, createdAt, propertyTypes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VERTEX LABEL " + label() + "(ID: " + idType());
        propertyTypes().entrySet().forEach(entry ->
                sb.append(", ").append(entry.getKey()).append(": ").append(entry.getValue().toString()));
        sb.append(")");
        return sb.toString();
    }
}
