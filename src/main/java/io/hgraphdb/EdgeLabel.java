package io.hgraphdb;

import java.util.Map;

public class EdgeLabel extends ElementLabel {

    private String outVertexLabel;
    private String inVertexLabel;

    public EdgeLabel(String label, String outVertexLabel, String inVertexLabel, ValueType idType,
                     Long createdAt, Map<String, ValueType> propertyTypes) {
        super(label, idType, createdAt, propertyTypes);
        this.outVertexLabel = outVertexLabel;
        this.inVertexLabel = inVertexLabel;
    }

    public String outVertexLabel() {
        return outVertexLabel;
    }

    public String inVertexLabel() {
        return inVertexLabel;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EDGE LABEL [" + outVertexLabel + " - " + label() + " -> " + inVertexLabel
                + "](ID: " + idType());
        propertyTypes().entrySet().forEach(entry ->
                sb.append(", ").append(entry.getKey()).append(": ").append(entry.getValue().toString()));
        sb.append(")");
        return sb.toString();
    }
}
