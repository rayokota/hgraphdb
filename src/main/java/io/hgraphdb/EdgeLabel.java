package io.hgraphdb;

public class EdgeLabel extends ElementLabel {

    private String outVertexLabel;
    private String inVertexLabel;

    public EdgeLabel(String label, String outVertexLabel, String inVertexLabel, ValueType idType,
                     Integer ttl, Object... propertyKeysAndTypes) {
        super(label, idType, ttl, propertyKeysAndTypes);
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
        return "EdgeLabel{" +
                "label='" + label() + '\'' +
                ", outVertexLabel=" + outVertexLabel() +
                ", inVertexLabel=" + inVertexLabel() +
                ", idType=" + idType() +
                ", ttl=" + ttl() +
                ", propertyKeys=" + propertyKeys() +
                '}';
    }
}
