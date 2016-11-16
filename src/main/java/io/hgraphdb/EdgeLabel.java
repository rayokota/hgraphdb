package io.hgraphdb;

public class EdgeLabel extends ElementLabel {

    private String outVertexLabel;
    private String inVertexLabel;

    public EdgeLabel(String label, String outVertexLabel, String inVertexLabel, ValueType idType,
                     Object... propertyKeysAndTypes) {
        super(label, idType, propertyKeysAndTypes);
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
        return "edgeLabel{" +
                "label='" + label() + '\'' +
                ", idType=" + idType() +
                ", outVertexLabel=" + outVertexLabel() +
                ", inVertexLabel=" + inVertexLabel() +
                ", propertyTypes=" + propertyTypes() +
                '}';
    }
}
