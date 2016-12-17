package io.hgraphdb;

public class LabelConnection {

    private final String outVertexLabel;
    private final String edgeLabel;
    private final String inVertexLabel;
    private final Long createdAt;

    public LabelConnection(String outVertexLabel, String edgeLabel, String inVertexLabel, Long createdAt) {
        this.outVertexLabel = outVertexLabel;
        this.edgeLabel = edgeLabel;
        this.inVertexLabel = inVertexLabel;
        this.createdAt = createdAt;
    }

    public String outVertexLabel() {
        return outVertexLabel;
    }

    public String edgeLabel() {
        return edgeLabel;
    }

    public String inVertexLabel() {
        return inVertexLabel;
    }

    public Long createdAt() {
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LabelConnection that = (LabelConnection) o;

        if (!outVertexLabel.equals(that.outVertexLabel)) return false;
        if (!edgeLabel.equals(that.edgeLabel)) return false;
        return inVertexLabel.equals(that.inVertexLabel);
    }

    @Override
    public int hashCode() {
        int result = outVertexLabel.hashCode();
        result = 31 * result + edgeLabel.hashCode();
        result = 31 * result + inVertexLabel.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LABEL CONNECTION [" + outVertexLabel + " - " + edgeLabel + " -> " + inVertexLabel + "]";
    }
}
