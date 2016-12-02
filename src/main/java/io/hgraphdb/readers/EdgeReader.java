package io.hgraphdb.readers;

import io.hgraphdb.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeReader extends ElementReader<Edge> {

    public EdgeReader(HBaseGraph graph) {
        super(graph);
    }

    @Override
    public Edge parse(Result result) {
        Object id = ValueUtils.deserializeWithSalt(result.getRow());
        Edge edge = graph.findOrCreateEdge(id);
        load(edge, result);
        return edge;
    }

    @Override
    public void load(Edge edge, Result result) {
        if (result.isEmpty()) {
            throw new HBaseGraphNotFoundException(edge, "Edge does not exist: " + edge.id());
        }
        Object inVertexId = null;
        Object outVertexId = null;
        String label = null;
        Long createdAt = null;
        Long updatedAt = null;
        Map<String, byte[]> rawProps = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                rawProps.put(key, CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.TO)) {
                inVertexId = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.FROM)) {
                outVertexId = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.LABEL)) {
                label = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        final String labelStr = label;
        Map<String, Object> props = rawProps.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> ValueUtils.deserializePropertyValue(graph, ElementType.EDGE, labelStr, e.getKey(), e.getValue())));
        if (inVertexId != null && outVertexId != null && label != null) {
            HBaseEdge newEdge = new HBaseEdge(graph, edge.id(), label, createdAt, updatedAt, props,
                    graph.findOrCreateVertex(inVertexId),
                    graph.findOrCreateVertex(outVertexId));
            ((HBaseEdge) edge).copyFrom(newEdge);
        } else {
            throw new IllegalStateException("Unable to parse edge from cells");
        }
    }
}
