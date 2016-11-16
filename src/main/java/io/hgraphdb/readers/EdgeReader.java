package io.hgraphdb.readers;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphNotFoundException;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

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
        Map<String, Object> props = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                Object value = ValueUtils.deserialize(CellUtil.cloneValue(cell));
                props.put(key, value);
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
