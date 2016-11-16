package io.hgraphdb.readers;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphNotFoundException;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.ValueUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

public class VertexReader extends ElementReader<Vertex> {

    public VertexReader(HBaseGraph graph) {
        super(graph);
    }

    @Override
    public Vertex parse(Result result) {
        Object id = ValueUtils.deserializeWithSalt(result.getRow());
        Vertex vertex = graph.findOrCreateVertex(id);
        load(vertex, result);
        return vertex;
    }

    @Override
    public void load(Vertex vertex, Result result) {
        if (result.isEmpty()) {
            throw new HBaseGraphNotFoundException(vertex, "Vertex does not exist: " + vertex.id());
        }
        String label = null;
        Long createdAt = null;
        Long updatedAt = null;
        Map<String, Object> props = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                Object value = ValueUtils.deserialize(CellUtil.cloneValue(cell));
                props.put(key, value);
            } else if (key.equals(Constants.LABEL)) {
                label = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        HBaseVertex newVertex = new HBaseVertex(graph, vertex.id(), label, createdAt, updatedAt, props);
        ((HBaseVertex) vertex).copyFrom(newVertex);
    }
}
