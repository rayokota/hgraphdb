package io.hgraphdb.readers;

import io.hgraphdb.Constants;
import io.hgraphdb.ElementType;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.google.common.collect.MapMaker;

public class VertexReader extends LoadingElementReader<Vertex> {

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
        Map<String, byte[]> rawProps = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                rawProps.put(key, CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.LABEL)) {
                label = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }
        final String labelStr = label;
        Map<String, Collection<Object>> props = new MapMaker().concurrencyLevel(1).makeMap();
        for (Map.Entry<String, byte[]> entry : rawProps.entrySet()) {
            String key = entry.getKey();
            Object deserialized = ValueUtils.deserializePropertyValue(graph, ElementType.VERTEX, labelStr, key, entry.getValue());
            props.put(key, new LinkedList<>());
            if (deserialized instanceof Iterable) {
                for (Object item : (Iterable)deserialized) {
                    props.get(key).add(item);
                }
            } else {
                props.get(key).add(deserialized);
            }
        }
        HBaseVertex newVertex = new HBaseVertex(graph, vertex.id(), label, createdAt, updatedAt, props);
        ((HBaseVertex) vertex).copyFrom(newVertex);
    }
}
