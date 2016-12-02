package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HBaseCounterTest extends HBaseGraphTest {

    @Override
    protected HBaseGraphConfiguration generateGraphConfig(String graphName) {
        HBaseGraphConfiguration config = super.generateGraphConfig(graphName);
        config.setElementCacheMaxSize(0);
        config.setRelationshipCacheMaxSize(0);
        config.setLazyLoading(true);
        config.setStaleIndexExpiryMs(0);
        config.setUseSchema(true);
        return config;
    }

    @Test
    public void testVertexCounter() {
        assertEquals(0, count(graph.vertices()));

        graph.createLabel(ElementType.VERTEX, "b", ValueType.LONG, "key2", ValueType.COUNTER);

        HBaseVertex v1 = (HBaseVertex) graph.addVertex(T.id, 10L, T.label, "b", "key2", 11L);

        Iterator<Vertex> it = graph.allVertices("b", "key2", 11L);
        assertEquals(1, count(it));

        v1.incrementProperty("key2", 1L);

        it = graph.allVertices("b", "key2", 12L);
        assertEquals(1, count(it));

        v1.incrementProperty("key2", -2L);

        it = graph.allVertices("b", "key2", 10L);
        assertEquals(1, count(it));
    }

    @Test
    public void testEdgeCounter() {
        assertEquals(0, count(graph.vertices()));

        graph.createLabel(ElementType.VERTEX, "a", ValueType.STRING, "key0", ValueType.COUNTER);
        graph.createLabel(ElementType.VERTEX, "b", ValueType.STRING, "key1", ValueType.COUNTER);
        graph.createLabel(ElementType.VERTEX, "c", ValueType.STRING, "key2", ValueType.COUNTER);
        graph.createLabel(ElementType.VERTEX, "d", ValueType.STRING, "key3", ValueType.COUNTER);

        HBaseVertex v1 = (HBaseVertex) graph.addVertex(T.id, id(10), T.label, "a", "key0", 10L);
        HBaseVertex v2 = (HBaseVertex) graph.addVertex(T.id, id(11), T.label, "b", "key1", 11L);
        HBaseVertex v3 = (HBaseVertex) graph.addVertex(T.id, id(12), T.label, "c", "key2", 12L);
        HBaseVertex v4 = (HBaseVertex) graph.addVertex(T.id, id(13), T.label, "d", "key3", 13L);

        graph.createLabel(ElementType.EDGE, "knows", ValueType.STRING, "key4", ValueType.COUNTER);
        graph.connectLabels("a", "knows", "b");

        HBaseEdge e = (HBaseEdge) graph.addEdge(v1, v2, "knows", "key4", 14L);

        Iterator<Edge> it = v1.edges(Direction.OUT, "knows", "key4", 14L);
        assertEquals(1, count(it));

        e.incrementProperty("key4", 1L);

        it = v1.edges(Direction.OUT, "knows", "key4", 15L);
        assertEquals(1, count(it));

        e.incrementProperty("key4", -2L);

        it = v1.edges(Direction.OUT, "knows", "key4", 13L);
        assertEquals(1, count(it));
    }
}
