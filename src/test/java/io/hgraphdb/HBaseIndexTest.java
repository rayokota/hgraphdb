package io.hgraphdb;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HBaseIndexTest extends HBaseGraphTest {

    @Override
    protected HBaseGraphConfiguration generateGraphConfig(String graphName) {
        HBaseGraphConfiguration config = super.generateGraphConfig(graphName);
        config.setElementCacheMaxSize(0);
        config.setRelationshipCacheMaxSize(0);
        config.setLazyLoading(true);
        config.setStaleIndexExpiryMs(0);
        return config;
    }

    @Test
    public void testVertexIndex() {
        assertEquals(0, count(graph.vertices()));
        graph.addVertex(T.id, id(0), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(1), T.label, "a", "key1", 2);
        graph.addVertex(T.id, id(2), T.label, "a", "key2", 2);
        graph.addVertex(T.id, id(3), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(4), T.label, "b", "key1", 1);

        Iterator<Vertex> it = graph.allVertices("a", "key1", 1);
        assertEquals(2, count(it));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(12), T.label, "a", "key2", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(14), T.label, "b", "key1", 11);

        it = graph.allVertices("a", "key1", 11);
        assertEquals(2, count(it));
    }

    @Test
    public void testUniqueVertexIndex() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1", true);
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
        Iterator<Vertex> it = graph.allVertices("a", "key1", 11);
        Vertex v = it.next();
        assertEquals(id(10), v.id());

        try {
            graph.addVertex(T.id, id(11), T.label, "a", "key1", 11);
            fail("should reject non-unique key");
        } catch (HBaseGraphNotUniqueException x) { }

        it = graph.allVertices("a", "key1", 11);
        v = it.next();
        assertEquals(id(10), v.id());
    }

    @Test
    public void testVertexIndexRange() {
        assertEquals(0, count(graph.vertices()));
        graph.addVertex(T.id, id(0), T.label, "a", "key1", 0);
        graph.addVertex(T.id, id(1), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(2), T.label, "a", "key1", 2);
        graph.addVertex(T.id, id(3), T.label, "a", "key1", 3);
        graph.addVertex(T.id, id(4), T.label, "a", "key1", 4);
        graph.addVertex(T.id, id(5), T.label, "a", "key1", 5);

        Iterator<Vertex> it = graph.allVertices("a", "key1", 1, 4);
        assertEquals(3, count(it));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 10);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(12), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 13);
        graph.addVertex(T.id, id(14), T.label, "a", "key1", 14);
        graph.addVertex(T.id, id(15), T.label, "a", "key1", 15);

        it = graph.allVertices("a", "key1", 11, 14);
        assertEquals(3, count(it));
    }

    @Test
    public void testVertexIndexLimit() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.addVertex(T.id, id(10), T.label, "a", "key1", 10);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", 11);
        graph.addVertex(T.id, id(12), T.label, "a", "key1", 12);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 13);
        graph.addVertex(T.id, id(14), T.label, "a", "key1", 14);
        graph.addVertex(T.id, id(15), T.label, "a", "key1", 15);
        graph.addVertex(T.id, id(16), T.label, "a", "key1", 16);
        graph.addVertex(T.id, id(17), T.label, "a", "key1", 17);

        Iterator<Vertex> it = graph.allVerticesWithLimit("a", "key1", null, 3, false);
        assertEquals(3, count(it));

        it = graph.allVerticesWithLimit("a", "key1", 11, 3, false);
        assertEquals(3, count(it));

        it = graph.allVerticesWithLimit("a", "key1", 12, 3, true);
        it.forEachRemaining(System.out::println);
        //assertEquals(2, count(it));  // 11 and 10
    }

    @Test
    public void testVertexIndexRangeNegative() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.addVertex(T.id, id(10), T.label, "a", "key1", -3);
        graph.addVertex(T.id, id(11), T.label, "a", "key1", -2);
        graph.addVertex(T.id, id(12), T.label, "a", "key1", -1);
        graph.addVertex(T.id, id(13), T.label, "a", "key1", 0);
        graph.addVertex(T.id, id(14), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(15), T.label, "a", "key1", 2);

        Iterator<Vertex> it = graph.allVertices("a", "key1", -2, 2);
        assertEquals(4, count(it));
    }

    @Test
    public void testEdgeIndex() {
        assertEquals(0, count(graph.vertices()));
        Vertex v0 = graph.addVertex(T.id, id(0));
        Vertex v1 = graph.addVertex(T.id, id(1));
        Vertex v2 = graph.addVertex(T.id, id(2));
        Vertex v3 = graph.addVertex(T.id, id(3));
        v0.addEdge("b", v1, "key1", 1);
        v0.addEdge("b", v2, "key1", 1);
        v0.addEdge("b", v3, "key1", 2);

        Iterator<Edge> it = ((HBaseVertex) v0).edges(Direction.OUT, "b", "key1", 1);
        assertEquals(2, count(it));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));
        v10.addEdge("b", v11, "key1", 11);
        v10.addEdge("b", v12, "key1", 11);
        v10.addEdge("b", v13, "key1", 12);

        it = ((HBaseVertex) v10).edges(Direction.OUT, "b", "key1", 11);
        assertEquals(2, count(it));
    }

    @Test
    public void testUniqueEdgeIndex() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1", true);
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));

        v10.addEdge("b", v11, "key1", 11);

        Iterator<Edge> it = ((HBaseVertex) v10).edges(Direction.OUT, "b", "key1", 11);
        Edge e = it.next();
        assertEquals(id(11), e.inVertex().id());

        try {
            v10.addEdge("b", v12, "key1", 11);
            fail("should reject non-unique key");
        } catch (HBaseGraphNotUniqueException x) { }

        it = ((HBaseVertex) v10).edges(Direction.OUT, "b", "key1", 11);
        e = it.next();
        assertEquals(id(11), e.inVertex().id());
    }

    @Test
    public void testEdgeIndexRange() {
        assertEquals(0, count(graph.vertices()));
        Vertex v0 = graph.addVertex(T.id, id(0));
        Vertex v1 = graph.addVertex(T.id, id(1));
        Vertex v2 = graph.addVertex(T.id, id(2));
        Vertex v3 = graph.addVertex(T.id, id(3));
        Vertex v4 = graph.addVertex(T.id, id(4));
        Vertex v5 = graph.addVertex(T.id, id(5));
        Vertex v6 = graph.addVertex(T.id, id(6));
        v0.addEdge("b", v1, "key1", 1);
        v0.addEdge("b", v2, "key1", 2);
        v0.addEdge("b", v3, "key1", 3);
        v0.addEdge("b", v4, "key1", 4);
        v0.addEdge("b", v5, "key1", 5);
        v0.addEdge("b", v6, "key1", 6);

        Iterator<Edge> it = ((HBaseVertex) v0).edges(Direction.OUT, "b", "key1", 2, 6);
        assertEquals(4, count(it));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));
        Vertex v14 = graph.addVertex(T.id, id(14));
        Vertex v15 = graph.addVertex(T.id, id(15));
        Vertex v16 = graph.addVertex(T.id, id(16));
        v10.addEdge("b", v11, "key1", 11);
        v10.addEdge("b", v12, "key1", 12);
        v10.addEdge("b", v13, "key1", 13);
        v10.addEdge("b", v14, "key1", 14);
        v10.addEdge("b", v15, "key1", 15);
        v10.addEdge("b", v16, "key1", 16);

        it = ((HBaseVertex) v10).edges(Direction.OUT, "b", "key1", 12, 16);
        assertEquals(4, count(it));
    }

    @Test
    public void testEdgeIndexLimit() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));
        Vertex v14 = graph.addVertex(T.id, id(14));
        Vertex v15 = graph.addVertex(T.id, id(15));
        Vertex v16 = graph.addVertex(T.id, id(16));
        v10.addEdge("b", v11, "key1", 10);
        v10.addEdge("b", v11, "key1", 11);
        v10.addEdge("b", v12, "key1", 12);
        v10.addEdge("b", v13, "key1", 13);
        v10.addEdge("b", v14, "key1", 14);
        v10.addEdge("b", v15, "key1", 15);
        v10.addEdge("b", v16, "key1", 16);

        Iterator<Edge> it = ((HBaseVertex) v10).edgesWithLimit(Direction.OUT, "b", "key1", null, 4, false);
        assertEquals(4, count(it));

        it = ((HBaseVertex) v10).edgesWithLimit(Direction.OUT, "b", "key1", 12, 4, false);
        assertEquals(4, count(it));

        it = ((HBaseVertex) v10).edgesWithLimit(Direction.OUT, "b", "key1", 12, 4, true);
        assertEquals(2, count(it));  // 11 and 10
    }

    @Test
    public void testEdgeIndexRangeNegative() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));
        Vertex v14 = graph.addVertex(T.id, id(14));
        Vertex v15 = graph.addVertex(T.id, id(15));
        Vertex v16 = graph.addVertex(T.id, id(16));
        v10.addEdge("b", v11, "key1", -3);
        v10.addEdge("b", v12, "key1", -2);
        v10.addEdge("b", v13, "key1", -1);
        v10.addEdge("b", v14, "key1", 0);
        v10.addEdge("b", v15, "key1", 1);
        v10.addEdge("b", v16, "key1", 2);

        Iterator<Edge> it = ((HBaseVertex) v10).edges(Direction.OUT, "b", "key1", -2, 2);
        assertEquals(4, count(it));
    }

    @Test
    public void testIndexRemoval() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v0 = graph.addVertex(T.id, id(0));
        Vertex v1 = graph.addVertex(T.id, id(1));
        Vertex v2 = graph.addVertex(T.id, id(2));
        Vertex v3 = graph.addVertex(T.id, id(3));
        Vertex v4 = graph.addVertex(T.id, id(4));
        Vertex v5 = graph.addVertex(T.id, id(5));
        Vertex v6 = graph.addVertex(T.id, id(6));
        v0.addEdge("b", v1, "key1", 1);
        Edge edge = v0.addEdge("b", v2, "key1", 2);
        v0.addEdge("b", v3, "key1", 3);
        v0.addEdge("b", v4, "key1", 4);
        v0.addEdge("b", v5, "key1", 5);
        v0.addEdge("b", v6, "key1", 6);

        Iterator<Edge> it = ((HBaseVertex) v0).edges(Direction.OUT, "b", "key1", 2, 6);
        assertEquals(4, count(it));

        graph.removeEdge(edge);

        it = ((HBaseVertex) v0).edges(Direction.OUT, "b", "key1", 2, 6);
        assertEquals(3, count(it));
    }

    @Test
    public void testVertexIndexModifyingProperty() {
        assertEquals(0, count(graph.vertices()));
        graph.createIndex(ElementType.VERTEX, "a", "key1");
        Vertex v = graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);

        Iterator<Vertex> it = graph.allVertices("a", "key1", 11);
        assertEquals(1, count(it));

        VertexProperty<Integer> p = v.property("key1", 11);

        it = graph.allVertices("a", "key1", 11);
        assertEquals(1, count(it));
        it = graph.allVertices("a", "key1", 12);
        assertEquals(0, count(it));

        p = v.property("key1", 12);

        it = graph.allVertices("a", "key1", 11);
        assertEquals(0, count(it));
        it = graph.allVertices("a", "key1", 12);
        assertEquals(1, count(it));

        p.remove();

        it = graph.allVertices("a", "key1", 11);
        assertEquals(0, count(it));
        it = graph.allVertices("a", "key1", 12);
        assertEquals(0, count(it));
    }

    @Test
    public void testEdgeIndexModifyingProperty() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        HBaseVertex v10 = (HBaseVertex) graph.addVertex(T.id, id(10));
        HBaseVertex v11 = (HBaseVertex) graph.addVertex(T.id, id(11));
        Edge e = v10.addEdge("b", v11, "key1", 11);

        Iterator<Edge> it = v10.edges(Direction.OUT, "b", "key1", 11);
        assertEquals(1, count(it));

        Property<Integer> p = e.property("key1", 11);

        it = v10.edges(Direction.OUT, "b", "key1", 11);
        assertEquals(1, count(it));
        it = v10.edges(Direction.OUT, "b", "key1", 12);
        assertEquals(0, count(it));

        p = e.property("key1", 12);

        it = v10.edges(Direction.OUT, "b", "key1", 11);
        assertEquals(0, count(it));
        it = v10.edges(Direction.OUT, "b", "key1", 12);
        assertEquals(1, count(it));

        p.remove();

        it = v10.edges(Direction.OUT, "b", "key1", 11);
        assertEquals(0, count(it));
        it = v10.edges(Direction.OUT, "b", "key1", 12);
        assertEquals(0, count(it));
    }

    @Test
    public void testGremlinVertexIndex() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.addVertex(T.id, id(0), T.label, "a", "key1", 0);
        graph.addVertex(T.id, id(1), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(2), T.label, "a", "key1", 2);
        graph.addVertex(T.id, id(3), T.label, "a", "key1", 3);
        graph.addVertex(T.id, id(4), T.label, "a", "key1", 4);
        graph.addVertex(T.id, id(5), T.label, "a", "key1", 5);

        GraphTraversalSource g = graph.traversal();
        Iterator<Vertex> it = g.V().has("a", "key1", 0);
        assertEquals(1, count(it));
    }

    @Test
    public void testGremlinEdgeIndex() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.EDGE, "b", "key1");
        Vertex v10 = graph.addVertex(T.id, id(10));
        Vertex v11 = graph.addVertex(T.id, id(11));
        Vertex v12 = graph.addVertex(T.id, id(12));
        Vertex v13 = graph.addVertex(T.id, id(13));
        v10.addEdge("b", v11, "key1", 11);
        v10.addEdge("b", v12, "key1", 11);
        v10.addEdge("b", v13, "key1", 12);

        GraphTraversalSource g = graph.traversal();
        Iterator<Edge> it = g.V(id(10)).outE().has("b", "key1", 11);
        assertEquals(2, count(it));
    }

    @Test
    public void testIndexExample() {
        assertEquals(0, count(graph.vertices()));

        graph.createIndex(ElementType.VERTEX, "a", "key1");
        graph.createIndex(ElementType.EDGE, "knows", "since");
        Vertex v0 = graph.addVertex(T.id, id(0), T.label, "person", "name", "John");
        Vertex v1 = graph.addVertex(T.id, id(1), T.label, "person", "name", "Kierkegaard");
        Vertex v2 = graph.addVertex(T.id, id(2), T.label, "person", "name", "Barth");
        Vertex v3 = graph.addVertex(T.id, id(3), T.label, "person", "name", "Brunner");
        Vertex v4 = graph.addVertex(T.id, id(4), T.label, "person", "name", "Niebuhr");
        Vertex v5 = graph.addVertex(T.id, id(5), T.label, "person", "name", "Tillich");

        v0.addEdge("knows", v1, "since", LocalDate.parse("2000-01-01"));
        v0.addEdge("knows", v2, "since", LocalDate.parse("2007-01-01"));
        v0.addEdge("knows", v3, "since", LocalDate.parse("2007-01-02"));
        v0.addEdge("knows", v4, "since", LocalDate.parse("2007-12-01"));
        v0.addEdge("knows", v5, "since", LocalDate.parse("2008-01-01"));

        Iterator<Vertex> it = graph.allVertices("person", "name", "John");
        HBaseVertex v = (HBaseVertex) it.next();

        Iterator<Edge> it2 = v.edges(Direction.OUT, "knows", "since",
                LocalDate.parse("2007-01-01"), LocalDate.parse("2008-01-01"));
        assertEquals(3, count(it2));
    }
}
