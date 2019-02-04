package io.hgraphdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.*;

public class HBaseElementTest extends HBaseGraphTest {

    @Test
    public void testNonStringIds() throws Exception {
        Object[] ids = new Object[]{
                10, 20, 30L, 40L,
                50.0f, 60.0f, 70.0d, 80.0d,
                BigDecimal.valueOf(90.01d), BigDecimal.valueOf(95.01d),
                LocalDate.now(), LocalTime.now(), LocalDateTime.now(), Duration.ofNanos(100),
                new byte[]{'a', 'b', 'c'},
                new String[]{"hello", "world"},
                ImmutableMap.of("a", "b"),
                ElementType.VERTEX,
                (byte) 'a', (byte) 'b', 'c', 'd',
                "str1", "str2",
                new UUID(1L, 2L), new UUID(3L, 4L),
                new KryoObject(ValueType.BINARY), new KryoObject(ValueType.BOOLEAN),
                new GenericObject("str3"), new GenericObject("str4"),
        };

        Object[] edgeIds = new Object[]{
                100, 200, 300L, 400L,
                500.0f, 600.0f, 700.0d, 800.0d,
                BigDecimal.valueOf(900.01d), BigDecimal.valueOf(950.01d),
                LocalDate.now(), LocalTime.now(), LocalDateTime.now(), Duration.ofNanos(1000),
                new byte[]{'e', 'f', 'g'},
                new String[]{"goodbye", "friends"},
                ImmutableMap.of("e", "f"),
                ElementType.EDGE,
                (byte) 'e', (byte) 'f', 'g', 'h',
                "str5", "str6",
                new UUID(5L, 6L), new UUID(7L, 8L),
                new KryoObject(ValueType.DATE), new KryoObject(ValueType.TIME),
                new GenericObject("str7"), new GenericObject("str8"),
        };

        for (Object id : ids) {
            assertFalse("Id " + id + " already exists", graph.vertices(id).hasNext());
            Vertex v = graph.addVertex(T.id, id);
            assertNotNull(v);
            assertTrue(graph.vertices(id).hasNext());
        }
        assertEquals(ids.length, count(graph.vertices()));

        for (int i = 1; i < edgeIds.length; i++) {
            assertFalse(graph.edges(edgeIds[i - 1]).hasNext());
            Edge e = graph.addEdge(
                    graph.vertices(ids[i - 1]).next(),
                    graph.vertices(ids[i]).next(), "label",
                    T.id, edgeIds[i - 1]);
            assertNotNull(e);
            assertTrue(graph.edges(edgeIds[i - 1]).hasNext());
        }
        assertEquals(edgeIds.length - 1, count(graph.edges()));
    }

    @Test
    public void testEmptyProperties() {
        assertEquals(0, count(graph.vertices()));
        for (int i = 0; i < 10; i++) {
            graph.addVertex(T.id, id(i));
        }

        Vertex v = graph.vertex(id(1));
        assertEquals(0, count(v.properties("foo")));
    }

    @Test
    public void testAllVerticesWithLabel() {
        assertEquals(0, count(graph.vertices()));
        graph.addVertex(T.id, id(0), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(1), T.label, "a", "key1", 2);
        graph.addVertex(T.id, id(2), T.label, "a", "key2", 2);
        graph.addVertex(T.id, id(3), T.label, "a", "key1", 1);
        graph.addVertex(T.id, id(4), T.label, "b", "key1", 1);

        Iterator<Vertex> it = graph.verticesByLabel("a");
        assertEquals(4, count(it));

        it = graph.verticesByLabel("a", "key1", 1);
        assertEquals(2, count(it));
    }

    @Test
    public void testVertexMultiProperty() {
        assertEquals(0, count(graph.vertices()));
        Vertex vertex = graph.addVertex(T.id, id(0), T.label, "a", "key1", "value1");
        int expect = 2;

        vertex.property(Cardinality.set, "key1", "value2");
        assertEquals(expect, count(vertex.properties("key1")));

        vertex.property(Cardinality.set, "key1", "value1");
        assertEquals(expect, count(vertex.properties("key1")));

        vertex.property(Cardinality.set, "key1", "value3");
        expect++;
        assertEquals(expect, count(vertex.properties("key1")));
    }

    @Test
    public void testCountersNotSupportedWithoutSchema() {
        assertEquals(0, count(graph.vertices()));

        HBaseVertex v1 = (HBaseVertex) graph.addVertex(T.id, 10L, T.label, "b", "key2", 11L);

        try {
            v1.incrementProperty("key2", 1L);
            fail("should reject counter op");
        } catch (HBaseGraphNoSchemaException ignored) {
        }
    }

    private static class KryoObject implements KryoSerializable {
        private ValueType id;

        public KryoObject() {
        }

        public KryoObject(ValueType id) {
            this.id = id;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.write(id.getCode());
        }

        @Override
        public void read(Kryo kryo, Input input) {
            id = ValueType.valueOf(input.readByte());
        }

        @Override
        public String toString() {
            return "KryoObject [id=" + id + "]";
        }
    }

    private static class GenericObject implements Serializable {
        private final Object id;

        public GenericObject(Object id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "GenericObject [id=" + id + "]";
        }
    }
}
