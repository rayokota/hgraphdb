package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class HBaseBulkLoaderTest extends HBaseGraphTest {

    @Test
    public void testBulkLoader() throws Exception {
        HBaseBulkLoader loader = new HBaseBulkLoader(graph);

        Vertex v1 = loader.addVertex(T.id, "A");
        Vertex v2 = loader.addVertex(T.id, "B", "P1", "V1", "P2", "2");
        loader.setProperty(v2, "P4", "4");
        Edge e = loader.addEdge(v1, v2, "edge", "P3", "V3");
        loader.setProperty(e, "P5", "5");
        loader.close();

        v1 = graph.vertex("A");
        assertNotNull(v1);

        Iterator<Edge> it = v1.edges(Direction.OUT);
        assertTrue(it.hasNext());

        e = it.next();
        assertEquals("edge", e.label());
        assertEquals("V3", e.property("P3").value());
        assertEquals("5", e.property("P5").value());

        v2 = e.inVertex();
        assertEquals("B", v2.id());
        assertEquals("V1", v2.property("P1").value());
        assertEquals("2", v2.property("P2").value());
        assertEquals("4", v2.property("P4").value());
    }

}
