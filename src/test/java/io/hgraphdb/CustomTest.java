package io.hgraphdb;

import io.hgraphdb.testclassification.SlowTests;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.junit.Assert.*;

@Category(SlowTests.class)
public class CustomTest extends AbstractGremlinProcessTest {

    @Ignore
    @Test
    public void shouldNotGetConcurrentModificationException() {
        for (int i = 0; i < 25; i++) {
            graph.addVertex("myId", i);
        }
        graph.vertices().forEachRemaining(v -> graph.vertices().forEachRemaining(u -> v.addEdge("knows", u, "myEdgeId", 12)));

        tryCommit(graph, getAssertVertexEdgeCounts(25, 625));

        final List<Vertex> vertices = new ArrayList<>();
        IteratorUtils.fill(graph.vertices(), vertices);
        for (Vertex v : vertices) {
            v.remove();
            tryCommit(graph);
        }

        tryCommit(graph, getAssertVertexEdgeCounts(0, 0));
    }

    @Ignore
    @Test
    public void shouldNotHaveAConcurrentModificationExceptionWhenIteratingAndRemovingAddingEdges() {
        final Vertex v1 = graph.addVertex("name", "marko");
        final Vertex v2 = graph.addVertex("name", "puppy");
        v1.addEdge("knows", v2, "since", 2010);
        v1.addEdge("pets", v2);
        v1.addEdge("walks", v2, "location", "arroyo");
        v2.addEdge("knows", v1, "since", 2010);
        assertEquals(4L, IteratorUtils.count(v1.edges(Direction.BOTH)));
        assertEquals(4L, IteratorUtils.count(v2.edges(Direction.BOTH)));
        v1.edges(Direction.BOTH).forEachRemaining(edge -> {
            v1.addEdge("livesWith", v2);
            v1.addEdge("walks", v2, "location", "river");
            edge.remove();
        });
        v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);
        assertEquals(0, IteratorUtils.count(v1.edges(Direction.BOTH)));
        assertEquals(0, IteratorUtils.count(v2.edges(Direction.BOTH)));
    }

    @Ignore
    @Test
    public void shouldEvaluateConnectivityPatterns() {
        final Vertex a;
        final Vertex b;
        final Vertex c;
        final Vertex d;
        if (graph.features().vertex().supportsUserSuppliedIds()) {
            a = graph.addVertex(T.id, graphProvider.convertId("1", Vertex.class));
            b = graph.addVertex(T.id, graphProvider.convertId("2", Vertex.class));
            c = graph.addVertex(T.id, graphProvider.convertId("3", Vertex.class));
            d = graph.addVertex(T.id, graphProvider.convertId("4", Vertex.class));
        } else {
            a = graph.addVertex();
            b = graph.addVertex();
            c = graph.addVertex();
            d = graph.addVertex();
        }

        tryCommit(graph, getAssertVertexEdgeCounts(4, 0));

        final Edge e = a.addEdge(graphProvider.convertLabel("knows"), b);
        final Edge f = b.addEdge(graphProvider.convertLabel("knows"), c);
        final Edge g = c.addEdge(graphProvider.convertLabel("knows"), d);
        final Edge h = d.addEdge(graphProvider.convertLabel("knows"), a);

        tryCommit(graph, getAssertVertexEdgeCounts(4, 4));

        graph.vertices().forEachRemaining(v -> {
            assertEquals(1L, IteratorUtils.count(v.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(v.edges(Direction.IN)));
        });

        graph.edges().forEachRemaining(x ->
            assertEquals(graphProvider.convertLabel("knows"), x.label())
        );

        if (graph.features().vertex().supportsUserSuppliedIds()) {
            final Vertex va = graph.vertices(graphProvider.convertId("1", Vertex.class)).next();
            final Vertex vb = graph.vertices(graphProvider.convertId("2", Vertex.class)).next();
            final Vertex vc = graph.vertices(graphProvider.convertId("3", Vertex.class)).next();
            final Vertex vd = graph.vertices(graphProvider.convertId("4", Vertex.class)).next();

            assertEquals(a, va);
            assertEquals(b, vb);
            assertEquals(c, vc);
            assertEquals(d, vd);

            assertEquals(1L, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vd.edges(Direction.OUT)));

            final Edge i = a.addEdge(graphProvider.convertLabel("hates"), b);

            assertEquals(1L, IteratorUtils.count(va.edges(Direction.IN)));
            assertEquals(2L, IteratorUtils.count(va.edges(Direction.OUT)));
            assertEquals(2L, IteratorUtils.count(vb.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vb.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(vc.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vc.edges(Direction.OUT)));
            assertEquals(1L, IteratorUtils.count(vd.edges(Direction.IN)));
            assertEquals(1L, IteratorUtils.count(vd.edges(Direction.OUT)));

            for (Edge x : IteratorUtils.list(a.edges(Direction.OUT))) {
                assertTrue(x.label().equals(graphProvider.convertLabel("knows")) || x.label().equals(graphProvider.convertLabel("hates")));
            }

            assertEquals(graphProvider.convertLabel("hates"), i.label());
            assertEquals(graphProvider.convertId("2", Vertex.class).toString(), i.inVertex().id().toString());
            assertEquals(graphProvider.convertId("1", Vertex.class).toString(), i.outVertex().id().toString());
        }

        final Set<Object> vertexIds = new HashSet<>();
        vertexIds.add(a.id());
        vertexIds.add(a.id());
        vertexIds.add(b.id());
        vertexIds.add(b.id());
        vertexIds.add(c.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        vertexIds.add(d.id());
        assertEquals(4, vertexIds.size());
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName(), MODERN);
        graphProvider.clear(config);
        Graph subgraph = graphProvider.openTestGraph(config);
        /////
        final Traversal<Vertex, String> traversal = get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(subgraph);
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
        subgraph = traversal.asAdmin().getSideEffects().get("sg");
        assertVertexEdgeCounts(subgraph, 5, 4);

        graphProvider.clear(subgraph, config);
    }

    public Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph) {
        return g.withSideEffect("sg", () -> subgraph).V().repeat(bothE("created").subgraph("sg").outV()).times(5).<String>values("name").dedup();
    }

    public static <A> GraphTraversal<A, Edge> bothE(final String... edgeLabels) {
        return __.<A>start().bothE(edgeLabels);
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXknowsAsStringIdX() {
        //final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX(convertToVertexId("marko").toString());
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX(convertToVertexId("marko"));
        assert_g_v1_outXknowsX(traversal);
    }

    public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
        return g.V(v1Id).out("knows");
    }

    private void assert_g_v1_outXknowsX(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        printTraversalForm2(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    public void printTraversalForm2(final Traversal traversal) {
        System.out.println(String.format("Testing: %s", name.getMethodName()));
        System.out.println("   pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("  post-strategy:" + traversal);
        verifyUniqueStepIds(traversal.asAdmin());
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX() throws Exception {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(final Object v1Id) {
        return g.V(v1Id)
                .outE("knows")
                .has("weight", 1.0d).as("here").inV().has("name", "josh").select("here");
    }

    private static void assertCommonB(final Traversal<Vertex, Edge> traversal) {
        assertTrue(traversal.hasNext());
        assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    public String ioType;

    public final Io.Builder ioBuilderToTest = GraphSONIo.build(GraphSONVersion.V2_0);

    public boolean assertDouble = true;

    public final boolean lossyForId = true;

    public String fileExtension = ".json";

    @Ignore
    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteModern() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ((HBaseGraph) graph).createIndex(ElementType.EDGE, "knows", "weight");
            final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            ((HBaseGraph) g1).createIndex(ElementType.EDGE, "knows", "weight");
            final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
            //((HBaseGraph) graph).dump();
            //((HBaseGraph) g1).dump();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            // modern uses double natively so always assert as such
            IoTest.assertModernGraph(g1, true, lossyForId);

            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    public void shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter() {
        final Vertex a = graph.addVertex();
        final Vertex b = graph.addVertex();
        final Vertex c = graph.addVertex();

        final String labelFriend = graphProvider.convertLabel("friend");
        final String labelHate = graphProvider.convertLabel("hate");

        final Edge aFriendB = a.addEdge(labelFriend, b);
        final Edge aFriendC = a.addEdge(labelFriend, c);
        final Edge aHateC = a.addEdge(labelHate, c);
        final Edge cHateA = c.addEdge(labelHate, a);
        final Edge cHateB = c.addEdge(labelHate, b);

        List<Edge> results = IteratorUtils.list(a.edges(Direction.OUT, labelFriend, labelHate));
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = IteratorUtils.list(a.edges(Direction.IN, labelFriend, labelHate));
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = IteratorUtils.list(b.edges(Direction.IN, labelFriend, labelHate));
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = IteratorUtils.list(b.edges(Direction.IN, graphProvider.convertLabel("blah1"), graphProvider.convertLabel("blah2")));
        assertEquals(0, results.size());
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXboth_simplePathX_timesX3X_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_repeatXboth_simplePathX_timesX3X_path();
        printTraversalForm(traversal);
        int counter = 0;
        String s = null;
        while (traversal.hasNext()) {
            counter++;
            assertTrue(traversal.next().isSimple());
        }
        assertEquals(18, counter);
        assertFalse(traversal.hasNext());
    }

    public Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path() {
        return g.V().repeat(both().simplePath()).times(3).path();
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_hasLabelXpersonX_order_byXage_decrX_limitX5X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_hasLabelXpersonX_order_byXage_decrX_limitX5X_name();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList("peter", "josh", "josh", "josh", "marko"), traversal);
    }

    public Traversal<Vertex, String> get_g_V_both_hasLabelXpersonX_order_byXage_decrX_limitX5X_name() {
        return g.V().both().hasLabel("person").order().by("age", Order.decr).limit(5).values("name");
    }

    @Ignore
    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Path path = traversal.next();
        assertEquals(3, path.size());
        assertEquals("marko", path.get(0));
        assertEquals("lop", path.get(1));
        assertEquals("peter", path.get(2));
        assertFalse(traversal.hasNext());
    }

    public Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(final Object v1Id) {
        return g.V(v1Id).repeat(__.both().simplePath()).until(__.has("name", "peter").or().loops().is(2)).has("name", "peter").path().by("name");
    }
}