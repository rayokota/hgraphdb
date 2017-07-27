package io.hgraphdb.gelly.examples;

import io.hgraphdb.*;
import io.hgraphdb.gelly.HBaseEdgeInputFormat;
import io.hgraphdb.gelly.HBaseVertexInputFormat;
import io.hgraphdb.testclassification.SlowTests;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.util.Collector;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(SlowTests.class)
public class NeighborhoodComputationTest extends HBaseGraphTest {

    @Test
    public void testNeighborhoodMethods() throws Exception {

        org.apache.tinkerpop.gremlin.structure.Vertex v1 = graph.addVertex(T.id, 1);
        org.apache.tinkerpop.gremlin.structure.Vertex v2 = graph.addVertex(T.id, 2);
        org.apache.tinkerpop.gremlin.structure.Vertex v3 = graph.addVertex(T.id, 3);
        org.apache.tinkerpop.gremlin.structure.Vertex v4 = graph.addVertex(T.id, 4);
        org.apache.tinkerpop.gremlin.structure.Vertex v5 = graph.addVertex(T.id, 5);
        v1.addEdge("e", v2, "weight", 0.1);
        v1.addEdge("e", v3, "weight", 0.5);
        v1.addEdge("e", v4, "weight", 0.4);
        v2.addEdge("e", v4, "weight", 0.7);
        v2.addEdge("e", v5, "weight", 0.3);
        v3.addEdge("e", v4, "weight", 0.2);
        v4.addEdge("e", v5, "weight", 0.9);

        HBaseGraphConfiguration hconf = graph.configuration();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, Long>> vertices = env.createInput(
                new HBaseVertexInputFormat<Long, Long>(hconf),
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }));
        DataSet<Tuple3<Long, Long, Double>> edges = env.createInput(
                new HBaseEdgeInputFormat<Long, Double>(hconf, "weight"),
                TypeInformation.of(new TypeHint<Tuple3<Long, Long, Double>>() {
                }));

        Graph<Long, Long, Double> gelly = Graph.fromTupleDataSet(vertices, edges, env);

        DataSet<Tuple2<Long, Double>> minWeights = gelly.reduceOnEdges(new SelectMinWeight(), EdgeDirection.OUT);
        //minWeights.print();

        Map<Long, Double> minWeightsMap = minWeights.collect()
                .stream()
                .collect(Collectors.toMap(tuple -> tuple.getField(0), tuple -> tuple.getField(1)));

        assertEquals(0.1, minWeightsMap.get(1L).doubleValue(), 0);
        assertEquals(0.3, minWeightsMap.get(2L).doubleValue(), 0);
        assertEquals(0.2, minWeightsMap.get(3L).doubleValue(), 0);
        assertEquals(0.9, minWeightsMap.get(4L).doubleValue(), 0);
        
        DataSet<Tuple2<Long, Long>> verticesWithSum = gelly.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);
        //verticesWithSum.print();

        Map<Long, Long> verticesWithSumMap = verticesWithSum.collect()
                .stream()
                .collect(Collectors.toMap(tuple -> tuple.getField(0), tuple -> tuple.getField(1)));

        assertEquals(1L, verticesWithSumMap.get(2L).longValue());
        assertEquals(1L, verticesWithSumMap.get(3L).longValue());
        assertEquals(6L, verticesWithSumMap.get(4L).longValue());
        assertEquals(6L, verticesWithSumMap.get(5L).longValue());


        DataSet<Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>> vertexPairs =
                gelly.groupReduceOnNeighbors(new SelectLargeWeightNeighbors(), EdgeDirection.OUT);
        //vertexPairs.print();

        Map<Vertex<Long, Long>, Vertex<Long, Long>> vertexPairsMap = vertexPairs.collect()
                .stream()
                .collect(Collectors.toMap(tuple -> tuple.getField(0), tuple -> tuple.getField(1)));

        assertEquals(4L, vertexPairsMap.get(new Vertex(2L, 2L)).getId().longValue());
        assertEquals(5L, vertexPairsMap.get(new Vertex(4L, 4L)).getId().longValue());
    }

    // user-defined function to select the minimum weight
    static final class SelectMinWeight implements ReduceEdgesFunction<Double> {

        @Override
        public Double reduceEdges(Double firstWeight, Double secondWeight) {
            return Math.min(firstWeight, secondWeight);
        }
    }

    // user-defined function to sum the neighbor values
    static final class SumValues implements ReduceNeighborsFunction<Long> {

        @Override
        public Long reduceNeighbors(Long firstNeighbor, Long secondNeighbor) {
            return firstNeighbor + secondNeighbor;
        }
    }
    
    // user-defined function to select the neighbors which have edges with weight > 0.5
    static final class SelectLargeWeightNeighbors implements NeighborsFunctionWithVertexValue<Long, Long, Double,
            Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>> {

        @Override
        public void iterateNeighbors(Vertex<Long, Long> vertex,
                                     Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, Long>>> neighbors,
                                     Collector<Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>> out) {

            for (Tuple2<Edge<Long, Double>, Vertex<Long, Long>> neighbor : neighbors) {
                if (neighbor.f0.f2 > 0.5) {
                    out.collect(new Tuple2<Vertex<Long, Long>, Vertex<Long, Long>>(vertex, neighbor.f1));
                }
            }
        }
    }
}
