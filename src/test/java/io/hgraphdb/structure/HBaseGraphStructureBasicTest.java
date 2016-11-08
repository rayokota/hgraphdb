package io.hgraphdb.structure;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphProvider;
import io.hgraphdb.StructureBasicSuite;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(StructureBasicSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = HBaseGraph.class)
public class HBaseGraphStructureBasicTest {
}
