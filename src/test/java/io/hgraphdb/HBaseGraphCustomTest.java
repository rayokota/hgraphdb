package io.hgraphdb;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(CustomSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = HBaseGraph.class)
public class HBaseGraphCustomTest {
}
