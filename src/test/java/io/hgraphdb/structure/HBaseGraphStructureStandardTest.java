package io.hgraphdb.structure;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

@Ignore
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = HBaseGraph.class)
public class HBaseGraphStructureStandardTest {
}
