package io.hgraphdb.process;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = HBaseGraph.class)
public class HBaseGraphProcessStandardTest {
}