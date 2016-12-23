package io.hgraphdb.process.groovy;

import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

@Ignore
@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = HBaseGraphProvider.class, graph = HBaseGraph.class)
public class HBaseGraphGroovyProcessStandardTest {
}