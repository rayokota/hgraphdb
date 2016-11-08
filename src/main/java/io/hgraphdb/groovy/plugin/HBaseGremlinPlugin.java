package io.hgraphdb.groovy.plugin;

import io.hgraphdb.HBaseGraph;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;

import java.util.HashSet;
import java.util.Set;

public final class HBaseGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + HBaseGraph.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "io.hgraphdb";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}