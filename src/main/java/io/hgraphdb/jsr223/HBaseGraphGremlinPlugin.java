package io.hgraphdb.jsr223;

import io.hgraphdb.*;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public final class HBaseGraphGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "io.hgraphdb";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
            .addClassImports(HBaseBulkLoader.class,
                    HBaseEdge.class,
                    HBaseElement.class,
                    HBaseGraph.class,
                    HBaseGraphConfiguration.class,
                    HBaseGraphFeatures.class,
                    HBaseProperty.class,
                    HBaseVertex.class,
                    HBaseVertexProperty.class).create();

    private static final HBaseGraphGremlinPlugin instance = new HBaseGraphGremlinPlugin();

    public HBaseGraphGremlinPlugin() {
        super(NAME, imports);
    }

    public static HBaseGraphGremlinPlugin instance() {
        return instance;
    }
}