package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class ElementReader<T extends Element> implements Reader<T> {

    protected final HBaseGraph graph;

    public ElementReader(HBaseGraph graph) {
        this.graph = graph;
    }
}
