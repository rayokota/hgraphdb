package io.hgraphdb.readers;

import io.hgraphdb.HBaseGraph;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class LoadingElementReader<T extends Element> extends ElementReader<T> {

    public LoadingElementReader(HBaseGraph graph) {
        super(graph);
    }

    public abstract void load(T element, Result result);
}
