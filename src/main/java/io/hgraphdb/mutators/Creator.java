package io.hgraphdb.mutators;

import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.structure.Element;

public interface Creator {

    Element getElement();

    Put constructPut();

    RuntimeException alreadyExists();
}
