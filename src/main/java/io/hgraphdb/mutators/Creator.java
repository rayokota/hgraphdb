package io.hgraphdb.mutators;

import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;
import java.util.List;

public interface Creator {

    Element getElement();

    Iterator<Put> constructInsertions();

    RuntimeException alreadyExists();
}
