package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;

public interface Creator {

    Element getElement();

    Iterator<Put> constructInsertions();

    default byte[] getQualifierToCheck() {
        return Constants.CREATED_AT_BYTES;
    }

    RuntimeException alreadyExists();
}
