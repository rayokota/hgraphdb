package io.hgraphdb.mutators;

import org.apache.hadoop.hbase.client.Put;

public interface Creator {

    Put constructPut();

    RuntimeException alreadyExists();
}
