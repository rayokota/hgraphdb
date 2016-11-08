package io.hgraphdb.readers;

import org.apache.hadoop.hbase.client.Result;

public interface Reader<T> {

    T parse(Result result);
}
