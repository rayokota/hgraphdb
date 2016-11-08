package io.hgraphdb.mutators;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.Iterator;

public interface Mutator {

    Iterator<Mutation> constructMutations();
}
