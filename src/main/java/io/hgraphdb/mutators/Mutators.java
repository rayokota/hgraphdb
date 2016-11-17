package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseElement;
import io.hgraphdb.HBaseGraphException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Mutators {

    public static final String IS_UNIQUE = "isUnique";

    public static void create(Table table, Creator... creators) {
        List<Mutation> batch = new ArrayList<>();
        for (Creator creator : creators) {
            Iterator<Put> insertions = creator.constructInsertions();
            insertions.forEachRemaining(put -> {
                byte[] isUniqueBytes = put.getAttribute(IS_UNIQUE);
                boolean isUnique = isUniqueBytes == null || Bytes.toBoolean(isUniqueBytes);
                if (isUnique) {
                    create(table, creator, put);
                } else {
                    batch.add(put);
                }
            });
        }
        write(table, batch);
    }

    private static void create(Table table, Creator creator, Put put) {
        byte[] row = put.getRow();
        try {
            boolean success = table.checkAndPut(row, Constants.DEFAULT_FAMILY_BYTES,
                    creator.getQualifierToCheck(), null, put);
            if (!success) {
                HBaseElement element = (HBaseElement) creator.getElement();
                if (element != null) {
                    element.removeStaleIndices();
                }
                throw creator.alreadyExists();
            }
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public static void write(Table table, Mutator... writers) {
        List<Mutation> batch = new ArrayList<>();
        for (Mutator writer : writers) {
            writer.constructMutations().forEachRemaining(batch::add);
        }
        write(table, batch);
    }

    private static void write(Table table, List<Mutation> mutations) {
        if (mutations.size() == 0) return;
        Object[] results = new Object[mutations.size()];
        try {
            table.batch(mutations, results);
            for (Object result : results) {
                if (result instanceof Exception) {
                    throw new HBaseGraphException((Exception) result);
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new HBaseGraphException(e);
        }
    }
}
