package io.hgraphdb.mutators;

import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraphException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mutators {

    public static void create(Table table, Creator creator) {
        Put put = creator.constructPut();
        byte[] row = put.getRow();
        try {
            boolean success = table.checkAndPut(row, Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES, null, put);
            if (!success) {
                throw creator.alreadyExists();
            }
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        }
    }

    public static void write(Table table, Mutator... writers) {
        List<Row> batch = new ArrayList<>();
        for (Mutator writer : writers) {
            writer.constructMutations().forEachRemaining(batch::add);
        }
        Object[] results = new Object[batch.size()];
        try {
            table.batch(batch, results);
            for (Object result : results) {
                // TODO RAY
            }
        } catch (IOException | InterruptedException e) {
            throw new HBaseGraphException(e);
        }
    }
}
