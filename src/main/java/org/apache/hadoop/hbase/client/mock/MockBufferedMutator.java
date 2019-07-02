/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MockBufferedMutator implements BufferedMutator {

    private final Connection conn;
    private final TableName name;
    private final Configuration config;
    private final List<Mutation> mutations = new ArrayList<>();

    public MockBufferedMutator(Connection conn, TableName name, Configuration config) {
        this.conn = conn;
        this.name = name;
        this.config = config;
    }

    /**
     * Gets the fully qualified table name instance of the table that this BufferedMutator writes to.
     */
    public TableName getName() {
        return name;
    }

    /**
     * Returns the {@link org.apache.hadoop.conf.Configuration} object used by this instance.
     * <p>
     * The reference returned is not a copy, so any change made to it will
     * affect this instance.
     */
    public Configuration getConfiguration() {
        return config;
    }

    public List<Mutation> getMutations() {
        return mutations;
    }

    /**
     * Sends a {@link Mutation} to the table. The mutations will be buffered and sent over the
     * wire as part of a batch. Currently only supports {@link Put} and {@link Delete} mutations.
     *
     * @param mutation The data to send.
     */
    public void mutate(Mutation mutation) {
        mutations.add(mutation);
    }

    /**
     * Send some {@link Mutation}s to the table. The mutations will be buffered and sent over the
     * wire as part of a batch. There is no guarantee of sending entire content of {@code mutations}
     * in a single batch; it will be broken up according to the write buffer capacity.
     *
     * @param ms The data to send.
     * @throws IOException if a remote or network exception occurs.
     */
    public void mutate(List<? extends Mutation> ms) throws IOException {
        mutations.addAll(ms);
    }

    /**
     * Performs a {@link #flush()} and releases any resources held.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    @Override
    public void close() throws IOException {
        flush();
    }

    /**
     * Executes all the buffered, asynchronous {@link Mutation} operations and waits until they
     * are done.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    public void flush() throws IOException {
        //noinspection EmptyCatchBlock
        try {
            if (conn != null) {
                Object[] results = new Object[mutations.size()];
                conn.getTable(name).batch(mutations, results);
            }
            mutations.clear();
        } catch (InterruptedException e) {
        }
    }

    /**
     * Returns the maximum size in bytes of the write buffer for this HTable.
     * <p>
     * The default value comes from the configuration parameter {@code hbase.client.write.buffer}.
     *
     * @return The size of the write buffer in bytes.
     */
    public long getWriteBufferSize() {
        return Long.MAX_VALUE;
    }
}
