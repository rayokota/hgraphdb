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

import io.hgraphdb.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class MockConnection implements Connection {

    private final Configuration config;
    private final Map<TableName, Table> tables = new ConcurrentHashMap<>();

    public MockConnection(Configuration config) {
        this.config = config;
    }

    /**
     * @return Configuration instance being used by this Connection instance.
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Retrieve a Table implementation for accessing a table.
     * The returned Table is not thread safe, a new instance should be created for each using thread.
     * This is a lightweight operation, pooling or caching of the returned Table
     * is neither required nor desired.
     * <p>
     * The caller is responsible for calling {@link Table#close()} on the returned
     * table instance.
     * <p>
     * Since 0.98.1 this method no longer checks table existence. An exception
     * will be thrown if the table does not exist only when the first operation is
     * attempted.
     * @param tableName the name of the table
     * @return a Table to use for interactions with this table
     */
    public Table getTable(TableName tableName) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            table = new MockHTable(tableName, Constants.DEFAULT_FAMILY).setConfiguration(config);
            tables.put(tableName, table);
        }
        return table;
    }

    /**
     * Retrieve a Table implementation for accessing a table.
     * The returned Table is not thread safe, a new instance should be created for each using thread.
     * This is a lightweight operation, pooling or caching of the returned Table
     * is neither required nor desired.
     * <p>
     * The caller is responsible for calling {@link Table#close()} on the returned
     * table instance.
     * <p>
     * Since 0.98.1 this method no longer checks table existence. An exception
     * will be thrown if the table does not exist only when the first operation is
     * attempted.
     *
     * @param tableName the name of the table
     * @param pool The thread pool to use for batch operations, null to use a default pool.
     * @return a Table to use for interactions with this table
     */
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            table = new MockHTable(tableName, Constants.DEFAULT_FAMILY);
            tables.put(tableName, table);
        }
        return table;
    }

    /**
     * <p>
     * Retrieve a {@link BufferedMutator} for performing client-side buffering of writes. The
     * {@link BufferedMutator} returned by this method is thread-safe. This BufferedMutator will
     * use the Connection's ExecutorService. This object can be used for long lived operations.
     * </p>
     * <p>
     * The caller is responsible for calling {@link BufferedMutator#close()} on
     * the returned {@link BufferedMutator} instance.
     * </p>
     * <p>
     * This accessor will use the connection's ExecutorService and will throw an
     * exception in the main thread when an asynchronous exception occurs.
     *
     * @param tableName the name of the table
     *
     * @return a {@link BufferedMutator} for the supplied tableName.
     */
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return new MockBufferedMutator(this, tableName, config);
    }

    /**
     * Retrieve a {@link BufferedMutator} for performing client-side buffering of writes. The
     * {@link BufferedMutator} returned by this method is thread-safe. This object can be used for
     * long lived table operations. The caller is responsible for calling
     * {@link BufferedMutator#close()} on the returned {@link BufferedMutator} instance.
     *
     * @param params details on how to instantiate the {@code BufferedMutator}.
     * @return a {@link BufferedMutator} for the supplied tableName.
     */
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        return new MockBufferedMutator(this, params.getTableName(), config);
    }

    /**
     * Retrieve a RegionLocator implementation to inspect region information on a table. The returned
     * RegionLocator is not thread-safe, so a new instance should be created for each using thread.
     *
     * This is a lightweight operation.  Pooling or caching of the returned RegionLocator is neither
     * required nor desired.
     * <br>
     * The caller is responsible for calling {@link RegionLocator#close()} on the returned
     * RegionLocator instance.
     *
     * RegionLocator needs to be unmanaged
     *
     * @param tableName Name of the table who's region is to be examined
     * @return A RegionLocator instance
     */
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieve an Admin implementation to administer an HBase cluster.
     * The returned Admin is not guaranteed to be thread-safe.  A new instance should be created for
     * each using thread.  This is a lightweight operation.  Pooling or caching of the returned
     * Admin is not recommended.
     * <br>
     * The caller is responsible for calling {@link Admin#close()} on the returned
     * Admin instance.
     *
     * @return an Admin instance for cluster administration
     */
    public Admin getAdmin() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Abort the server or client.
     * @param why Why we're aborting.
     * @param e Throwable that caused abort. Can be null.
     */
    public void abort(String why, Throwable e) {
    }

    /**
     * Check if the server or client was aborted.
     * @return true if the server or client was aborted, false otherwise
     */
    public boolean isAborted() {
        return false;
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Returns whether the connection is closed or not.
     * @return true if this connection is closed
     */
    public boolean isClosed() {
        return false;
    }
}
