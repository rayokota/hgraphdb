# HGraphDB - HBase as a TinkerPop Graph Database

[![Build Status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[travis-shield]: https://travis-ci.org/rayokota/hgraphdb.svg?branch=master
[travis-link]: https://travis-ci.org/rayokota/hgraphdb
[maven-shield]: https://img.shields.io/maven-central/v/io.hgraphdb/hgraphdb.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.hgraphdb
[javadoc-shield]: https://javadoc.io/badge/io.hgraphdb/hgraphdb.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.hgraphdb/hgraphdb

HGraphDB is a client layer for using HBase as a graph database.  It is an implementation of the [Apache TinkerPop 3](http://tinkerpop.apache.org) interfaces.

**Note:  the format of vertex indices has changed between 1.x and 2.x.  If you upgrade, you will need to drop any existing vertex indices while on 1.x and recreate them after upgrading to 2.x.**

## Installing

Releases of HGraphDB are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.hgraphdb</groupId>
    <artifactId>hgraphdb</artifactId>
    <version>2.0.0</version>
</dependency>
```

## Setup

To initialize HGraphDB, create an `HBaseGraphConfiguration` instance, and then use a static factory method to create an `HBaseGraph` instance.

```java
Configuration cfg = new HBaseGraphConfiguration()
    .setInstanceType(InstanceType.DISTRIBUTED)
    .setGraphNamespace("mygraph")
    .setCreateTables(true)
    .setRegionCount(numRegionServers)
    .set("hbase.zookeeper.quorum", "127.0.0.1")
    .set("zookeeper.znode.parent", "/hbase-unsecure");
HBaseGraph graph = (HBaseGraph) GraphFactory.open(cfg);
```

As you can see above, HBase-specific configuration parameters can be passed directly.  These will be used when obtaining an HBase connection. 

The resulting graph can be used like any other TinkerPop graph instance.

```java
Vertex v1 = graph.addVertex(T.id, 1L, T.label, "person", "name", "John");
Vertex v2 = graph.addVertex(T.id, 2L, T.label, "person", "name", "Sally");
v1.addEdge("knows", v2, T.id, "edge1", "since", LocalDate.now());
```

A few things to note from the above example :

- HGraphDB accepts user-supplied IDs, for both vertices and edges.
- The following types can be used for both IDs and property values:
  - boolean
  - String
  - numbers (byte, short, int, long, float, double)
  - java.math.BigDecimal
  - java.time.LocalDate
  - java.time.LocalTime
  - java.time.LocalDateTime
  - java.time.Duration
  - java.util.UUID
  - byte arrays
  - Enum instances
  - [Kryo](https://github.com/EsotericSoftware/kryo)-serializable instances
  - Java-serializable instances

## Using Indices

Two types of indices are supported by HGraphDB:

- Vertices can be indexed by label and property.
- Edges can be indexed by label and property, specific to a vertex.

An index is created as follows:

```java
graph.createIndex(ElementType.VERTEX, "person", "name");
...
graph.createIndex(ElementType.EDGE, "knows", "since");
```

The above commands should be run before the relevant data is populated.  To create an index after data has been populated, first create the index with the following parameters:

```java
graph.createIndex(ElementType.VERTEX, "person", "name", false, /* populate */ true, /* async */ true);
```

Then run a MapReduce job using the `hbase` command:

```bash
hbase io.hgraphdb.mapreduce.index.PopulateIndex \
    -t vertex -l person -p name -op /tmp -ca gremlin.hbase.namespace=mygraph
```

Once an index is created and data has been populated, it can be used as follows:

```java
// get persons named John
Iterator<Vertex> it = graph.verticesByLabel("person", "name", "John");
...
// get persons first known by John between 2007-01-01 (inclusive) and 2008-01-01 (exclusive)
Iterator<Edge> it = johnV.edges(Direction.OUT, "knows", "since", 
    LocalDate.parse("2007-01-01"), LocalDate.parse("2008-01-01"));
```

Note that the indices support range queries, where the start of the range is inclusive and the end of the range is exclusive.

An index can also be specified as a unique index.  For a vertex index, this means only one vertex can have a particular property name-value for the given vertex label.  For an edge index, this means only one edge of a specific vertex can have a particular property name-value for a given edge label.

```java
graph.createIndex(ElementType.VERTEX, "person", "name", /* unique */ true);
```

To drop an index, invoke a MapReduce job using the `hbase` command:

```bash
hbase io.hgraphdb.mapreduce.index.DropIndex \
    -t vertex -l person -p name -op /tmp -ca gremlin.hbase.namespace=mygraph
```

## Pagination

Once an index is defined, results can be paginated.  HGraphDB supports [keyset pagination](http://use-the-index-luke.com/no-offset), for both vertex and edge indices. 

```java
// get first page of persons (note that null is passed as start key)
final int pageSize = 20;
Iterator<Vertex> it = graph.verticesWithLimit("person", "name", null, pageSize);
...
// get next page using start key of last person from previous page
it = graph.verticesWithLimit("person", "name", "John", pageSize + 1);
...
// get first page of persons most recently known by John
Iterator<Edge> it = johnV.edgesWithLimit(Direction.OUT, "knows", "since", 
    null, pageSize, /* reversed */ true);
```

Also note that indices can be paginated in descending order by passing `reversed` as `true`.

## Schema Management

By default HGraphDB does not use a schema.  Schema management can be enabled by calling `HBaseGraphConfiguration.useSchema(true)`.  Once schema management is enabled, the schema for vertex and edge labels can be defined.

```java
graph.createLabel(ElementType.VERTEX, "author", /* id */ ValueType.STRING, "age", ValueType.INT);
graph.createLabel(ElementType.VERTEX, "book", /* id */ ValueType.STRING, "publisher", ValueType.STRING);
graph.createLabel(ElementType.EDGE, "writes", /* id */ ValueType.STRING, "since", ValueType.DATE);   
```

Edge labels must be explicitly connected to vertex labels before edges are added to the graph.

```java
graph.connectLabels("author", "writes", "book"); 
```

Additional properties can be added to labels at a later time; otherwise labels cannot be changed.

```java
graph.updateLabel(ElementType.VERTEX, "author", "height", ValueType.DOUBLE);
```

Whenever vertices or edges are added to the graph, they will first be validated against the schema.    

## Counters

One unique feature of HGraphDB is support for counters.  The use of counters requires that schema management is enabled.

```java
graph.createLabel(ElementType.VERTEX, "author", ValueType.STRING, "bookCount", ValueType.COUNTER);

HBaseVertex v = (HBaseVertex) graph.addVertex(T.id, "Kierkegaard", T.label, "author");
v.incrementProperty("bookCount", 1L);
```

One caveat is that indices on counters are not supported.

Counters can be used by clients to materialize the number of edges on a node, for example, which will be more efficient than retrieving all the edges in order to obtain the count.  In this case, whenever an edge is added or removed, the client would either increment or decrement the corresponding counter.

Counter updates are atomic as they make use of the underlying support for counters in HBase.  

## Graph Analytics with Giraph

HGraphDB provides integration with [Apache Giraph](http://giraph.apache.org) by providing two input formats, `HBaseVertexInputFormat` and `HBaseEdgeInputFormat`, that can be used to read from the vertices table and the edges tables, respectively.  HGraphDB also provides two abstract output formats, `HBaseVertexOutputFormat` and `HBaseEdgeOutputFormat`, that can be used to modify the graph after a Giraph computation.

Finally, HGraphDB provides a testing utility, `InternalHBaseVertexRunner`, that is similar to `InternalVertexRunner` in Giraph, and that can be used to run Giraph computations using a local Zookeeper instance running in another thread.

See [this blog post](https://yokota.blog/2016/12/13/graph-analytics-on-hbase-with-hgraphdb-and-giraph/) for more details on using Giraph with HGraphDB.

## Graph Analytics with Spark GraphFrames

[Apache Spark GraphFrames](https://graphframes.github.io) can be used to analyze graphs stored in HGraphDB.  First the vertices and edges need to be wrapped with Spark DataFrames using the [Spark-on-HBase Connector](https://github.com/hortonworks-spark/shc) and a custom [SHCDataType](https://github.com/rayokota/shc/blob/master/core/src/main/scala/org/apache/spark/sql/execution/datasources/hbase/types/HGraphDB.scala).  Once the vertex and edge DataFrames are available, obtaining a GraphFrame is as simple as the following:

```scala
val g = GraphFrame(verticesDataFrame, edgesDataFrame)
```

See [this blog post](https://yokota.blog/2017/04/02/graph-analytics-on-hbase-with-hgraphdb-and-spark-graphframes/) for more details on using Spark GraphFrames with HGraphDB.

## Graph Analytics with Flink Gelly

HGraphDB provides support for analyzing graphs with [Apache Flink Gelly](https://flink.apache.org/news/2015/08/24/introducing-flink-gelly.html).  First the vertices and edges need to be wrapped with Flink DataSets by importing graph data with instances of `HBaseVertexInputFormat` and `HBaseEdgeInputFormat`.  After obtaining the DataSets, a Gelly graph can be created as follows:

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
Graph gelly = Graph.fromTupleDataSet(vertices, edges, env);
```

See [this blog post](https://yokota.blog/2017/07/27/graph-analytics-on-hbase-with-hgraphdb-and-apache-flink-gelly/) for more details on using Flink Gelly with HGraphDB.

## Support for Google Cloud Bigtable

HGraphDB can be used with [Google Cloud Bigtable](https://cloud.google.com/bigtable/).  Since Bigtable does not support namespaces, we set the name of the graph as the table prefix below.

```java
Configuration cfg = new HBaseGraphConfiguration()
    .setInstanceType(InstanceType.BIGTABLE)
    .setGraphTablePrefix("mygraph")
    .setCreateTables(true)
    .set("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_x.BigtableConnection")
    .set("google.bigtable.instance.id", "my-instance-id")
    .set("google.bigtable.project.id", "my-project-id");
HBaseGraph graph = (HBaseGraph) GraphFactory.open(cfg);
```

## Using the Gremlin Console

One benefit of having a TinkerPop layer to HBase is that a number of graph-related tools become available, which are all part of the TinkerPop ecosystem.  These tools include the Gremlin DSL and the Gremlin console.  To use HGraphDB in the Gremlin console, run the following commands:

```
         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: tinkerpop.server
plugin activated: tinkerpop.utilities
plugin activated: tinkerpop.tinkergraph
gremlin> :install org.apache.hbase hbase-client 1.4.1
gremlin> :install org.apache.hbase hbase-common 1.4.1
gremlin> :install org.apache.hadoop hadoop-common 2.5.1
gremlin> :install io.hgraphdb hgraphdb 2.0.0
gremlin> :plugin use io.hgraphdb
```

Then restart the Gremlin console and run the following:

```
gremlin> graph = HBaseGraph.open("mygraph", "127.0.0.1", "/hbase-unsecure")
```

## Performance Tuning

### Caching

HGraphDB provides two kinds of caches, global caches and relationship caches.  Global caches contain both vertices and edges. Relationship caches are specific to a vertex and cache the edges that are incident to the vertex.  Both caches can be controlled through `HBaseGraphConfiguration` by specifying a maximum size for each type of cache as well as a TTL for elements after they have been accessed via the cache.  Specifying a maximum size of 0 will disable caching.

### Lazy Loading

By default, vertices and edges are eagerly loaded.  In some failure conditions, it may be possible for indices to point to vertices or edges which have been deleted.  By eagerly loading graph elements, stale data can be filtered out and removed before it reaches the client.  However, this incurs a slight performance penalty.  As an alternative, lazy loading can be enabled.  This can be done by calling `HBaseGraphConfiguration.setLazyLoading(true)`.  However, if there are stale indices in the graph, the client will need to handle the exception that is thrown when an attempt is made to access a non-existent vertex or edge.

### Bulk Loading

HGraphDB also provides an `HBaseBulkLoader` class for more performant loading of vertices and edges.  The bulk loader will not attempt to check if elements with the same ID already exist when adding new elements.

## Implementation Notes

HGraphDB uses a tall table schema.  The schema is created in the namespace specified to the `HBaseGraphConfiguration`.  The tables look as follows:

### Vertex Table

| Row Key | Column: label | Column: createdAt | Column: [property1 key] | Column: [property2 key] | ... |
|---|---|---|---|---|---|
| [vertex ID] | [label value] | [createdAt value] | [property1 value] | [property2 value] |...|

### Edge Table

| Row Key | Column: label | Column: fromVertex | Column: toVertex | Column: createdAt | Column: [property1 key] | Column: [property2 key] | ... |
|---|---|---|---|---|---|---|---|
| [edge ID] | [label value] | [fromVertex ID ] | [toVertex ID] | [createdAt value] | [property1 value] | [property2 value] | ... |

### Vertex Index Table

| Row Key | Column: createdAt | Column: vertexID |
|---|---|---|
| [vertex label, isUnique, property key, property value, vertex ID (if not unique)] | [createdAt value] | [vertex ID (if unique)] |

### Edge Index Table

| Row Key | Column: createdAt | Column: vertexID | Column: edgeID |
|---|---|---|---|
| [vertex1 ID, direction, isUnique, property key, edge label, property value, vertex2 ID (if not unique), edge ID (if not unique)] | [createdAt value] | [vertex2 ID (if unique)] | [edge ID (if unique)] |

### Index Metadata Table

| Row Key | Column: createdAt | Column: isUnique | Column: state |
|---|---|---|---|
| [label, property key, element type] | [createdAt value] | [isUnique value] | [state value] |

Note that in the index tables, if the index is a unique index, then the indexed IDs are stored in the column values; otherwise they are stored in the row key.  

If schema management is enabled, two additional tables are used:

### Label Metadata Table

| Row Key | Column: id | Column: createdAt | Column: [property1 key] | Column: [property2 key] | ... |
|---|---|---|---|---|---|
| [label, element type] | [id type] | [createdAt value] | [property1 type] | [property2 type] |...|

### Label Connections Table

| Row Key | Column: createdAt |
|---|---|
| [from vertex label, edge label, to vertex label] | [createdAt value] |

HGraphDB was designed to support the features mentioned [here](https://rayokota.wordpress.com/2016/11/10/hgraphdb-hbase-as-a-tinkerpop-graph-database/).

## <a name="future"></a>Future Enhancements 

Possible future enhancements include MapReduce jobs for the following:

- Cleaning up stale indices.
