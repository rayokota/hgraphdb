
# HGraphDB - HBase as a Tinkerpop Graph Database

HGraphDB is a client layer for using HBase as a graph database.  It is an implementation of the [Apache TinkerPop 3](http://tinkerpop.apache.org) framework.

## Building

Prerequisites for building HGraphDB:

* git
* Maven
* Java 8

```
git clone https://github.com/rayokota/hgraphdb.git
cd hgraphdb
mvn clean package -DskipTests
```

## Setup

To initialize HGraphDB, create an `HBaseGraphConfiguration` instance, and then use a static factory method to create an `HBaseGraph` instance.

		...
		Configuration cfg = new HBaseGraphConfiguration()
   			.setInstanceType(InstanceType.Distributed)
  			.setGraphNamespace("mygraph")
  			.setCreateTables(true)
  			.set("hbase.zookeeper.quorum", "127.0.0.1")
  			.set("zookeeper.znode.parent", "/hbase-unsecure");
		HBaseGraph graph = GraphFactory.open(cfg);
		...

As you can see above, HBase-specific configuration parameters can be passed directly.  These will be used when obtaining an HBase connection. 

The resulting graph can be used like any other TinkerPop graph instance.

		...
		Vertex v1 = graph.addVertex(T.id, 1, T.label, "person", "name", "John");
		Vertex v2 = graph.addVertex(T.id, 2, T.label, "person", "name", "Sally");
		v1.addEdge(T.id, "edge1", T.label, "knows", "since", LocalDate.now());
		...
		
A few things to note from the above example :

- HGraphDB accepts user-supplied IDs, for both vertices and edges.
- The following types can be used for both IDs and property values:
	- boolean
	- String
	- numbers (byte, short, int long, float, double)
	- java.math.BigDecimal
	- java.time.LocalDate
	- java.time.LocalTime
	- java.time.LocalDateTime
	- java.time.Duration
	- byte arrays
	- Enum instances
	- Kryo-serializable instances
	- Java-serializable instances

## Creating Indices

Two types of indices are supported by HGraphDB:

- Vertices can be indexed by label and property.
- Edges can be indexed by label and property, specific to a vertex.

An index is created as follows:

		graph.createIndex(IndexType.VERTEX, "person", "name")
		...
		graph.createIndex(IndexType.EDGE, "knows", "since")

Indices should be created before the relevant data is populated.  A future [enhancement](#future) will allow for index creation after data population.

Once an index is created and data has been populated, it can be used as follows:

		// get persons named John
		Iterator<Vertex> it = graph.allVertices("person", "name", "John");
		...
		// get persons first known betwen 2007-01-01 (inclusive) and 2008-01-01 (exclusive)
		Iterator<Edge> it = v1.edges(Direction.OUT, "knows", "since", 
			LocalDate.parse("2007-01-01"), LocalDate.parse("2008-01-01"));
		
		
## Using the Gremlin Shell

One advantage of providing a TinkerPop layer to HBase is the abundance of tools that suddenly become available, including the Gremlin DSL and the Gremlin shell.  To use the Gremlin shell, the following commands can be used.

                 \,,,/
                 (o o)
        -----oOOo-(3)-oOOo-----
        plugin activated: tinkerpop.server
        plugin activated: tinkerpop.utilities
        plugin activated: tinkerpop.tinkergraph
        gremlin> :install io.hgraphdb hgraphdb 0.0.1
        gremlin> :install org.apache.hbase hbase-client 1.2.0
        gremlin> :install org.apache.hbase hbase-common 1.2.0
        gremlin> :install org.apache.hadoop hadoop-common 2.5.1
        gremlin> :install io.hgraphdb hgraphdb 0.0.1
        gremlin> :plugin use io.hgraphdb
                
Then restart the console.

		gremlin> g = HBaseGraph.open("foo", "127.0.0.1", "/hbase-unsecure")


## Performance Tuning

### Lazy Loading

By default, vertices and edges are lazily loaded.  In some failure conditions, it may be possible for indices to point to vertices or edges which no longer exist.  In this case the application will need to handle the exception that is thrown when the vertex or edge is accessed.  However, if lazy loading is disabled, then HGraphDB will automatically clean up stale indices.  This can be done by calling `HBaseGraphConfiguration.setLazyLoading(true)`.

### Caching

HGraphDB supports two kinds of caches, element caches and relationship caches.  Element caches are global, while relationship caches are specific to a vertex.  Both caches can be controlled through `HBaseGraphConfiguration` by specifying a maximum size for each type of cache as well as a TTL for elements after they have been accessed via the cache.  Specifying a maximum size of 0 will disable caching.

### Bulk Loading

HGraphDB also provides a `HBaseBulkLoader` class for more performant loading of vertices and edges.  The bulk loader will not attempt to check if elements with the same ID already exist before adding new elements.

## Implementation Notes

HGraphDB uses a tall table model, similar to Zen and S2Graph.  The tables look as follows:

### Vertex Table

| Row Key | Column: label | Column: createdAt | Column: [property1 key] | Column: [property2 key] |
|---|---|---|---|---|
| [vertex ID] | [label value] | [createdAt value] | [property1 value] | [property2 value] |

### Edge Table

| Row Key | Column: label | Column: createdAt | Column: [property1 key] | Column: [property2 key] |
|---|---|---|---|---|
| [edge ID] | [label value] | [createdAt value] | [property1 value] | [property2 value] |

### Vertex Index Table

| Row Key | Column: createdAt |
|---|---|
| [vertex label, property key, property value, vertex ID] | [createdAt value] |
	
### Edge INdex Table

| Row Key | Column: createdAt |
|---|---|
| [vertex1 ID, direction, edge label, property key, property value, vertex2 ID, edge ID] | [createdAt value] |

### Index Metadata Table

| Row Key | Column: createdAt | Column: state |
|---|---|---|
| [label, property key, index type] | [createdAt value] | [index state] |
	
## <a name="future"></a>Future Enhancements 

Possible future enhancements include map-reduce jobs for the following:

- Populating indices after graph elements have already been added
- Cleaning up stale indices
- Deleting indices



