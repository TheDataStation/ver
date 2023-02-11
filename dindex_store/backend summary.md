# Summary of Discovery Graph Backend Choices

Workload  
* We use pair combinations of number of nodes [100, 200, 400, 800, 1600] and edge sparsity [0.1, 0.2].  
* Workload are measured by execution time and memory usage on data loading, 2-hop neighborhood, and path finding queries  
* Memory usage calculated from 2 sides: client side (query runner) and server side (database). For file based and/or runtime databases, we measured the memory usage only on the client side. For a detached database, we measured the server side maximum memory usage from the docker container that host said database.  

---

## DuckDB
Link: https://github.com/duckdb/duckdb

Pros:
* Mature Community - constantly updated
* Easy to use - import duckdb is all we need.
* Framework is fairly simple - two tables and recursive SQL queries for graph traversal

Cons:
* SQL database is not suitable for graph workloads (long join paths / multiple-hop neighborhood search). Might need to tweek duckdb (not clear how much we need to do) to optimize, which adds complexity to the system.

License:
MIT License

---

## Kuzu
Github link: https://github.com/kuzudb/kuzu

An in-process graph database.

Pros:
* Easy to use - import kuzu is all we need (though less API than duckdb)
* A Graph Database! - ideal for our workload.

Cons:
* Still in active development - system is not stable (see issues page on github).
* Missing a lot of features, in particular shortest_path, which makes it hard for path queries (don’t know how we can do path queries otherwise).

License:
MIT License

---

## DGraph
Github link:
https://github.com/dgraph-io/dgraph 

Pros:
* A graph database!
* Stable releases and fairly popular

Cons:
* Harder to set up - need docker and connecting that server, thus more complexity
* Framework is awkward to use:
    * Uses RDF triple natively to identify data (though also supports JSON)
    * GraphQL and graph is a bit unintuitive: Each node is either a value and a “node” type, and each field/attribute is represented by an edge, connecting the node to that value.
    * How do we populate the database?
        * 1 by 1: Each insertion takes two queries (to locate each end node) and a mutation (to add the edge).
        * In bulk: A super long string in python.
* Licensing issues

License:
Apache Public License 2.0 (APL) and Dgraph Community License (DCL)

---

## GrainDB
Github link:
https://github.com/graindb/graindb

Pros:
* Modified specifically for graph query and claims to be faster than DuckDB for graph queries
* Might be a good start if we decide to tweak DuckDB

Cons:
* Requires a lot of changes to support Python (still haven’t been able to run)
* The project is no longer active

License:
MIT License

---

## Graph tool
Link:
https://graph-tool.skewed.de/

Pros:
* Very quick runtime for finding path between two vertices.
* Easy to use and easy to setup.

Cons:
* The graph is stored in the memory, large dataset will consume a lot of memory.
* Not so good performance for finding neighborhood

License:
LGPLv3

Notes:
* We can reduce the memory workload by moving unnecessary part of the vertex property to disk by using conventional database. Only leaving the essential part of the properties to enable faster graph query (path finding, n-hop neighborhood).
* But by doing so, we increase the complexity of our backend, and implementing it on graph-tool have been quite a challenge.

---

## NetworKit
Link:
https://networkit.github.io

Pros:
* Almost the same speed as graph-tool for finding path, very fast
* Far quicker for finding neighborhood

Cons:
* Use in memory storage
* The python documentation is not very detailed

License:
MIT License

Notes:
* Because of the in-memory storage, we can't just store all of the graph properties. The backend memory will be bloated for very large datasets.
* The workaround: We use NetworKit as main graph storage, storing the nodes and the relationship between the nodes. We use RDBMS as the node properties storage.
* We already combine a few RDBMS (postgres, etc.) as a properties storage with NetworKit as the graph backend. The initial performance results have been quite good. The combined implementation performs better than DuckDB on pathfinding and neighborhood queries with relatively low memory usage while indexing the TPC-H dataset.
* But because of the added complexity, writing modular code with easy-to-use API is very challenging. Not only that, every RDBMS is different. Some features on one might not exist on another, thus complicating the backend development to maintain compatibility with various RDBMS.

---

## ArangoDB
Github link:
https://github.com/arangodb/arangodb

Pros:
* Path finding is fast (compared to DuckDB, around 50x faster)
* Have a lot of graph features
* Mature community
* Offer Docker image and Python API

Cons:
* 2-hop neighborhood is slow (compared to DuckDB, around 3x slower)
* Python API is not as complete as Javascript API (this can be solved using manual query)

License:
Apache 2.0 License

