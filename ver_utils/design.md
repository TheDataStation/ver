# API Design Document
The front end of **Ver** takes an example table as input and outputs a set of views. In this document, I define the APIs needed to construct views from user-given examples.

## Column Retrieval
Given a set of keywords, returns a list of candidate columns and their corresponding scores.

`input`: 

kw_list (List[str]): a list of keywords

`output`: 

List[(column, score)]: a list of (candidate column, score) pairs
    
- candidate column: column having values overlapping with the keyword list
    
- score: the number of values in the column overlapping with the keyword list

```python
def column_retrieval(kw_list: List[str]):
    return [(column, score)]
```


## Column Clustering
To deal with noisy inputs, this component  clusters candidate columns based on column content similarity (measured by jaccard similarity).

`input`:

candidate_columns: the columns retrieved from the column retrieval stage.

`output`:

clusters: a set of column clusters. Each cluster contains columns in the same connected component of Aurum's EKG graph.

```python
def cluster_columns(candidate_columns: List[(column, score)]):
    return clusters
```

## Join Path Search
Compute all join paths having lengths smaller than a threshold starting from a column.

`input`:

column: a column

max_hop: maximum length of a join path from the input column

`output`:

join_paths: a list of join paths starting from the input column

```python
def join_path_search(column, max_hop):
    return join_paths
```


## Join Graph Search
Compute the join graphs between a set of columns.

`input`:

columns: a list of columns

max_hop: maximum length of a join path between every pair of columns. 

`output`:

join_graphs: a list of join graphs between columns

```python
def join_graph_search(columns: List[column], max_hop: int):
    return join_graphs
```

## Materializer
`input`: 

join_graph: a join graph obtained from the join_graph search stage.

`output`:

view: a materialized view based on the input join graph

```python
def materialize(join_graph):
    return view
```