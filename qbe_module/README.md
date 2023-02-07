# Ver - QBE module

`Prerequisite`: Aurum

`Input`: An example table

`Output`: A list of views and their overlap scores corresonding to the example table

## Stage 1: Column Retrieval

The first step of finding views is to find columns that users need. Given a example column e.g. ['China', 'United States'], column retrieval wants to find columns containing user-specified examples and help users navigate through candidate columns.

### Why do we need to navigate users among candidate columns?

 Because of the ambiguity in examples and large repository of data, there can be a large number of columns containing user examples.

### How do we navigate users?

 Columns with similar content are grouped together. Users can choose columns by groups instead of looking at each column individually. 

## Stage 2: Join Graph Search
After getting a set of candidate columns for each example column, we need to figure out how to combine those columns into views, which is the mission of Join-Graph-Search component. Join-Graph-Search component takes a set of columns, and output join graphs to combine given columns.

## Stage 3: Materializer
After join graphs are produced, the last step is to materialize the join graph and get the final view. Materializer component takes a join graph as input and output the view after materializing the join graph.