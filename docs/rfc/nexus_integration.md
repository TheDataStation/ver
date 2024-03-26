# Nexus Integration

Nexus supports aligning datasets along spatio-temporal aggregation hierarchies, calculating and organizing correlations.

## The value of integrating Nexus and Ver(Aurum)

1. Finding tables and attributes of interest: aurum api can help users look for attributes or tables of interest. Instead of discovering correlations for all datasets, correlation discovery can be conducted on the selected subset of data.

2. Navigating the correlation result: aurum api can let users specify additional filters to navigate the correlation result, such as filtering based on values in a column.

3. More-General Joins: To calculate correlations, we aggregate the numeric values associated with a key into a single number. In Nexus, this key is spatio-temporal identifier. However, this key identifier can be more general, i.e., the key can be any categorical or identifier columns. Aurum api can query joinable attributes for columns of more types (text or numerical). We can utilize this api to extend Nexus to discover correlations beyond spatio-temporal alignment. However, we still need to index the aggregation of data using Nexus api.

## Options of integration

1. Integrate Nexus into Aurum's graph index. Aurum's graph index maintains relationships between attributes. Nexus can be part of the indexing stage that calculates the correlation information between attributes and store these info in the edges.

2. Maintain a separate set of APIs of Nexus.

    a. query spatial-temporal joinable datasets
    ```python
        joinable(tbl, st_attrs, st_granu, overlap_t) -> joinable datasets
    ```

    b. join two spatial-temporal datasets to a given granularity
    ```python
        join(tbl1, st_attrs1, tbl2, st_attrs2, st_granu)
    ```

    c. find all correlated attributes for a given dataset
    ```python
        find_correlation(tbl, st_attrs, st_granu)
    ```

    d. find all correlations in a data collection
    ```python
        find_all_correlations(st_granu)
    ```

These two options are compatible. Option 1 is equivalent to store the result of calling API d-`find_all_correlations` in the graph and then implement API a,b,c by querying the graph instead of re-calculation. Option 1 works well when the underlying data is static and users only query data that has been indexed.

I think it is still neccessary to have a separate set of nexus apis because not all the possible results can be precalculated and stored in the graph. First, user can select a subset of data from an existing dataset and query all correlations for this new subset of data whose result is not precalculated. Secondly, user can ask for correlations w.r.t. arbitrary correlation functions, which are not pre-calculated and stored in the graph.

## Major Todos of integrating Nexus into Ver.

### Label spatial-temporal attributes
This part needs to be integrated into the ddprofiler.

XSystem analyzer can label spatial attributes represented in geo-coordinates or zipcode; and temporal attributes with datetimes.

- [x] Test xsystem on demo datasets to see whether it can label attributes successfully and how the performance is.

After attributes are labeled, ddprofiler stores the profile of each attribute in a json file. The dindex_builder then iterates each attribute profile and builds a discovery index. However, besides column-level profile, Nexus also needs a view of dataset profiles. That is what spatial/temporal/numerical attributes are included in a dataset. This is because Nexus needs to iterate each spatio-temporal dataset to build the index subsequently.

### Aggregate and index spatio-temporal datasets

Add a new type of index `SpatioTemporalIndex` in `DiscoveryIndex`. This index contains a connection to postgres. Nexus's implementation can be migrated to `SpatioTemporalIndex`.

### Queries (API)
1. query spatial-temporal joinable datasets
```python
    joinable(tbl, st_attrs, st_granu, overlap_t) -> joinable datasets
```

2. join two spatial-temporal datasets to a given granularity
```python
    join(tbl1, st_attrs1, tbl2, st_attrs2, st_granu)
```

3. find all correlated attributes for a given dataset
```python
    find_correlation(tbl, st_attrs, st_granu)
```

4. find all correlations in a data collection
```python
    find_all_correlation(st_granu)
```

Question: Create a separate `nexus_api` or integrate these to `aurum_api`.