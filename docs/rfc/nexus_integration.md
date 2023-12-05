# Nexus Integration

Nexus supports aligning datasets along spatio-temporal aggregation hierarchies, calculating and organizing correlations.

Major Todos of integrating Nexus into Ver.

## Label spatial-temporal attributes
This part needs to be integrated into the ddprofiler.

XSystem analyzer can label spatial attributes represented in geo-coordinates or zipcode; and temporal attributes with datetimes.

- [x] Test xsystem on demo datasets to see whether it can label attributes successfully and how the performance is.

After attributes are labeled, ddprofiler stores the profile of each attribute in a json file. The dindex_builder then iterates each attribute profile and builds a discovery index. However, besides column-level profile, Nexus also needs a view of dataset profiles. That is what spatial/temporal/numerical attributes are included in a dataset. This is because Nexus needs to iterate each spatio-temporal dataset to build the index subsequently.

## Aggregate and index spatio-temporal datasets

Add a new type of index `SpatioTemporalIndex` in `DiscoveryIndex`. This index contains a connection to postgres. Nexus's implementation can be migrated to `SpatioTemporalIndex`.

## Queries (API)
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