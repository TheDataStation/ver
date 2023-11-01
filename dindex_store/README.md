# Discovery Index Store - dindex-store

The dindex-store component manages storage for the discovery index. While
ddprofiler computes profiles from input data, dindex-build builds an index from
those profiles, and dindex-store offers a place to store such an index.
Naturally, this is also used by downstream components that need to access the
index.

### Components

Brief description of what's to be found in this module.

#### common.py

Contains common definitions, including the abstract classes for discovery index
(ProfileIndex, GraphIndex, and FullTextSearchIndex). These are the classes that any
implementation of the dindex-store must implement.

It also includes other definitions, such as EdgeType.

#### json_data_loader.py

Trivial. Takes JSON files (that correspond to columns of input data) and represents them as
nodes in the discovery index

#### graph_index_XXX.py

These correspond to different implementations of the GraphIndex. Different implementations
correspond to different backends.

#### profile_index_XXX.py

These correspond to different implementations of the ProfileIndex. Different implementations
correspond to different backends.

Implementations of the ProfileIndex will read the profile schema that is shared with ddprofiler
and convert it to a format that is appropriate for the backend. The profile schema is a file that
contains the configuration of the different analyzers that are to be used to compute the profiles.
The file is called `profile_schema.yml` and is located in the root folder of the project.

#### full_text_search_index_XXX.py

These correspond to different implementations of the FullTextSearchIndex. Different implementations
correspond to different backends.
