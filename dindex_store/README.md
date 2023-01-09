# Discovery Index Store - dindex-store

The dindex-store component manages storage for the discovery index. While
ddprofiler computes profiles from input data, dindex-build builds an index from
those profiles, and dindex-store offers a place to store such an index.
Naturally, this is also used by downstream components that need to access the
index.

### Components

Brief description of what's to be found in this module.

##### common.py

Contains common definitions, including the abstract class for DiscoveryIndex. This is the class
that any implementation of the dindex-store must implement.

It also includes other definitions, such as EdgeType.

##### json_data_loader.py

Trivial. Takes JSON files (that correspond to columns of input data) and represents them as
nodes in the discovery index

##### dindex-store-XXX.py

These correspond to different implementations of the dindex-store. Different implementations
correspond to different backends.
