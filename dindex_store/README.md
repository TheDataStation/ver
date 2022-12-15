# Discovery Index Store - dindex-store

The dindex-store component manages storage for the discovery index. While
ddprofiler computes profiles from input data, dindex-build builds an index from
those profiles, and dindex-store offers a place to store such an index.
Naturally, this is also used by downstream components that need to access the
index.
