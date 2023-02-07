## Discovery Index Builder (dindex-builder)

This module orchestrates the functionality necessary to build the graph. It works closely with the 
discovery index store (dindex-store) to achieve that goal.

In particular, the module is in charge of building edges among nodes of the discovery graph and
storing those on the store.