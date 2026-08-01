# Support consumer-owned runtime seams in v0.2

Some applications already own Raft pairing, credential activation, private attachment storage, and authenticated transport to an Eve deployment. Requiring those consumers to use Eve Raft's standalone credential and inline-attachment paths would duplicate control planes and prevent a split always-on bridge plus serverless Eve topology.

Eve Raft v0.2 will therefore expose a consumer entry point with injectable connection, Eve transport, and attachment-preparation seams. The package remains the sole owner of its durable event queue and delivery checkpoints. Consumer applications retain their existing control plane and authorization boundaries. The co-located CLI stays the default standalone path and remains backward compatible.
