# Co-locate Eve and the Raft channel for v0.1

Raft delivery and credential state require a continuously running process with persistent storage, while a split deployment introduces another private protocol and failure boundary. The v0.1 release will run Eve and Eve Raft together on one persistent Railway or Docker host; split Vercel and Railway deployments remain outside the release boundary.
