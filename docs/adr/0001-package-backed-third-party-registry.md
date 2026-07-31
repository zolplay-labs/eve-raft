# Distribute through a package-backed third-party registry

Eve channels must be installed at the consumer's root `agent/channels/`, while Eve extensions cannot mount channels there. Eve Raft will therefore publish its implementation as `@zolplay/eve-raft` and distribute a thin root channel entry through a Zolplay-hosted, shadcn-style Eve registry. This preserves a normal `eve add @zolplay/raft` installation path without requiring changes to Eve first.
