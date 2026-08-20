# drakeydb

**drakeydb** is a fork of [DragonflyDB](https://github.com/dragonflydb/dragonfly) that ports
[KeyDB](https://github.com/Snapchat/KeyDB)-style **active-replica / multi-master replication**
onto Dragonfly's modern, multi-threaded, shared-nothing architecture.

> Dragonfly's performance. KeyDB's active-active topology.

## Status

Early development. The fork tracks upstream `dragonflydb/dragonfly` continuously; multi-master
support is being built in phases:

| Phase | Feature | Status |
|---|---|---|
| 0 | Fork setup + rebrand (binary: `drakeydb`) | complete |
| 1 | Persistent node identity (UUID) + handshake | planned |
| 2 | Writable multi-source replica (fan-in) | planned |
| 3 | Origin-tagged journal + active-active mesh | planned |
| 4–6 | MVCC timestamps + convergent last-write-wins (streaming + full-sync merge) | planned |
| 7 | One-way onboarding from live KeyDB masters | planned |
| 8–9 | Mesh hardening, CI, first release | planned |

With multi-master flags off, drakeydb retains wire protocol, on-disk format, metrics, and
`INFO` compatibility with upstream Dragonfly. Runtime defaults and fork-specific surface
behavior can differ; see [BRANDING.md](BRANDING.md) for the compatibility policy.

## Planned multi-master interface

The multi-master interface is planned for Phase 2 and is not implemented yet. The following
commands illustrate the intended interface:

```bash
# Node A and node B, both writable, replicating from each other (full mesh):
drakeydb --active_replica --multi_master --port 6379   # on node A
drakeydb --active_replica --multi_master --port 6379   # on node B

redis-cli -h nodeA replicaof nodeB 6379
redis-cli -h nodeB replicaof nodeA 6379
```

Conflicts on state-carrying commands (`SET`, `DEL`, `EXPIRE`, …) resolve by per-key
last-write-wins MVCC timestamps. NTP-synchronized clocks are a hard requirement for
multi-master deployments.

## Building from source

Identical to upstream Dragonfly — see [docs/build-from-source.md](docs/build-from-source.md).

```bash
git clone --recursive https://github.com/darkspadez/drakeydb
cd drakeydb
./helio/blaze.sh -release
cd build-opt && ninja dragonfly    # CMake target keeps the upstream name...
./drakeydb --alsologtostderr       # ...the binary is drakeydb
```

Linux is the primary platform (kernel 5.11+ recommended for io_uring). On macOS, build and run
inside a Linux container.

## Relationship to upstream

drakeydb deliberately renames **only the surface**: the binary, docs, and packaging. Internal
namespaces, the `DFLY` command family, replication wire tokens, RDB format markers, Prometheus
metric names, and the `dragonfly_version` INFO field all stay unchanged for ecosystem
compatibility and to keep upstream merges cheap. See [BRANDING.md](BRANDING.md) for the full
policy and [docs/UPSTREAM-SYNC.md](docs/UPSTREAM-SYNC.md) for how upstream patches are merged.

## License

drakeydb inherits Dragonfly's [Business Source License 1.1](LICENSE.md). It converts to Apache
2.0 on Nov 1, 2030, or on the fourth anniversary of the first public distribution of a specific
version, whichever comes first. The Additional Use Grant permits use only as part of your own
product or service when it is not an in-memory data store product or service, and prohibits using,
providing, distributing, or making the Licensed Work available as a Service to third parties.
See [LICENSE.md](LICENSE.md) for the complete applicable terms. Ported KeyDB logic is
BSD-3-Clause; see [NOTICE](NOTICE) for attributions.

## Credits

- The [DragonflyDB](https://www.dragonflydb.io) team — the fastest in-memory store engine there is.
- [KeyDB](https://docs.keydb.dev/docs/active-rep) by EQ Alpha Technology / Snap Inc. — the
  active-replication design this fork ports.
