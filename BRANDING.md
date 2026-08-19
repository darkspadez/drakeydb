# drakeydb branding policy

drakeydb renames only what users touch, and nothing that upstream merges or the Redis/Dragonfly
ecosystem depend on. The test for every rename: **does it break upstream mergeability, wire
compatibility, or existing tooling?** If yes, it keeps the Dragonfly name.

## Renamed

| Item | Value | Where |
|---|---|---|
| Binary | `drakeydb` (+ `dragonfly` symlink for tooling) | `src/server/CMakeLists.txt` `OUTPUT_NAME` |
| CMake project | `DRAKEYDB` | `CMakeLists.txt` |
| Startup banner / usage / `--version` text | drakeydb | `src/server/dfly_main.cc` |
| Log file names | `drakeydb.*` (follows binary name) | glog program-name derived |
| README / fork docs | drakeydb | `README.md`, `docs/` |
| Helm chart (new copy) | `contrib/charts/drakeydb/` | upstream chart left untouched |
| Release tags | `drakey-X.Y.Z` (no `v` prefix) | avoids upstream update-check pings |
| `--version_check` default | `false` | fork versions ≠ upstream releases |

## Deliberately NOT renamed

| Item | Why |
|---|---|
| `namespace dfly`, source file names | 433+ files; every upstream patch would conflict |
| `DFLY` command family, `REPLCONF capa dragonfly` | replication wire protocol — breaks interop with Dragonfly peers |
| RDB `df-ver` aux field, `df-` version prefix | on-disk / snapshot format compatibility |
| INFO `dragonfly_version` field | client libraries probe it; hottest file in the repo |
| Prometheus `dragonfly_*` metric names | existing dashboards/alerts break |
| `tests/dragonfly/` directory, `DRAGONFLY_PATH` env | test-harness surface, purely internal |
| Per-file `Copyright DragonflyDB authors` headers | legally required to stay; also line-1 conflicts everywhere |
| Debian packaging file names | deferred until the first drakeydb release is actually cut |
| CMake target name `dragonfly` | target renames ripple through upstream build patches; `OUTPUT_NAME` does the job |
