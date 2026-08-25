# Upstream sync workflow

drakeydb tracks `dragonflydb/dragonfly` continuously. Fork changes are kept **additive and
flag-gated** so that with `--active_replica` off, behavior and journal bytes are identical to
upstream — which keeps merges cheap.

## Remotes

```bash
git remote -v
# origin    git@github.com:darkspadez/drakeydb.git
# upstream  https://github.com/dragonflydb/dragonfly
```

One-time per clone (activates the `.gitattributes` merge=ours entries):

```bash
git config merge.ours.driver true
```

## Merging upstream

Cadence: monthly, plus after each upstream release.

```bash
git fetch upstream
git checkout -b merge/upstream-$(date +%Y%m%d) main
git merge upstream/main
# resolve conflicts (see watchlist below), then:
git submodule update --init --recursive
```

Verification gate before the merge PR lands:

1. Build: `./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF && ninja -C build-dbg -j4 dragonfly`
2. C++ tests: `(cd build-dbg && ctest -L DFLY)` (at minimum `journal_test`, `dragonfly_test`, `server_family_test`)
3. Replication pytest subset: `(cd "$(git rev-parse --show-toplevel)" && python3 -m pytest tests/dragonfly/replication_test.py -x)`
4. Multi-master suite: `(cd "$(git rev-parse --show-toplevel)" && python3 -m pytest tests/dragonfly/multimaster_test.py -x)` (once it exists)

## Conflict watchlist

Files where drakeydb carries real logic changes — review upstream's side carefully instead of
taking either side blindly:

| File | drakeydb change |
|---|---|
| `src/server/journal/serializer.cc` | journal v2 framing (origin/mvcc extension). **Hazard:** the reader HARD-ERRORS on a framing version outside `{1, 2}` (`errc::illegal_byte_sequence`) — if upstream ever repurposes that deprecated varint field with a `3`, every drakeydb replica fails to connect to it until this reader is updated to recognize the new value |
| `src/server/journal/types.h`, `types.cc`, `journal_slice.cc`, `streamer.*`, `executor.*` | origin tagging + peer filtering; `journal::PassesPeerEchoFilter` is the SINGLE shared predicate — if upstream churn forces a copy, the copies must stay identical or stable sync and full sync silently diverge |
| `src/server/main_service.cc` | `PrepareTransaction` origin hook (P3) — note EXEC's `dist_trans` and the non-atomic squash stub bypass it and inherit origin separately |
| `src/server/snapshot.cc`, `rdb_save.*`, `rdb_load.*` | full-sync window: peer filter on the concurrent journal blob, and `SetApplyOrigin` on the loader |
| `src/server/journal/tx_executor.cc` | peer-mode authoritative `Op::LSN` adoption (filtered streams desync a counting receiver) |
| `src/server/node_identity.cc` | uuid persistence via `link()` for atomic EEXIST — do NOT "simplify" to `O_CREAT|O_EXCL`+write, which exposes a zero-length uuid file to a concurrent reader and turns into `exit(1)` |
| `src/server/replica.cc` | peer-mode (writable, no-flush) sync path; hooks: no shard flip/flush, sync gate, self/duplicate uuid guard + claim release |
| `src/server/main_service.h`, `main_service.cc` | exclusive LOADING reservation for peer full sync |
| `src/server/dflycmd.cc` | peer version/UUID gating; `DFLY TAKEOVER` refusal in active mode |
| `src/server/server_family.cc` | REPLICAOF delegation + INFO additions (kept to additive lines); `--replicaof` list parser + active-mode REPLICAOF/INFO/REPLCONF/REPLTAKEOVER hooks, peers_ member |
| `src/server/transaction.cc`, `tx_base.cc` | journal origin context |
| `src/server/version.h` | upstream protocol versions — **always take upstream**, fork uses 65 |
| `src/server/dfly_main.cc` | banner/usage strings, `version_check` default; `ValidateReplicaOfFlags()` + `ValidateMultiMasterFlags()` calls |
| `src/server/CMakeLists.txt` | `OUTPUT_NAME drakeydb` + symlink lines; `peer_replication.cc` + `peer_replication_test` |
| `tests/dragonfly/instance.py` | per-instance `--node_uuid` default in `DflyInstanceFactory.create()` |

Files under `merge=ours` (`.gitattributes`): `README.md`.
Deleted-by-fork workflows (`release.yml`, `docker-release2.yml`, `generate-osrepo-site.yml`):
on merge, git may resurrect them as add/delete conflicts — keep them deleted
(`git rm` again).

## Rules

- Never rename `namespace dfly`, wire tokens, or metric names (see `BRANDING.md`).
- Never edit `helio/` here — it is upstream's submodule; bump the pointer only via upstream merges.
- New fork code goes in **new files** where possible (`multi_master.*`, `node_identity.*`).
- `DflyVersion` in `version.h` belongs to upstream; the fork's replication capability constant
  (`kDrakeydbReplVersion = 65`) lives in `node_identity.h` and must stay far above upstream's.
