# Task 1.4 report: `capa activeExpire` and replica active expiry

Branch `feat/phase7-1-rreplay-unwrap`, started at HEAD `265d241`. Not committed (the orchestrator
commits). Implementer: Sonnet. Brief: `brief-task-1.4.md` (it carries the advisor's design-check
corrections, which win over the plan text); plan Task 1.4; spec D-2 and D-9; ledger decisions 13 and 24.
Scratch material (good copies of every touched file, each falsification's raw output, build and run logs)
is in the orchestrator's scratchpad under `p74/`.

## Summary

- A plain replica of an active KeyDB expires keys itself. The node's main link sets a per-shard flag
  (`EngineShard::replica_active_expiry_`) after a successful `Greet()` that saw `active-replica`; the flag
  opens the heartbeat's expiry sweep on a replica and the replica gate of `DbSlice::ExpireIfNeeded`, and
  never eviction. A master that does not say `active-replica` (Redis, Valkey, stock Dragonfly, a non-active
  KeyDB) never sets it, and sees the handshake it always saw.
- `Greet()` sends `REPLCONF capa activeExpire` as its own command, once per `Greet()`, right after the first
  capa reply that reveals `active-replica`, at either of the two capa sites. Peer links send it too and never
  touch the shards.
- 5 new gtests (`ReplicaActiveExpiryTest`, `classic_replay_test` went from 66 to 71) and 12 new pytest cases
  (`keydb_onboarding_test.py` went from 60 to 72). Every behavior is falsified (tables below).
- Owner decision 24 (the lag window) is written down in spec D-9 and `docs/PLAN.md`; `docs/multi-master.md`
  has no KeyDB section yet (P7-4 adds it), so, as the brief says, D-9 and the PLAN note carry it.

## What changed

| File | Change |
|---|---|
| `src/server/engine_shard.h` | `replica_active_expiry_` with `SetReplicaActiveExpiry`/`ReplicaActiveExpiry` beside `is_replica_`; `class ReplicaActiveExpiryTest` forward-declared and made a friend (the fixture drives `Heartbeat()`). |
| `src/server/engine_shard.cc` | `Heartbeat` gates on `!IsReplica() \|\| replica_active_expiry_`; `RetireExpiredAndEvict`'s `eviction_goal` is `(!IsReplica() && FLAGS_enable_heartbeat_eviction) ? CalculateEvictionBytes() : 0`. |
| `src/server/db_slice.cc` | `ExpireIfNeeded`'s replica gate gains `!owner_->ReplicaActiveExpiry()`. |
| `src/server/replica.h`, `replica.cc` | `Greet()`: a lambda sends `REPLCONF capa activeExpire` once, called after each capa check; `Replica::ApplyReplicaActiveExpiry(bool)` (no-op for a peer-mode link and for a link not marked as the main one), called after `SetShardStates(true)` if `R_GREETED`, after a successful in-loop `Greet()` and after `SetShardStates(false)`; `SetMainLink()`/`main_link_`. |
| `src/server/server_family.cc` | `new_replica->SetMainLink()` in `ReplicaOfInternal` (one line); `AddReplicaOf`'s links stay unmarked. |
| `src/server/classic_replay_test.cc` | `ReplicaActiveExpiryTest` fixture and 5 tests. |
| `tests/dragonfly/keydb_onboarding_test.py` | 4 tests (12 cases), `assert_dbsize`, `write_expiring_keys`, `capa_requests`. |
| `tests/dragonfly/fake_classic_master.py` | `script_capa_reply(reply, only_for=None)`: with `only_for` (a capability word) only the capa requests naming it get `reply`, so the second capa site can be told apart. |
| `tests/dragonfly/instance.py` | `KeyDBServer.log_text()`: the whole log. |
| `tests/dragonfly/multimaster_test.py` | A comment in `test_greet_accepts_capa_reply_with_capability_words`: its `active` replies now make that replica send `activeExpire` and turn the flag on. No assertion changed. |
| `docs/UPSTREAM-SYNC.md` | New rows for `engine_shard.{h,cc}` and `db_slice.cc`; the `replica.{h,cc}` and `server_family.cc` rows gain their Task 1.4 hunks, with exact counts (`replica`: 7 hunks in the `.cc`, 4 in the `.h`; `engine_shard`: 2 and 3; 1 each in `db_slice.cc` and `server_family.cc`) and the tests to re-run. |
| spec D-2, D-9, D-15, file map, risk 8; `docs/PLAN.md`; plan Task 1.4 | D-9 rewritten as built (seam, gate, main-link marker, apply sites, failed-`Greet()` window, journaling, decision 24); D-2's capa bullet; D-15 rows with their falsifications; PLAN's corrections bullet and the `replica.h/.cc` row; the plan's "as built" paragraph. |

## Design notes and deviations

- **Anchors, re-checked by reading at HEAD:** every one in the brief's item 1 is where it says (the heartbeat
  gate `engine_shard.cc:847`, `eviction_goal` `:885`, `db_slice.cc:2095-2098` and `:2681`, `:1084`/`:1096`; the
  `replica.cc` capa sites, `SetShardStates` calls and `R_GREETED` sites). Nothing else reads `IsReplica()`
  (grep over `src/server`), so the four gates in `db_slice.cc`, the heartbeat and the loader are the whole set.
- **Seam as the advisor said**, not the plan's `expire_only` parameter: no new argument, `CalculateEvictionBytes`
  (which mutates `eviction_state_`) never runs on a replica, and `db_slice.cc:2681`'s `DCHECK` is untouched.
- **Main-link marker, not `!slot_range_`**: a setter, `SetMainLink()`, called by `ReplicaOfInternal` right after
  constructing `replica_`, rather than a constructor parameter: one line in an upstream-hot file, and the two
  trailing constructor parameters stay as they are. It must be called before `Start()`/`EnableReplication()`
  (it is). Pinned: the `with_slot_range` pytest case attaches with `REPLICAOF <host> <port> 0 16383` and
  still expires (P3 below: the `!slot_range_` predicate fails it).
- **Both reads of `master_active_replica_` are after `R_GREETED`**: `MainReplicationFb` applies the value from
  `Start()`'s `Greet()` only `if (state_mask_ & R_GREETED)` (a `--replicaof` link is greeted by the fiber, which
  applies it after the in-loop `Greet()` succeeds). A failed `Greet()` neither sets nor clears; the window that
  leaves (a KeyDB replaced by Redis at the same host:port keeps self-expiring until the first successful greet)
  is in D-9.
- **`REPLCONF capa activeExpire`** is a lambda with a local sent bit, called after the site-1 check and inside the
  site-2 `Redis` branch. Its reply is read through the same `read_capa_reply` (so `ok` is `ParseCapaReply(...).ok`)
  and a non-OK is a `LOG_FIRST_N` warning; a transport error still returns, as for any other command. Real KeyDB
  sends the same capability inside one batch (`capa eof capa psync2 capa activeExpire`); KeyDB only sets a bit
  on that client for `activeExpire`, so a separate command is equivalent, and the real-KeyDB log test shows it.
- **Fixture sets `--hz 0`** before `BaseFamilyTest::SetUp`, so no periodic heartbeat runs beside the ones the test
  drives; the shards' state is set directly (`SetReplica(true)` alone leaves `ServerState::is_master` true, so
  `Run({"set", ...})` still works, as the brief noted). `friend class` rather than a `TEST_` hook: no production
  API surface.
- **Memory pressure in the gtest**: data is populated at the harness's `INT_MAX` limit, then `max_memory_limit`
  is set to 1000 bytes (usage far above it), so every heartbeat on a master would evict. A control test shows that
  it does (`evicted_keys > 0`), so "never evicts" on a replica is not an inert setup.
- **DBSIZE, not stats, everywhere**: `DBSIZE` counts entries and reads no key; `result.deleted` is bumped even when
  the gate returns early, and an expired key is deleted by the read that asks for it (a replica with the flag, a
  Redis or a KeyDB master), so any other observation would hide what the sweep did.
- **Deviations from the brief's test list**
  - `test_plain_replica_of_active_keydb_expires_keys` writes `PX 3000`, not `PX 500`: the test first waits for all
    the keys to arrive (DBSIZE N+2) before any is due, which 500 ms could race on a loaded runner. It is also
    parametrized with the slot-range attach (`plain`, `with_slot_range`).
  - The Redis control, after its negative check, reads one key on the master (lazy expiry propagates a `DEL`) and
    waits for the replica's DBSIZE to drop by exactly that key: proof that the link was live, so the unchanged DBSIZE
    is not a dead link's.
  - `test_greet_sends_capa_active_expire_only_after_active_replica_reply` covers four reveal patterns (never,
    both sites, first site only, second site only) x plain and peer node, which needed `script_capa_reply(only_for=)`
    in the fake master. Its wait is "the master saw the `PSYNC`" (the handshake is over), not `wait_for_synced_link`:
    a peer link to a stock master does not print `repl_offset` (D-13), so that helper cannot wait there.
  - The real-KeyDB half is its own test, `test_keydb_is_told_the_replica_expires_keys_itself`, with a `log_text()`
    reader added to `KeyDBServer` (there was only `log_tail`).
  - No `proxy.py` change was needed (the brief said so).
- **KeyDB facts checked in its source, for the docs**: an active KeyDB propagates no expiry `DEL`
  (`propagateExpire`, `db.cpp`: "Active replicas do their own expiries"); a plain KeyDB replica of an active
  master expires its own keys in its sweep (`expireOwnKeys`, `server.cpp`, via `mi->isActive`) while its read path
  only hides an expired key (`expireIfNeeded` returns 1 without deleting). So the read path of this fork now
  differs from KeyDB's on such a replica (it deletes), as D-9 says.

## Falsification

Gtests: the named line was changed in the source, `cd /home/user/drakeydb/build-dbg && nice ninja -j3
classic_replay_test`, the test run, the source restored and `cmp`-ed against the good copy.

| Mutation | Run | Result |
|---|---|---|
| F1: `Heartbeat` gate back to `if (!IsReplica())` | `./classic_replay_test --gtest_filter='ReplicaActiveExpiryTest.<Case>*'` | `ReapsExpiredKeysButNeverEvicts`: `DbSize() Which is: 200` vs `kPlainKeys` 100, twice. `BypassesReplicaDeleteExpiredFlag`: the read path deleted one (`Which is: 199`), the sweep none (199 vs 100). The controls and `ReapsNothingWithoutTheFlag` pass. |
| F2: `ExpireIfNeeded` gate without `ReplicaActiveExpiry()` | same | `BypassesReplicaDeleteExpiredFlag`: `Run({"get","ttl:0"})` `Expected: is nil  Actual: '....'` (the 1000-byte value, served as live), then `200` vs `199`, then `200` vs `100` (the sweep's `ExpireIfNeeded` returned early although it counted the keys as deleted). `ReapsExpiredKeysButNeverEvicts` passes, as it should: it runs with the default `--replica_delete_expired=true`, so only the flag-off test sees this gate. |
| F3: eviction allowed on a replica (`eviction_goal` without `!IsReplica() &&`) | `--gtest_filter='ReplicaActiveExpiryTest.ReapsExpiredKeysButNeverEvicts'` | exit 134: `db_slice.cc:2683] Check failed: !owner_->IsReplica()`, stack `FreeMemWithEvictionStepAtomic <- RetireExpiredAndEvict <- Heartbeat <- ReplicaActiveExpiryTest::RunHeartbeats`. |
| F4: sweep on every replica (`if (true)`) | `ReapsNothingWithoutTheFlag*`, `ControlWithoutTheFlag*`, `ReapsExpiredKeys...` | `ReapsNothingWithoutTheFlag`: `DbSize()` 100 vs 200 (the keys were reaped without the flag). `ControlWithoutTheFlag...` passes (its `--replica_delete_expired=false` still holds the read-path gate); `ReapsExpiredKeys...` passes. |
| F5: no eviction at all (`false && ...`) | `ControlAMasterHeartbeatUnderTheSamePressureEvicts` | `Expected: (GetMetrics().events.evicted_keys) > (0u), actual: 0 vs 0`: the control is not vacuous. |

Pytests: the named change in `replica.cc`, `ninja -j3 dragonfly`, `flock /tmp/drakey-pytest.lock
/root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k "<expr>"` with
`KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`,
the source restored, **and `dragonfly` rebuilt** after the last one (see "A stale binary" below).

| Mutation | `-k` | Result |
|---|---|---|
| P1: the shard flag is never set (`SetReplicaActiveExpiry(false && enabled)`) | `expires_keys or never_expires` | 2 failed, 2 passed: both `test_plain_replica_of_active_keydb_expires_keys` cases fail at `assert_dbsize(c, 2)` with `assert 52 == 2` (KeyDB's own DBSIZE had already reached 2, the replica's stays at 52). The Redis control passes. |
| P2: `activeExpire` never sent (`if (true \|\| ...) return {};`) | `expires_keys or never_expires or capa_active_expire or told_the_replica` | 7 failed, 5 passed: the six active greet cases (`assert 0 == 1` on `capas.count(active_expire)`) and `test_keydb_is_told_...`, whose log is `Warning: replica 127.0.0.1:38681 does not support active expiration.  This client may not correctly process key expirations.` The two KeyDB-expiry cases and the control pass: an active KeyDB sends no expiry `DEL` either way, and the capability only silences its warning. |
| P3: `slot_range_.has_value()` instead of the marker | `expires_keys` | `with_slot_range` fails (`assert 52 == 2`), `plain` and the KeyDB-log test pass. |
| P4: `activeExpire` sent to every master (`if (active_expire_sent) return {};`) | `capa_active_expire` | 4 failed: both `never` cases (`assert ['REPLCONF','capa','activeExpire'] not in [[... 'eof' ...], ['REPLCONF','capa','activeExpire'], ['REPLCONF','capa','dragonfly']]`) and both `second_site_only` cases (sent after site 1, not after the reply that revealed it). |
| P5a: sent from site 1 only | `capa_active_expire` | `second_site_only` x2 fail. |
| P5b: sent from site 2 only | `capa_active_expire` | `both_sites` x2 and `first_site_only` x2 fail. |
| P6: the once-per-`Greet()` bit dropped | `capa_active_expire` | `both_sites` x2 and `first_site_only` x2 fail (two `activeExpire`s). |
| P7: the flag set whatever the master (`SetReplicaActiveExpiry(true \|\| enabled)`) | `never_expires` | fails: `assert 2 == (50 + 2)`: the replica of a plain Redis, which still holds all 52 keys, drops its 50. |

Not falsified individually: the peer-mode early return and the `main_link_` check of `ApplyReplicaActiveExpiry` (no
observable: a peer node expires as a master either way, and an `ADDREPLICAOF` link needs cluster mode and a
KeyDB that speaks it); the clear after a successful greet of a master that stopped saying `active-replica`.

### A stale binary, found and corrected

Each pytest falsification restores the source but not the binary. After P7 I ran the `dragonfly` target's
dependents (the gtests) and then a whole-file pytest run **on the P7 binary**: its one failure was
`test_plain_replica_of_plain_redis_never_expires_on_its_own`, exactly what P7 predicts. I noticed because the same
test had passed twice in isolation earlier, reran it (4 failures in a row), then rebuilt `dragonfly` from the
restored sources (`cmp` against the good copies) and reran: it passes, twice. Every "Results" line below is from a
binary built after the last restore; the P7 run is the P7 row above and is not counted as a failure.

## Results (debug build, gcc, `nice ninja -j3 dragonfly classic_replay_test multi_master_test dragonfly_test generic_family_test multi_test server_family_test`, rebuilt after the last source edit, no `warning` in the log)

- `./classic_replay_test`: `[  PASSED  ] 71 tests.`
- `./multi_master_test`: `[  PASSED  ] 222 tests.`
- `./dragonfly_test`: `[  PASSED  ] 54 tests.` (includes `Bug207`, the heartbeat-eviction family)
- `./generic_family_test`: `[  PASSED  ] 84 tests.` (the expiry tests)
- `./multi_test`: `[  PASSED  ] 67 tests.`
- `./server_family_test`: 43 passed, 1 failed, `ServerFamilyTest.GetTcpSocketInfoIPv6` (no IPv6 in the sandbox, as before).
- `keydb_onboarding_test.py` and `keydb_harness_test.py`, real KeyDB, `KEYDB_REQUIRED=1`: `93 passed in 135.52s`
  (72 + 21).
- `redis_replication_test.py` (default `-m "not large"`): `12 passed, 7 deselected`.
- `multimaster_test.py -k "greet or capa or classic or keydb or expir"`: `20 passed, 57 deselected`.
- `replication_test.py -k expir`: `2 passed, 62 deselected` (the upstream expiry-on-replica tests, unchanged).
- `pre-commit run --files <the 15 changed files>`: `pyflakes`, `trim trailing whitespace`, `fix end of files`,
  `check python ast`, `Clang formatting`, `black` all Passed, no file changed.

## Not done / not verified

- No release, ASAN or UBSAN build; the full `ctest -L DFLY` and the whole of `replication_test.py` were not run.
- **Journaling of the replica's expiry `DEL` under `--experimental_cascaded_partial_sync` with a sub-replica** is the
  existing path (`journal_deletions = true`, `owner_->journal()`), not exercised here; D-9 states it from the code.
- **An `ADDREPLICAOF` link never driving the flag** and **a peer link never touching it** hold by construction
  (`ApplyReplicaActiveExpiry` returns for both) and are untested: neither has an observable in a test this cheap.
- **The failed-`Greet()` window** (no set, no clear) is documented, not tested.
- The sweep's per-heartbeat budget (`DeleteExpiredStep`'s traversal quota) makes reaping N keys take more than one
  heartbeat; the pytests poll for up to 10 s, the gtests loop up to 200 heartbeats. No throughput figure was taken.

## Open risks

1. **The lag window (owner decision 24, accepted):** a TTL extension that reaches the replica after it expired the
   key is a no-op there while KeyDB keeps the key, forever after `PERSIST`; stream lag and clock skew widen it.
   Documented in D-9 and PLAN, not yet in an operator page (P7-4).
2. **The read path deletes on such a replica whatever `--replica_delete_expired` says**, unlike KeyDB's, which only
   hides. A harness that compares a KeyDB-fed replica's data with its master's by reading keys sees the deletion.
3. **`SetMainLink()` must be called by any future code that constructs the node's main `Replica`.** A new creation
   site that forgets it silently turns the feature off for that link (the `with_slot_range` and KeyDB pytests catch it
   for `REPLICAOF`; not for another path).
4. **Upstream conflicts** in `Heartbeat`/`RetireExpiredAndEvict` (two gates) and `ExpireIfNeeded` (one expression):
   the UPSTREAM-SYNC rows say what to re-run; resolving to upstream's side re-opens either "the replica never expires"
   or "eviction reaches a replica" (the latter aborts `ReapsExpiredKeysButNeverEvicts` on the `DCHECK`).
5. **A KeyDB replaced by a Redis at the same address** keeps expiring until the first successful greet clears it (a
   full resync follows). Documented.

## Review fix round

Base: HEAD `e8f28b7` (Task 1.4 as committed). Opus's review: approve once Important #2 is fixed, no
Critical. Not committed. Scratch (good copies, raw falsification output, run logs) is under `p74r/`.

**Important #1 is not touched, as instructed**: spec D-9's lag-window text, `docs/PLAN.md` and
`decisions.md` still say what they said; the owner decides, and an advisor is scoping a code option.

### What changed

| Item | Change |
|---|---|
| Important #2, the in-loop apply | `test_plain_replica_of_active_keydb_expires_keys` gains `boot_replicaof` (`df_factory.create(replicaof=f"localhost:{keydb.port}")`): its first greet is the fiber's, so the flag can only come from the in-loop apply. New `test_replica_active_expiry_follows_the_master_of_every_reconnect` (fake master, reconnects). |
| Minor 3, `classic_master_` | Both apply sites pass `classic_master_ && master_active_replica_` (INFO's pair). Tests: `test_dfly_master_that_says_active_replica_never_turns_replica_expiry_on[replicaof_command\|boot_replicaof]`, one per site. |
| Minor 4, flags | `absl::FlagSaver saver_;` in `ReplicaActiveExpiryTest`, and a check in its `SetUp` that `hz` and `replica_delete_expired` are at their defaults on entry. |
| Minor 5, lenient reply | `test_greet_survives_any_reply_to_capa_active_expire[error\|two_element_array\|integer]x[plain_replica\|peer_mode]`. `FakeClassicMaster.script_capa_reply(reply, only_for=)` became per-word rules (one call per word, the default still set without `only_for`), so `activeExpire` can get its own reply while the first capa reply says `active-replica`. |
| Minor 6, comment | `engine_shard.cc`: the `eviction_goal` comment no longer says `eviction_state_` never advances on a replica: `CalculateEvictionBytes` advances it, and the store guarded by `track_deleted_bytes` at the end of the function can still run on a node that was a master, writing only `deleted_bytes_at_prev_eviction`, which `CalculateEvictionBytes` alone reads. No code change. |
| Minor 7, docs | D-9: `mi->isActive` is also reset when a master is created (`server.cpp:3251`); the flag is per shard, so keys arriving through `ADDREPLICAOF` links are self-expired too while the main link has it on; the flag's trigger is `classic_master_ && master_active_replica_` (and D-2's "only trigger" sentence says so). `engine_shard.h`: the flag is read by `Heartbeat` and by `DbSlice::ExpireIfNeeded`. D-15 rows and the `replica.cc` UPSTREAM-SYNC row follow. |

### Design notes and deviations

- **The reconnect variant uses a fake master, not a swap of real servers.** The reviewer's ad hoc check swapped KeyDB
  for a Redis with active expiry off at the same address and back. The test does the same thing with
  `FakeClassicMaster` on one address: `active-replica`, then plain `+OK`, then `active-replica`, each reconnect (the
  master drops the link, `serve(...)` re-scripts capa and PSYNC) a full resync whose stream sets keys with `PX 3000`.
  No process restarts, no port reuse, deterministic; it is the in-loop `Greet()` of the same `Replica` each time.
  A real Redis/KeyDB swap was not added.
- **The `classic_master_` test is not the one the brief sketched.** A fake master answering `capa dragonfly` with the
  single string `+OK active-replica` is, to `Greet()`, a classic master (the replica takes the Redis branch on a
  one-element reply) and *should* set the flag: that is the existing `second_site_only` case. To have
  `master_active_replica_` true and `classic_master_` false the first capa reply must say it and the second must be a
  real Dragonfly array, so the test is a real Dragonfly master (`--hz=0`, so it sweeps nothing; nothing reads a key)
  behind the proxy, whose `override_next_response(b"REPLCONF capa eof", b"+OK active-replica\r\n")` makes the first
  reply say it. The replica, which then also sends `capa activeExpire`, must keep every key, like the master.
- **Both `classic_master_` conjuncts are pinned**, because the test has an attach variant per site (the `REPLICAOF`
  command is applied by the top of `MainReplicationFb`, `--replicaof` by the in-loop apply).
- **The flags check in `SetUp` is the test of the `FlagSaver`**: `--gtest_repeat=2` cannot show a leak (the leaked flags
  do not break the other tests), the ordering of the fixture's own tests can.

### Falsification

Pytests: the named change in `replica.cc`, `ninja -j3 dragonfly`, `pytest tests/dragonfly/keydb_onboarding_test.py -k "<expr>"`
(`KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly`,
under `flock /tmp/drakey-pytest.lock`), the source restored; `dragonfly` was **rebuilt after the last one** and every run
below "Results" is on that rebuild.

| Mutation | `-k` | Result |
|---|---|---|
| FA: the in-loop apply deleted (`:340`) | `boot_replicaof or every_reconnect` | 2 failed: `boot_replicaof` at `assert_dbsize(c, 2)` with `assert 52 == 2` (the flag was never set, that link is greeted only by the loop); the reconnect test's middle phase, `a replica of a plain master expired keys` (`assert 1 == (50 + 1)`: the flag stayed on from the first phase). |
| FA2: the in-loop apply always clears (`ApplyReplicaActiveExpiry(false)`) | same | 2 failed: `boot_replicaof` `assert 52 == 2`; the reconnect test's last phase, `assert_dbsize(c, 1)` with `assert 51 == 1` (the next greet no longer sets it). |
| FB1: first apply site without `classic_master_` | `never_turns_replica_expiry_on` | `replicaof_command` fails, `boot_replicaof` passes. |
| FB2: in-loop site without `classic_master_` | same | `boot_replicaof` fails, `replicaof_command` passes. (FB1's first run, before the test had an attach variant per site, failed with `the replica expired keys on its own`, `assert 2 == (50 + 2)`.) |
| FD: the `activeExpire` reply parse strict (`PC_RETURN_ON_BAD_RESPONSE(read_capa_reply().ok)`) | `survives_any_reply or capa_active_expire` | 6 failed, 8 passed: every `survives_any_reply` case, `redis.exceptions.ResponseError: replication cancelled` (the `Greet()` failed); the eight greet cases, whose reply is an OK, pass. |
| FC: `FlagSaver` removed from the fixture | `./classic_replay_test --gtest_filter='ReplicaActiveExpiryTest.*'` | the first test passes, the next four fail in `SetUp`: `hz` is `"0"`, default `"100"`; the last also `replica_delete_expired` `"false"`, default `"true"`. |

Not falsified: a *desync* of the handshake after `activeExpire` (the sequence assertion and `connection_count == 1` would catch
one; no mutation makes `Greet()` misread the reply's length).

### Results (debug build, rebuilt after the last mutation)

- `./classic_replay_test --gtest_repeat=2`: `[  PASSED  ] 71 tests.` twice.
- `keydb_onboarding_test.py`, the whole file, real KeyDB, `KEYDB_REQUIRED=1`: `82 passed in 177.39s` (72 before, 10 new cases:
  `boot_replicaof` 1, reconnect 1, `survives_any_reply` 6, DFLY master 2).
- `multimaster_test.py -k "greet or capa or classic or keydb or expir"`: `20 passed, 57 deselected`.
- `./multi_master_test`: `[  PASSED  ] 222 tests.` (rebuilt after the `replica.cc` change).
- `pre-commit run --files <the changed files>`: `pyflakes`, `trim trailing whitespace`, `fix end of files`, `check python ast`,
  `Clang formatting`, `black` all Passed, no file changed.

### Not done

- The other gtest binaries (`dragonfly_test`, `generic_family_test`, `multi_test`, `server_family_test`) and
  `keydb_harness_test.py`, `redis_replication_test.py`, `replication_test.py -k expir` were not rerun: this round changes a comment in
  `engine_shard.{h,cc}`, one predicate in `replica.cc` and tests only.
- A real Redis/KeyDB swap at one address, and the `classic_master_` conjunct for a *reconnect* from a classic master to a Dragonfly
  one (the in-loop site is exercised on a first greet by `boot_replicaof`, not after a swap), stay untested.

## Decision 24 revision

Base: HEAD `29e493d` (ledger decisions 24 revised, 27 and 28 recorded). Not committed. Scratch (the good copy of
`db_slice.cc`, raw falsification output in `falsify/`, the loaded runs, the two driver scripts, a shallow clone of
upstream) is in the orchestrator's scratchpad under `p71/`. Nothing in `helio/` was touched.

### What changed

| File | Change |
|---|---|
| spec D-9 | The lag-window bullet is replaced by: decision 28 (`--replica_delete_expired=false` stays ignored); the rule (first access after the replica's own per-command clock passes the absolute expiry, else the sweep); when it diverges (both directions); the band; the affected classes and the unaffected ones; the equivalences; the contrast with KeyDB's own **plain** replica (never deletes on access, writes land on the stale object, only the slow `activeExpireCycle` reaps, options B and C rejected as wider); decision 27's mechanism (seam, 60 s rule, the 1 ms edge); and **what it leaves open** (the sweep). |
| spec D-15, PR stack, file map, risks | D-15: two rows for the new P7-1 pins (with the observed falsification values) and two for Task 2.8's tests. The P7-2 PR row, the file map (`transaction.{h,cc}`, `main_service.cc`, new `conn_context.h` row), the churn note and a new risk 14. |
| `docs/PLAN.md` | The Task 1.4 bullet carries the same account instead of "KeyDB's own plain replicas share it". |
| plan, new Task 2.8 "Envelope-time apply" | Goal, files, interfaces, the fallback rule and why, the 1 ms edge, the interaction with Task 2.2's stamps, a clock-read audit step, failing tests, falsification list, done line. The P7-2 gate line and Task 4.4's docs line mention it. |
| `docs/UPSTREAM-SYNC.md` | `db_slice.cc` row: upstream `main` (`36f511b`) split `ExpireIfNeeded` into `IsExpired()`/`Expire()` and moved the replica gate into `Expire()` (`:1525`, `:1539`, `:1544`); read from a shallow clone, not from memory. |
| `src/server/classic_replay_test.cc` | `ReplicaActiveExpiryTest.ARefreshThatArrivesAfterTheDeadlineFindsNoKey`, `.ControlARefreshBeforeTheDeadlineKeepsTheKey`, `.EveryCommandClassOfTheWindowSeesNoKey` (74 tests in the binary, from 71). |
| `tests/dragonfly/keydb_onboarding_test.py` | `test_plain_replica_of_active_keydb_loses_a_ttl_refresh_that_arrives_after_the_deadline[raw\|envelope]x[sweep\|access_only]` and `test_plain_replica_of_active_keydb_recomputes_a_counter_from_nothing_after_the_deadline[raw\|envelope_applied_after_the_deadline\|envelope_applied_before_the_deadline]x[sweep\|access_only]`: 10 cases (92 in the file, from 82). |
| `tests/dragonfly/fake_classic_master.py` | `send_stream(data)`: writes into the stream of the connection last answered a PSYNC, so a test can time a command against the clock. Nothing else changed. |

### Disagreements with the analysis in the brief

1. **TTLs on the wire from an active KeyDB are absolute, so the "relative against absolute" refinement does not
   apply to the case this decision is about.** The analysis read the command-level propagation (`t_string.cpp:120-133`,
   `expire.cpp:776-781`: `SET .. PX <relative>`, `EXPIRE` verbatim). An active master's feed does not send those:
   `replicationFeedSlave` (`replication.cpp:521`) and the RREPLAY builder (`:658`) pass every command through
   `catCommandForAofAndActiveReplication` (`aof.cpp:682-726`) whenever `fSendRaw = !fActiveReplica` is false (`:584`),
   which makes `EXPIRE`/`PEXPIRE`/`EXPIREAT` `PEXPIREAT <ms>` and `SET .. EX/PX` `SET .. PXAT <ms>`. Spec D-1.14 already said
   so ("`SET` with `EX/PX/EXAT/PXAT` propagates as exactly `SET k v PXAT <abs>`"). The live capture proves it; I decoded the
   bytes of `tests/dragonfly/data/keydb_v6.3.4_rreplay_stream.bin` rather than trusting its README: segment 2 is `SET k2 v2
   PXAT 1791058569837` (issued `EX 100`), segment 6 `INCRBY c 1` (issued `INCR c`), segment 12 `SET expiring v PXAT
   1791058475348` (issued `PX 200`). Better still, the envelope's `mvcc >> 20` equals `PXAT - relative TTL` to the
   millisecond in both (`1791058469837` and `1791058475148`), i.e. the mvcc's ms is the very clock the master anchored the
   deadline on, which is the premise of decision 27. Consequence: `E_r == E`, and the band is the full `lag + skew` for
   **every** key, not `|lag_now - lag_at_set| + skew` for stream-set TTLs. (For a relative TTL skew would cancel, since both
   sides are then on the replica's clock; a flagged replica never sees one.) Spec D-9 and `PLAN.md` say it this way, and the
   tests stream the real forms (`SET .. PXAT`, `PEXPIREAT`, `INCRBY`). **Ledger row 24's parenthetical ("the full lag for
   absolute TTLs and for keys received in a full sync") therefore understates the window** (it is the full lag for all of
   them); I did not edit `decisions.md`, the owner's rows are binding. The owner may want to amend the parenthetical.
2. **The divergence condition has a mirror case.** "Diverges iff the master ran a command at `Tm < E` and the replica applied
   it at `Tm + lag + skew >= E`" is one direction. With `lag + skew < 0` (a replica clock behind the master's by more than
   the lag) the replica applies a command the master ran after `E` (and which saw no key) to a still-live key. D-9 states both.
3. **Test (b) as specified does not flip under decision 27.** `INCR c` then `EXPIRE c 60` after the deadline is the case where
   master and replica *agree* ("the value the master computed after its own lazy delete"): under envelope time the stamp is
   after the deadline, so the replica still deletes first and reads `1`. What flips is the mirror image, an `INCR` the master
   ran *before* the deadline (it holds 6; today's replica holds 1). The test therefore has three scenarios: `raw`,
   `envelope_applied_after_the_deadline` (the brief's (b), `1` before and after) and `envelope_applied_before_the_deadline`
   (`1` now, `6` under Task 2.8).
4. **Decision 27, as scoped, leaves the window open in the default configuration, because the sweep stays on the local
   clock.** The sweep deletes a due key at the replica's own `E`; a command the master ran before `E` that is still in
   flight then finds no key whatever clock it is applied at. The pytests have a `sweep` and an `access_only` (`--hz=0`)
   case each, and that split is what Task 2.8 will show: only `envelope-access_only` and
   `envelope_applied_before_the_deadline-access_only` can flip; the `-sweep` cases keep their value. How much the change
   closes thus depends on how slowly the sweep reaches a key (the prime-table walk is 1 ms per heartbeat). Spec D-9 ("What it
   leaves open") and Task 2.8 say this, and Task 2.8 records, as an open question for the owner and not as a task, the option
   that would close it (a stream clock for the sweep). This is the one finding I would put in front of the owner before
   Task 2.8 is built.
5. **Line anchors.** `db.cpp:2080` (`now > when`) is `db.cpp:2068`; the delete in `expireIfNeeded` is `:2110` (the early return
   is `:2101` as stated, the propagate gate `:1980`); the verbatim `EXPIRE` propagation is `expire.cpp:776-781`, not
   `782-788`; `incrementMvccTstamp` is `server.cpp:7270-7288`. The sweep facts hold (checked in `expire.cpp:311-314`,
   `server.h:344` and `:382`: hz 10, 25 ms budget, 20 keys per loop, repeat only above 10% stale; the fast cycle skips a
   replica, `server.cpp:2891-2892`). The rest of the analysis held when I read it (`db.cpp:181-186`, `:249-253`,
   `:2101`, `server.cpp:2054-2064`).

### Design notes

- **The tests mirror the wire.** Every TTL is `PXAT <abs>` with the deadline 3 s after the stream is written (the existing
  tests use `PX 3000` for the same reason: the SET must be applied before it is due), and the late commands are streamed
  once the clock is 1 s past it. Load only delays an apply, which moves it further past the deadline, so the outcome does
  not depend on the box. "Applied" is told by a marker `SET` streamed behind them (the stream applies in order), and
  the due key is never read before the late commands: a read deletes it.
- **`access_only` is the load-bearing case.** With the default heartbeat the sweep deletes a due key within the second of
  margin, so those cases pass for an option-C replica (sweep only) too; with `--hz=0` only an access can delete it, and the
  test asserts the key is still in the table first. The falsifications below show both.
- **The gtest table pins the doc's claims one by one** (it passed on its first run, so the doc's affected list is right for
  every class it names: refreshes, `INCR`/`APPEND`/`SETRANGE`/`HSET`/`LPUSH`/`SADD`, `RENAME`/`COPY`/`LMOVE`/`SMOVE`/
  `SUNIONSTORE`, `SET NX`; and the plain `SET` and `DEL` are indeed unaffected). `SETRANGE` needed a 4-byte value: at
  offset 0 over `5` the replies and values of "stale" and "absent" coincide.
- **`ReplicaActiveExpiryTest` was not changed**, only extended.

### Falsification

Both mutations are in `DbSlice::ExpireIfNeeded`'s gate (`db_slice.cc`, restored from a good copy after each, `cmp`-ed, `git diff
-- src/server/db_slice.cc` empty, `dragonfly` and `classic_replay_test` rebuilt from the restored source). Commands, as run by
`p71/falsify.sh`: patch, `nice ninja -C build-dbg -j3 dragonfly classic_replay_test`, `build-dbg/classic_replay_test
--gtest_filter='ReplicaActiveExpiryTest.*'`, `cat build-dbg/drakeydb > /dev/null` (see "A cold binary"), then
`KEYDB_SERVER_PATH=<scratchpad>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 DRAGONFLY_PATH=/home/user/drakeydb/build-dbg/dragonfly flock
/tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest tests/dragonfly/keydb_onboarding_test.py -k
after_the_deadline -v`.

- **M1: a flagged replica serves a due key as live, on every path** (the gate becomes
  `owner_->IsReplica() && (owner_->ReplicaActiveExpiry() || !absl::GetFlag(FLAGS_replica_delete_expired))`).
  - gtest: 4 failed, 4 passed. The two new tests fail as intended: `ARefreshThatArrivesAfterTheDeadlineFindsNoKey` has
    `CheckedInt({"persist", "k"}) Which is: 1` (expected 0) and `DbSize() Which is: 1` (expected 0), and
    `EveryCommandClassOfTheWindowSeesNoKey` fails 30 expectations with the values the master holds: `expire`, `pexpire`,
    `expireat`, `pexpireat` and `persist` reply 1, `getex` returns `'5'`, `incr` 6, `append` 2 and `get` `'5x'`, `setrange` 4 and
    `'x555'`, `hlen`, `llen`, `scard` 2, `rename` OK, `copy` 1, `lmove` `'a'`, `smove` and `sunionstore` 1, `set .. nx` nil, `del`
    1; only the plain `set` row passes. `ControlARefreshBeforeTheDeadlineKeepsTheKey` passes, as a control must. Two older tests
    also fail (`ReapsExpiredKeysButNeverEvicts`, `BypassesReplicaDeleteExpiredFlag`), as M1 disables the sweep and the read path.
  - pytest: **10 failed**, 4 with `AssertionError: k survived a refresh that arrived after its deadline` (`assert 1 == 0`) and 6
    with `AssertionError: c is '6': the replica computed it from a due key`.
- **M2: option C, access serves the key as live and the sweep still deletes** (M1's gate unchanged and `|| (owner_->IsReplica() &&
  owner_->ReplicaActiveExpiry() && events == nullptr)` added; the read path passes no `events`, the sweep does).
  - gtest: 3 failed, 5 passed: the two new tests and `BypassesReplicaDeleteExpiredFlag` fail (its read path); the sweep tests
    (`ReapsExpiredKeysButNeverEvicts`) pass.
  - pytest: **5 failed, 5 passed**. Every `access_only` case fails (2 with `k survived ...`, 3 with `c is '6'`); every `sweep` case
    passes, which is the point of having both: they pass for a replica that only sweeps.

Not falsified individually: the `raw` cases against the `envelope` ones (identical under option A by design: they are Task
2.8's controls).

### Results (debug build, rebuilt from the restored sources after the last mutation)

- `./classic_replay_test --gtest_filter='ReplicaActiveExpiry*'`: `[  PASSED  ] 8 tests.`; the whole binary: `[  PASSED  ] 74 tests.`
- `keydb_onboarding_test.py -k "expir or after_the_deadline"`, real KeyDB, `KEYDB_REQUIRED=1`: `32 passed` (22 existing and the
  10 new; the brief's `-k expir` alone does not select the new tests, whose names say `after_the_deadline`).
- The whole file: `92 passed in 231.96s`.
- **5 runs under load**: three busy loops pinned the box (`load average: 3.00`, three of four cores at about 98%),
  and `pytest ... -k after_the_deadline` ran five times: `10 passed` each time (`52.20`, `52.22`, `52.17`, `52.43`, `52.36` s); then
  `classic_replay_test --gtest_filter='ReplicaActiveExpiryTest.*' --gtest_repeat=5`: `8 passed` five times. The tests are
  sleep-bound, so the load changes their duration little; the margins hold because of what is said under "Design notes".
- `pre-commit run --files <the seven changed files>`: `pyflakes`, `trim trailing whitespace`, `fix end of files`, `check python
  ast`, `Clang formatting`, `black` all Passed.

### A cold binary

The first test of a pytest session after `dragonfly` was relinked failed twice with `DflyStartException: Process didn't start
listening on port in time` (`START_DELAY` is 0.8 s; the 455 MB binary was not in the page cache). It is the environment, not
the tests: after `cat build-dbg/drakeydb > /dev/null` the same case passes, and every run above was warmed that way.

### Not done / not verified

- Task 2.8 is a plan, not code: none of its seam exists, and the flips described in the test docstrings were derived, not
  run. Decision 27's effect on the `sweep` cases is argued from the code (the sweep calls `ExpireIfNeeded` with the local
  clock), and the premise is demonstrated by the `sweep` cases passing under M2.
- No release, ASAN or UBSAN build; the other gtest binaries and `keydb_harness_test.py`, `redis_replication_test.py`,
  `multimaster_test.py` were not rerun (no product source changed: `db_slice.cc` is byte-identical to HEAD).
- Upstream was read at one commit (`36f511b`, a shallow clone, no checkout).

### Open risks

1. Ledger row 24's parenthetical understates the window (item 1 above); the owner decides whether to amend it.
2. Decision 27 closes less than its row suggests in the default configuration (item 4). Task 2.8 is still worth building for
   the large-keyspace and low-`--hz` cases, but its description to operators must not promise convergence.
3. The 60 s window and the unshifted clock are my choices (Task 2.8 gives the reasons); both are one constant or one line to
   change if the owner prefers otherwise.
