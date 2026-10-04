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
