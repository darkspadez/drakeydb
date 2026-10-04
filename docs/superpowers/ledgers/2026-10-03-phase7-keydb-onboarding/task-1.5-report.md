# Task 1.5 report: throughput, "must keep up with KeyDB"

Branch `feat/phase7-1-rreplay-unwrap`, started at HEAD `2bdf3d7`. Not committed (the orchestrator
commits). Implementer: Sonnet. Brief: `brief-task-1.5.md`; plan Tasks 1.5 and 1.6; spec D-12; ledger
`decisions.md` row 12. Scratch material (every run's JSON line, pytest log and load/cpu snapshot, the
wrapper scripts, the falsification sources and build scripts) is in the orchestrator's scratchpad under
`runs/`, `falsify/`, `perf_run.sh`, `smoke_run.sh`, `campaign.sh`, `aggregate.py`.

## Summary

- **The release bar passes, with a wide margin, and Task 1.6 is not opened.** Median of three release
  runs under `DRAKEYDB_PERF=1`: `apply/produce = 1.0003` (bar >= 0.95), maximum lag 1.35 MB (bar
  <= 61 MB, that is `max(2 s x produce rate, 8 MB)`), drain 0.036 s after the load stops (bar <= 2 s),
  no reconnect, no second sync, and both servers hold the same 100,000 `key:` and 100,000 `counter:`
  values. The comparator (a second active KeyDB as the replica) is at or above drakeydb on every lag
  measure: drakeydb is 0.80x its max lag, 0.94x its median lag and 0.73x / 0.47x / 0.41x its replication
  delay at p50 / p99 / max, so within the 1.5x bound.
- The load is bounded by KeyDB's one server thread (99% of cpu 0 in every run), about 213,000 writes/s
  (30.5 MB/s of stream, 143 bytes per command: the RREPLAY wrapper is 100 of them). drakeydb's replica
  took 55% of one cpu for that (2.6 us per command), KeyDB's own replica 96% (4.4 us). The bar is
  therefore "keeps up with what one KeyDB thread can produce"; a rough ceiling for drakeydb's envelope
  path is 1/2.6 us, about 385,000 commands/s, about 1.8x that.
- The test `test_keydb_onboarding_keeps_up_under_load` (`slow`, `keydb`) is a 15 s smoke by default (about
  5000 writes/s, ratio >= 0.5, lag < 32 MB, drain < 10 s) and the release bar under `DRAKEYDB_PERF=1`. A
  second, perf-only test, `test_keydb_throughput_reference_setups[keydb|drakeydb_raw]`, measures the
  comparator and the raw squashed reference with the same harness and no bounds.
- Falsified (below): a 1 ms sleep per enveloped command fails the smoke's ratio assertion (0.18 vs 0.5),
  fails the release run (KeyDB drops the lagging replica at its 256 MB output buffer, reported as a down
  link), and a calibrated 3 us busy wait per command fails the release ratio assertion itself (0.86 vs
  0.95, lag 108 MB).

## What changed

| File | Change |
|---|---|
| `tests/dragonfly/keydb_onboarding_test.py` | +491 lines (0 removed): 10 import lines and, at the end, the harness (`BenchmarkLoad`, `CappedLoad`, `ReplicationProbe`, `take_sample`, `summarise`, `bar_bounds`, `wait_drained`, `assert_idle_synced`, `assert_same_keys`, `sync_counts`, `record_throughput`, `run_throughput`, `pin_threads`, `driver_pinned_to`) and two tests. No existing line changed. |
| this report | new |

No product source changed. `git status` shows only the test file; `ninja -C build-opt -n dragonfly` says
"no work to do" (the release binary matches the tree: `git diff --stat 8eeba7c..HEAD` touches docs,
`classic_replay_test.cc`, `fake_classic_master.py` and `keydb_onboarding_test.py` only).

## Method

Box: 4 vCPU Intel Xeon 2.80 GHz VM, 16 GB, Linux 6.18.44. KeyDB v6.3.4 (`KEYDB_SERVER_PATH`),
`redis-benchmark` 7.0.15, release drakeydb `build-opt/dragonfly` (built 20:03Z from `2bdf3d7`),
debug drakeydb `build-dbg/dragonfly`.

- **Setup per run.** Active KeyDB master (`--active-replica yes --server-threads 1`, harness defaults
  otherwise, so `client-output-buffer-limit replica 256mb 64mb 60` stays on); the replica attached and
  **idle-synced** (link up, KeyDB lists it `online`, offsets equal, KeyDB `sync_full == 1`); then the
  load for 30 s; then a drain wait; then the key comparison.
- **Replicas.** `drakeydb`: a plain `REPLICAOF` replica (`--proactor_threads 2`, the harness's
  `num_shards = 1`) of the active KeyDB: the envelope path under test. `keydb`: a second active KeyDB
  `REPLICAOF` the master: the comparator. `drakeydb_raw`: the same drakeydb replica of a **non-active**
  KeyDB master: the raw stream the squasher batches (reference). Run **apart**, one at a time, so that no
  replica shares cpus with another. The method is in the `run_throughput` docstring.
- **Pinning** (perf mode only; `os.sched_setaffinity` on every thread of each process right after it is
  up): KeyDB master cpu 0; the replica under test cpus 1-2 (drakeydb keeps helio's `Proactor<i>` -> one
  cpu of the set, as under `taskset -c 1,2`); redis-benchmark under `taskset -c 3`, and the test's own
  event loop on cpu 3 too (its sampling is light). Per-cpu busy time from `/proc/stat` over each run:
  cpu0 81-90%, cpu1 43-52% (drakeydb, raw) and cpu2 2-5% (so the apply path runs on one proactor), cpu3
  14-16%.
- **drakeydb flags in perf mode:** `--latency_tracking=false --vmodule=` (production defaults), not the
  harness's per-command latency tracking and `vmodule` with VLOG(1) on. Smoke mode keeps the harness
  defaults.
- **Load:** two `redis-benchmark` processes side by side, `-P 100 -c 25 -r 100000 -n 2000000000 -q`,
  one `-t set`, one `-t incr` (50 connections in all, `key:%012d` and `counter:%012d`), started and
  killed by the test, so the window is exactly 30 s. Why not `-c 50 -t set,incr -n N`: redis-benchmark
  7.0 runs `set` and then `incr` one after the other, and has no duration; an `-n` sized for this box
  would make the window as long as another box is slow, and a window with only one command in it
  would not exercise both. KeyDB runs the commands it has read whole, so killing the generators leaves a
  clean stream. The smoke uses one asyncio connection instead (`CappedLoad`, 100-command pipelines
  every 20 ms): `redis-benchmark` has no rate cap and a debug build has to be kept below its limit.
- **Sampling:** at 1 Hz (31 samples), KeyDB `INFO replication` `master_repl_offset` and the replica's
  `INFO replication` `slave_repl_offset` (the plain-replica field name drakeydb prints, and KeyDB's),
  the two in parallel with `INFO commandstats` (offered writes: `set` + `incr` + `incrby`, because an
  active KeyDB rewrites `INCR` into the `INCRBY` it replicates and counts it as that), plus `psutil`
  CPU times of both servers and the loader. **Steady window** = the samples without the first and last
  two (26 s). Rates are offset deltas over the window; `ratio` = apply / produce; `lag` = master offset
  minus replica offset, per sample. "ops/s" of the apply side is derived at the master's bytes per
  command (143, 43 raw); drakeydb's own `rreplay_unwrapped` counter gives 226,256 vs 226,178 offered in
  run 1, so the derivation holds.
- **Replication delay probe** (perf mode; `ReplicationProbe`, not part of the brief): every 100 ms a
  marker key is written into KeyDB and the replica is polled every millisecond until it shows it. Two
  `INFO`s a few ms apart resolve a lag to a few hundred KB at 30 MB/s (the lag can read below 0), so
  the byte lags of two replicas cannot be compared at 1.5x resolution; the delay is in ms and says what
  a client waits. About 200 markers per run, 10 writes and a few hundred reads a second.
- **Drain:** poll both offsets every 50 ms from the moment the load generators are dead; the drain time
  is the time of the first poll with equal offsets that the next poll finds unchanged on the master
  (KeyDB may still be working through what the generators sent).
- **Final checks:** `DBSIZE` equal and all 100,000 `key:` and 100,000 `counter:` names `MGET`ed on both
  servers and compared (a counter's value is the number of `INCR`s it took; this is the "sum" check, in
  full).
- **Discipline:** every release run through `perf_run.sh`: wait until `/proc/loadavg` 1-minute load is
  < 0.5, take `flock /tmp/drakey-pytest.lock`, re-check, run pytest with the lock fd closed in the
  children. Every run's load average at lock, the test's own reading after setup and per-cpu `/proc/stat`
  before and after are in `runs/<label>.meta`; the top-five `ps` before and after show nothing but this
  session's own processes. Runs were interleaved (drakeydb, keydb, raw) x3, 20:39-20:53Z.

## Numbers: three release runs each (`DRAKEYDB_PERF=1`, `DRAGONFLY_PATH=.../build-opt/dragonfly`)

Columns: offered = KeyDB `set`+`incr`+`incrby` calls/s; produce / apply = offset deltas/s over the steady
window; lags in KB (1 KB = 1000 B) from the 1 Hz samples; delay = marker delay p50 / p99 / max in ms
(n markers); drain after the load stops; CPU % of one cpu (KeyDB master / replica / loader). "load" = the
1/5/15-minute load average when the lock was taken.

### drakeydb, envelope path (`test_keydb_onboarding_keeps_up_under_load[df_factory0]`)

| run | load | offered ops/s | produce MB/s | apply MB/s | apply ops/s | ratio | max lag KB | median lag KB | delay ms (n) | drain s | cpu % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.49 0.56 0.79 | 226,178 | 32.3 | 32.4 | 226,256 | 1.0003 | 1284 | 887 | 7.2/20.8/23.1 (200) | 0.036 | 99.3 / 54.5 / 6.8 |
| 2 | 0.49 0.69 0.79 | 213,025 | 30.5 | 30.5 | 213,008 | 0.9999 | 1565 | 961 | 8.0/19.9/20.4 (196) | 0.071 | 99.2 / 55.4 / 6.7 |
| 3 | 0.48 0.73 0.80 | 212,061 | 30.3 | 30.3 | 212,123 | 1.0003 | 1351 | 933 | 8.2/23.2/23.3 (197) | 0.028 | 99.3 / 56.8 / 6.8 |
| **median** | | **213,025** | **30.5** | **30.5** | **213,008** | **1.0003** | **1351** | **933** | **8.0/20.8/23.1** | **0.036** | **99.3 / 55.4 / 6.8** |

Bar: ratio >= 0.95; max lag <= `max(2 x produce rate, 8 MB)` = 64.7 / 60.9 / 60.6 MB; drain <= 2 s. All
pass in every run. Steady lag series of run 1 (KB, 31 samples, first is the baseline): `0 715 1284 786 893
1280 871 846 744 772 1169 1202 965 793 856 1161 772 978 1053 772 990 848 932 1175 886 853 1187 887 802
947 856`. The lag does not grow: it is the 5,000 commands (715 KB) a KeyDB event-loop iteration
(50 connections x 100 pipelined) writes to its replicas at once.

### Comparator: a second active KeyDB as the replica (`test_keydb_throughput_reference_setups[df_factory0-keydb]`)

| run | load | offered ops/s | produce MB/s | apply MB/s | apply ops/s | ratio | max lag KB | median lag KB | delay ms (n) | drain s | cpu % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.47 0.62 0.79 | 212,534 | 30.4 | 30.4 | 212,515 | 0.9999 | 1682 | 1086 | 11.5/46.5/56.2 (192) | 0.130 | 99.1 / 96.6 / 7.0 |
| 2 | 0.48 0.71 0.79 | 218,526 | 31.2 | 31.4 | 219,735 | 1.0055 | 5585 | 991 | 11.0/30.2/37.8 (192) | 0.023 | 99.0 / 95.3 / 6.8 |
| 3 | 0.47 0.73 0.79 | 221,045 | 31.6 | 31.6 | 221,005 | 0.9998 | 1511 | 930 | 10.7/44.7/64.8 (195) | 0.066 | 99.2 / 96.5 / 6.9 |
| **median** | | **218,526** | **31.2** | **31.4** | **219,735** | **0.9999** | **1682** | **991** | **11.0/44.7/56.2** | **0.066** | **99.1 / 96.5 / 6.9** |

The KeyDB replica sits at 96% of its cpu: it has about no headroom left at this rate, and it has
multi-MB stalls (run 2's 5.6 MB max lag) that drakeydb's runs do not.

### drakeydb / comparator, from the medians of three

| measure | drakeydb | KeyDB replica | ratio | within 1.5x |
|---|---|---|---|---|
| max lag (bytes, steady window) | 1,351,120 | 1,682,373 | 0.80 | yes |
| median lag (bytes) | 933,412 | 990,685 | 0.94 | yes |
| replication delay p50 (ms) | 8.0 | 11.0 | 0.73 | yes |
| replication delay p99 (ms) | 20.8 | 44.7 | 0.47 | yes |
| replication delay max (ms) | 23.1 | 56.2 | 0.41 | yes |
| drain after the load stops (s) | 0.036 | 0.066 | 0.55 | yes |

The byte lags are at the resolution limit (see the method: +-a few hundred KB of INFO skew) and say
"the same"; the delay probe is the finer measure, and drakeydb is the faster one.

### Reference: drakeydb replica of a plain KeyDB, raw stream squashed (`...[df_factory0-drakeydb_raw]`)

| run | load | offered ops/s | produce MB/s | apply MB/s | apply ops/s | ratio | max lag KB | median lag KB | delay ms (n) | drain s | cpu % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.46 0.66 0.79 | 276,231 | 11.9 | 11.9 | 276,196 | 0.9999 | 386 | 293 | 5.0/14.8/20.4 (208) | 0.065 | 99.1 / 46.1 / 7.8 |
| 2 | 0.49 0.73 0.80 | 273,086 | 11.7 | 11.7 | 272,945 | 0.9995 | 430 | 292 | 5.0/12.9/16.1 (208) | 0.118 | 98.9 / 46.5 / 8.1 |
| 3 | 0.48 0.77 0.81 | 268,630 | 11.6 | 11.5 | 268,619 | 1.0000 | 430 | 289 | 5.0/16.2/22.1 (208) | 0.025 | 99.2 / 47.6 / 8.3 |
| **median** | | **273,086** | **11.7** | **11.7** | **272,945** | **0.9999** | **430** | **292** | **5.0/14.8/20.4** | **0.065** | **99.1 / 46.5 / 8.1** |

43 bytes per command (no wrapper) and a master that is 28% faster (it does not wrap): the squashed raw
path costs the replica 1.7 us per command (47% of a cpu at 273k/s), the envelope path 2.6 us, that is
1.5x per command, and KeyDB's replica 4.4 us. The envelope path therefore pays about 0.9 us per command
over the raw path for per-command dispatch, and the bar leaves room for it.

Offered rates differ between the three setups (213k, 219k, 273k) for the master's own reasons, not the
replica's: an active master wraps every write in an RREPLAY envelope.

### Headroom (not a bar)

With the 3 us busy wait of the falsification below the replica saturated (99% of a cpu) at 174,885
commands/s, so 5.67 us per command, 2.67 us without the wait, which matches the 2.6 us measured above:
the apply path of a single link is one thread, and its ceiling here is about 375-385k envelopes/s,
about 1.8x what one KeyDB server thread produces. KeyDB with `--server-threads` > 1 was not measured (the
brief fixes it at 1).

An earlier campaign of the same nine runs (20:21-20:35Z) used a first version of the harness (a lag
corrected for INFO skew, no delay probe) and gave the same picture: drakeydb ratio 0.9998-0.9999, drain
<= 0.077 s, lag <= 1 MB; comparator and reference likewise. Its numbers are superseded and not used.

## Verdict on the release bar

| bar (spec D-12, decision 12) | result | |
|---|---|---|
| `apply/produce >= 0.95` over the steady window | 1.0003 (runs: 1.0003, 0.9999, 1.0003) | pass |
| max lag `<= max(2 s x produce, 8 MB)` | 1.35 MB median, 1.56 MB worst, of 61 MB | pass |
| lag drains within 2 s of the load stopping | 0.036 s median, 0.071 s worst | pass |
| no reconnect; KeyDB one full sync | `dragonfly_replica_reconnect_count` unchanged, KeyDB `sync_full == 1` and no sync counter moved, `connected_slaves == 1`, link `up` | pass |
| final key counts equal (and the INCR counters) | `DBSIZE` equal; all 200,000 `key:`/`counter:` values equal | pass |
| drakeydb lag within 1.5x of a KeyDB active replica's | 0.80x max, 0.94x median (offsets); 0.47x p99 delay | pass |

**Task 1.6 is not triggered.** Task 1.6 Step 1 (the profile) was not run; the per-command cost figures
above come from CPU time, not from a profile.

## The smoke

`DRAKEYDB_PERF` unset, harness-default flags, 5000 writes/s, 10 s window (steady 6 s):

- debug (`build-dbg/dragonfly`): 3 of 3 passed, 15.3 / 15.6 / 14.5 s per run (the final file);
- release (`build-opt/dragonfly`): 3 of 3 passed, 13.8 / 14.6 / 13.7 s per run (the final file).

Numbers of the final six: ratio 1.0000-1.0001, max and median lag exactly 14,750 B (the samples are
phase-locked to the loader's 20 ms batches, so each sees one 100-command batch in flight), drain
0.007-0.012 s, debug replica at 32-34% of a cpu, release 3.2-3.3%. The same six runs also passed on two
earlier revisions of the file, and a seventh, the very first, passed before the `incrby` fix.

Every perf-mode test above also passed once more on the final file (`final2-*`): drakeydb ratio
1.0001, max lag 1.37 MB, drain 0.07 s; KeyDB replica 1.0069 / 6.4 MB / 0.041 s; raw 1.0002 / 0.42 MB /
0.089 s.

## Falsification (step 4)

**Method.** The brief's edit, a 1 ms `util::ThisFiber::SleepFor` at the top of `ClassicApplier::ApplyCommand`,
but built **outside the tree**, so that neither the working tree, `build-opt` nor `build-dbg` ever held
it (the reviewer reads the tree and runs `build-dbg`): a patched copy of `classic_replay.cc` in the
scratchpad, compiled with the exact command `ninja -t commands dragonfly` prints for it (ccache off,
`-O3 -DNDEBUG`), and linked with that object listed explicitly ahead of `libdragonfly_lib.a` into a
scratch binary (`falsify/compile2.sh`, `link2.sh`; `compile3.sh`, `link3.sh` for the busy wait). Both
scratch binaries (378 MB each) are deleted. After it: `git status` = only the test file, `git diff --stat
-- src helio` empty, `ninja -C build-opt -n dragonfly` = "no work to do". The diff of the patched copy
against `src/server/classic_replay.cc` is the include and one line:

```
25a26 > #include "util/fibers/fibers.h"
348a350 > util::ThisFiber::SleepFor(std::chrono::milliseconds(1));  // FALSIFICATION ONLY
```

Consequence: the debug binary was not falsified (rebuilding `build-dbg/dragonfly` under the reviewer
was the alternative); the smoke's falsification ran on a release binary, whose only difference to the
debug one is speed.

**1. Smoke (default mode) on the 1 ms binary:** FAILS, as required.

```
tests/dragonfly/keydb_onboarding_test.py:2763: AssertionError
E  AssertionError: {'offered_ops_per_s': 5000, 'produce_bytes_per_s': 737526, 'apply_bytes_per_s': 133296, 'apply_ops_per_s': 904, ...}
E  assert 0.1807 >= 0.5
```

(`DRAGONFLY_PATH=<scratch>/drakeydb-falsified`, 1 failed in 25.3 s; lag growing 600 KB a second.)

**2. Release bar (`DRAKEYDB_PERF=1`) on the 1 ms binary:** FAILS. The first attempt ended with a
`KeyError: 'slave_repl_offset'` in the sampler: KeyDB closed the replica when its output buffer reached
the 256 MB limit about 10 s into the window (`omem=268435449 ... scheduled to be closed ASAP for
overcoming of output buffer limits` in KeyDB's log), the replica reconnected and asked for a resync. The
sampler now says what happened, and the rerun failed there (9,681 envelopes had been applied):

```
E  AssertionError: the replica's link is down: {... 'master_link_status': 'down', ..., 'rreplay_unwrapped': 9681, ...}
```

This is the bar's "no reconnect" assertion in a sharper form; a replica as far behind as a 1 ms sleep
makes it never reaches the ratio assertion in a 30 s window.

**3. Release bar on a calibrated binary** (3 us busy wait per command, `falsify/classic_replay_busy.cc`),
slow enough to miss the ratio and fast enough not to be dropped: FAILS on the ratio assertion itself.

```
E  AssertionError: {'offered_ops_per_s': 202392, 'produce_bytes_per_s': 28936369, 'apply_bytes_per_s': 25003561, 'apply_ops_per_s': 174885, ...}
E  assert 0.8641 >= 0.95
```

(max lag 107,853,504 B against a 57.9 MB bound, median lag 56 MB, replica at 99.2% of a cpu, marker
delay p50 1.5 s.) Because the ratio is asserted before the lag, that is the line that fired.

**Restored:** nothing to restore in the tree (see the method). The unmodified release binary ran the
campaign above before the falsification and the confirmations after it (`final-*` and `final2-*`); the
1 ms and busy-wait binaries were deleted before them.

## Design notes and deviations from the brief

- **Two `redis-benchmark` processes instead of one `-t set,incr`**, killed at the window's end
  (reasons in the method). `-c 50` is kept as 25 + 25.
- **`assert_keydb_saw_one_full_sync` is not used.** A KeyDB replica of a KeyDB master first sends
  `PSYNC <its own replid> <offset>` (it was a master a moment ago), which the master counts as
  `sync_partial_err = 1`, so that helper (zero partial attempts) fails the comparator before the load
  starts. The test records KeyDB's three sync counters once the replica is idle-synced, requires
  `sync_full == 1`, and requires none of them to move during the load. For drakeydb the counters are
  (1, 0, 0), the helper's own condition. Found in the comparator's first shakeout.
- **The comparator and the raw reference are one perf-only parametrized test** with no bounds, not a
  mode of the bar test, so that the three setups share one harness and the default run is not made
  longer; it is skipped unless `DRAKEYDB_PERF=1`. The test ids carry the class-scoped `df_factory`
  parameter: `...[df_factory0-keydb]`, `...[df_factory0-drakeydb_raw]`.
- **The release run uses production drakeydb flags** (no `latency_tracking`, empty `vmodule`);
  the smoke uses the harness defaults.
- **The drain is timed before the bounds are asserted** (at most 5 s after the bound), so that a run
  that fails them still records all its numbers in `$DRAKEYDB_PERF_OUT`; the assertions are in the
  brief's order of importance, link first.
- **`$DRAKEYDB_PERF_OUT`**: when set, `record_throughput` appends the run's JSON line (all numbers,
  the lag series and the raw samples) to it; the report's tables are made from those.
- **Lag estimator.** A first version corrected the master's offset for the INFO read skew by
  extrapolating at the previous second's rate. It was dropped: the plain difference is the brief's
  definition, and the bounds are 40-1000x larger than the skew; the comparison between replicas moved
  to the delay probe.

## Not done, not verified, open risks

- **Not run:** Task 1.6 Step 1 (not triggered); the debug binary's falsification (see above); any run
  with KeyDB `--server-threads` above 1, with `num_shards` 2 (the harness gives 1 for two proactors),
  with larger values or other command types (3-byte `SET` values and `INCR` only), or with the
  keyspace larger than 100,000. The bar is about this load.
- **The headroom (1.8x) is an estimate** from CPU time and one saturated variant; the apply path of a
  link runs on one proactor (cpu2 stayed at 2-5%), so a faster master than one KeyDB thread could move
  the answer. Peer mode (Task 2.4's re-run) adds stamping, dedup and the LWW guard on top.
- **The box is a VM** and its neighbours are not visible; every run began at 1-minute load 0.46-0.49
  (the loop only started a run once the previous run's load had decayed under 0.5, so it was never
  lower), and the test's own reading after setup was 0.42-0.52. Nothing of the reviewer's was seen in
  the `ps` snapshots or the per-cpu times, but they are snapshots at the two ends of a run.
- **The offset lags have a resolution of a few hundred KB** (the median lag of 0.93 MB is mostly the
  master's 715 KB batch, sampled at an arbitrary phase). They are right for the bar's bounds and not
  for comparisons finer than that; the delay probe is, and is perf-only.
- **Smoke timing on a heavily loaded CI box** was not tried beyond this VM (6 of 6 passes on the final
  file, each 13-16 s; the debug replica used 33% of a cpu for 5000 writes/s, so there is a factor of
  3 of room before ratio 0.5 is at risk).
- **`DRAKEYDB_PERF=1` with a debug binary** would fail the bar and mean nothing; the test does not
  check the build type.
- The plan's Task 1.5 Step 5 commit (`test: pin that the RREPLAY path keeps up with KeyDB under load (P7)`)
  is the orchestrator's. Nothing in the plan, the spec or `docs/PLAN.md` was edited; D-12 and decision 12
  need no change (the bar passed as written). The plan's Task 1.5 text could gain an "as built" line for
  the two-process load, the sync-counter check and the reference test.

## Commands

```
# smoke, debug or release (default mode)
cat <bin> > /dev/null
KEYDB_SERVER_PATH=<scratch>/KeyDB/src/keydb-server KEYDB_REQUIRED=1 DRAGONFLY_PATH=<bin> \
  flock /tmp/drakey-pytest.lock /root/drakey-venv-pinned/bin/python -m pytest \
  tests/dragonfly/keydb_onboarding_test.py::test_keydb_onboarding_keeps_up_under_load -p no:cacheprovider

# the release bar and the two reference setups (perf_run.sh adds the load wait and the lock)
DRAKEYDB_PERF=1 DRAKEYDB_PERF_OUT=<file>.jsonl KEYDB_SERVER_PATH=... KEYDB_REQUIRED=1 \
  DRAGONFLY_PATH=/home/user/drakeydb/build-opt/dragonfly \
  /root/drakey-venv-pinned/bin/python -m pytest -p no:cacheprovider \
  "tests/dragonfly/keydb_onboarding_test.py::test_keydb_onboarding_keeps_up_under_load[df_factory0]"
  # and ...::test_keydb_throughput_reference_setups[df_factory0-keydb] / [df_factory0-drakeydb_raw]

pre-commit run --files tests/dragonfly/keydb_onboarding_test.py   # pyflakes, whitespace, ast, black: passed
```
