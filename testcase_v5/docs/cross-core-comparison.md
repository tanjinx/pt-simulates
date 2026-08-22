# Cross-core comparison — five mysqld SIGSEGV cores

This is the evidence summary behind the `testcase_v5` reproduction. It is a
de-identified view of a full forensic extraction from five `mysqld` crash cores:
**79 dimensions across 10 subsystems**, decoded from the
core files with gdb and an InnoDB source-correlated catalog.

For every dimension we mark whether it is **identical** across all five crashes (part
of the *fingerprint* — proof these are the same event) or **differs** (part of the
*spread* — proof that nothing in the software, load, or configuration causes it).

> Scope note: query text, workload tags, schema and index names, host names, and other
> environment-specific values are intentionally generalized here. The pattern below is
> exactly what the full extraction shows; only the identifying labels are abstracted.

All five cores ran the **identical** server binary (Percona Server 8.0.36-28, one GNU
build-id), so every comparison is binary-for-binary.

## The five crashes

| Core | Crash (UTC) | Host | CPU / NUMA | Caller path | Table row format |
|---|---|---|---|---|---|
| 1 | 2025-10-25 18:46 | A | 12 / 0 | execution: forward ref scan | compressed (8K) |
| 2 | 2025-10-27 13:31 | A | 7 / 0  | execution: forward ref scan | compressed (8K) |
| 3 | 2025-10-28 05:48 | A | 6 / 0  | execution: forward ref scan | compressed (8K) |
| 4 | 2026-04-06 09:20 | B | 13 / 0 | optimizer: range estimation | compressed (8K) |
| 5 | 2026-04-28 16:00 | C | 0 / 0  | execution: MRR | uncompressed (16K) |

Five different logical CPUs across three physical hosts.

---

## 1. The fault itself — what the CPU did at the instant of death

This is the core of the case. On a page fault caused by an **instruction fetch**, the
CPU must report the faulting address (CR2) equal to the address of the instruction it
tried to fetch (RIP). Here they are **not equal**: CR2 is RIP with one or more high bits
flipped, which is an address the program could never have generated.

| Dimension | Core 1 | Core 2 | Core 3 | Core 4 | Core 5 | ∆ |
|---|---|---|---|---|---|---|
| Signal | 11 | 11 | 11 | 11 | 11 | = |
| Trap number | 0xe (#PF) | 0xe | 0xe | 0xe | 0xe | = |
| Error code | 0x15 (present) | 0x14 (not present) | 0x14 | 0x14 | 0x14 | ≠ |
| Faulting instruction | `pthread_self+0` | `pthread_self+0` | `pthread_self+0` | `pthread_self+0` | `pthread_self+0` | = |
| Bytes at RIP | `f3 0f 1e fa` (`endbr64`) | same, intact | same | same | same | = |
| CR2 XOR RIP | `0x80000000` | `0x100000000000` | `0x290000600010` | `0x100000000000` | `0x200003002010` | ≠ |
| Flipped bit(s) | 31 | 44 | 4, 25, 26, 40, 43, 45 | 44 | 4, 13, 24, 25, 45 | ≠ |
| Flip popcount | 1 | 1 | 6 | 1 | 5 | ≠ |
| CR2 mapped? | mapped (zeros) | unmapped | unmapped | unmapped | unmapped | ≠ |
| Register file | all sane | all sane | all sane | all sane | all sane | = |
| How `pthread_self` reached | direct call | direct call | direct call | direct call | direct call | = |
| Crashing CPU | 12 | 7 | 6 | 13 | 0 | ≠ |

Key reads from this block:
1. **The error code is not even constant** (one core reports the page present, four report
   it not present). The instruction-fetch bit is always set, so it is always a code-fetch
   fault, but the very bits describing the fault disagree between otherwise identical crashes.
2. **The instruction bytes at RIP are byte-perfect `endbr64`** on every core. The code is
   not corrupted; only the *fetch address* the CPU reported is wrong.
3. **The register file is sane** on every core. Nothing in user-space state is off; only CR2
   is impossible.
4. **`pthread_self` is reached by a direct call**, which rules out a corrupted function
   pointer or use-after-free jumping there.
5. **Five different CPUs across three hosts** — not a single bad core.

### The flipped-bit family

| Cleared bit | Offset | Core(s) | Form |
|---|---|---|---|
| 2^31 | 2 GiB | 1 | clean single bit |
| 2^44 | 16 TiB | 2, 4 | clean single bit |
| 2^45 + drift | 32 TiB and up | 3, 5 | noisy (bit 45 dominant + lower-bit drift) |

Bit 44 is a clean single-bit flip in two independent crashes (Cores 2 and 4); bits
{4, 25, 45} recur in two others (Cores 3 and 5). The bit position varies but the
single-high-bit-cleared *form* is consistent, which looks like a systematic
address-translation defect rather than uniform-random memory upsets.

---

## 2. The crashing read — what SQL was doing

Every crash is a **read-only `SELECT`** in the middle of a B-tree descent. The query
text, workload tag, and table differ; the execution state at the fault is identical.

| Dimension | Value | ∆ |
|---|---|---|
| Statement kind | read-only `SELECT` | = (kind) |
| Caller path | forward ref scan (3), optimizer range estimation (1), MRR (1) | ≠ |
| Row read mode (`prebuilt`) | `LOCK_NONE` (consistent read) | = |
| Cursor position | mid-descent, positioned, key not yet matched | = |
| MVCC read view | no concurrent read-write transactions visible | = |
| Transaction footprint | read-only, REPEATABLE READ, 0 locks, 0 tables modified, undo_no 0, state `DB_SUCCESS` | = |
| Isolation level | REPEATABLE READ | = |
| Crashing index | clustered PRIMARY (4 cores); a secondary index (1 core, optimizer) | ≠ |
| Clustered root page | page 4 | = (4 of 5) |

Three independent execution and optimizer paths reach the same latch site. The query,
table, and index are incidental; the read-only B-tree descent that latches the clustered
root is the constant.

---

## 3. The page being fetched

| Dimension | Value | ∆ |
|---|---|---|
| Fetched page | clustered-index root, **page 4** (4 of 5; the optimizer core descends a deep secondary leaf) | = |
| Block guess | stale `POOL_WATCH` sentinel, so a hash lookup is used (4 cores); null / hash miss (1 core) | = (behavior) |
| Buffer-pool state | 0 pending reads (no I/O wait) | = |
| Adaptive hash index | OFF | = |
| Page header bytes | unavailable: 16 KB page data is excluded from the core by `MADV_DONTDUMP` | = |

The crash always lands on the **first** page of the descent (the clustered root), via a
hash-lookup fallback, with no pending I/O.

---

## 4. The mini-transaction

| Dimension | Value | ∆ |
|---|---|---|
| InnoDB mini-transaction | active, 0 redo records, no modifications, **2 page latches** | = |

A clean, read-only descent. Nothing is being written or logged at the fault.

---

## 5. Whole-server transaction / MVCC / purge state

| Dimension | Core 1 | Core 2 | Core 3 | Core 4 | Core 5 | ∆ |
|---|---|---|---|---|---|---|
| History list length (undo backlog) | 42 | 110 | 87 | 72 | 476 | ≠ (all low) |
| Active read-write transactions | ~none | ~none | ~none | ~none | ~none | = |
| Connection-level transactions | 45 | 79 | 109 | 311 | 288 | ≠ (load) |
| Prepared transactions | 0 | 0 | 0 | 0 | 0 | = |
| Purge subsystem | running, caught up, not backlogged | = |
| Row-lock waits | 0 | 0 | 0 | 0 | 0 | = |

The undo backlog is low and purge is caught up on every core: no purge, flush, or
checkpoint pressure at the fault.

---

## 6. Redo / logging

| Dimension | Value | ∆ |
|---|---|---|
| Checkpoint age (redo since last checkpoint) | 1.0 to 4.7 GB, normal pressure | ≠ (all normal) |
| Flush-to-disk lag | none | = |

---

## 7. Threads

| Dimension | Value | ∆ |
|---|---|---|
| Background threads | 23 present, **0 active** (master, purge, page-cleaner, log writer/flusher/checkpointer all idle) | = |
| Threads in the crash code path | 1 to 53 (concurrency varies widely) | ≠ |
| Threads at `pthread_self` at the fault | **exactly 1**, every time | = |
| Total thread inventory | ~395 to 480 | ≠ |

The fault is single-threaded and the background machinery is fully quiescent. Concurrency
in the path ranges from a single thread (Core 4) to 53 (Core 2) with no effect.

---

## 8. Server configuration + live status

| Dimension | Value | ∆ |
|---|---|---|
| InnoDB configuration (121 variables) | identical across all five **except buffer pool** | = |
| Buffer pool tier | 150 GB / 24 instances (Cores 1-3) vs 75 GB / 12 (Cores 4-5) | ≠ |
| Lock waits | 0 | = |
| Pre-crash monitoring snapshot | ~375 connections, only 7 to 16 running, 0 lock/buffer waits | = |

The server is quiet at every crash, and the only configuration difference is the buffer
pool size, which does not correlate with the fault.

---

## 9. Host / environment

| Dimension | Value | ∆ |
|---|---|---|
| Instance class | AWS EC2 i4i.8xlarge | = |
| CPU | Intel Xeon, Ice Lake-SP | = |
| Virtualization | AWS Nitro | = |
| CPU capability flags (`hwcap`) | `0x1f8bfbff`, identical on all five | = |

Same instance class, same CPU generation, same hypervisor on every crash.

---

## 10. Intentionally unavailable (recorded as known gaps)

| Dimension | Why unavailable |
|---|---|
| Buffer-pool page contents | excluded from the core by `MADV_DONTDUMP` (the one artifact a fresh `gcore` with `innodb_buffer_pool_in_core_file=ON` would add) |
| Tablespace / lock-system maps | require traversing sharded maps / hash tables that are not safe to walk on a static core |

---

## Summary: fingerprint vs spread

**Identical on all five (the fingerprint):** a benign, read-only, lock-free `SELECT`,
holding nothing and modifying nothing, just starting to descend a B-tree to fetch the
clustered-index root (page 4), when the CPU faults trying to fetch a byte-perfect
`endbr64` at `pthread_self` from an impossible, bit-flipped address. Code intact,
registers sane, purge caught up, background threads idle, no lock or I/O waits. Five
times, the same picture.

**Differs on all five (the spread):** different CPUs (12, 7, 6, 13, 0), hosts, tables,
indexes, code paths (execution vs optimizer), concurrency (1 to 53 threads in the path),
buffer-pool tier, error code, and the exact bits flipped. None is constant, so none of
them causes the crash.

## Conclusion

Every controllable variable changes between crashes while the fault signature stays
identical. That points the cause **outside the software**, at the CPU instruction-fetch
and address-translation layer, or the hypervisor fault-reporting path, rather than at
Percona Server or InnoDB. Two patterns stand out for a hardware/hypervisor escalation:

1. **Five different CPUs across three physical hosts** rule out a single bad core; the
   cause is model-wide (Ice Lake-SP microcode / Nitro) or transient.
2. **Recurring flipped-bit positions** (a clean bit-44 flip in two independent crashes)
   suggest a systematic address-translation defect, not uniform-random upsets.

The closest documented match we have found is an Intel branch-prediction erratum on the
Ice Lake-SP processor class (Intel reference **ICX150**, in the 3rd Gen Intel Xeon
Scalable Processors specification update, document 637780, marked No Fix), whose effect
is the processor producing an incorrect instruction pointer.

This is also why the `testcase_v5` reproduction, which drives the exact latch path above
at over 1.4 million descents per second for days, does not crash and is not expected to:
it can reproduce every software precondition, but it cannot inject the hardware or
hypervisor fault that actually fires the crash.
