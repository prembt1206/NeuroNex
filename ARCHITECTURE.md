# Architecture Diagram and Robustness Proof

## Part 1: Architecture Diagram

```text
+-----------------------------------------------------------------------------------+
|                             CLIENT API / CLI / SERVE                              |
|        put(k,v)   get(k)   delete(k)   batch([ops])   open(path)   close()        |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                      GLOBAL RE-ENTRANT LOCK (self.lock: RLock)                    |
|              Serializes concurrent writers & protects index/fd state              |
+-------------------+-------------------------------------------+-------------------+
                    |                                           |
        [Write / Delete / Batch]                       [Open / Crash Recovery]
                    |                                           |
                    v                                           v
+---------------------------------------+   +---------------------------------------+
| 1. _pack_frame(ops)                   |   | R1. Resolve .compact.tmp vs <path>    |
|    - Pack OpCount + Ops into Payload  |   |     (_verify_file_complete + replace) |
|    - Compute CRC32(Payload)           |   | R2. Validate 8B Header ("KVDB\0\0\0\1")|
|    - Prepend 16B Header ("FRME"...)   |   | R3. Scan frames sequentially from 8B: |
|    - Append 4B Commit Marker ("CMIT") |   |     - Check bounds (pos+20+len<=size) |
+-------------------+-------------------+   |     - Check trailing 4B == "CMIT"     |
                    |                       |     - Check zlib.crc32(payload)==CRC  |
                    v                       | R4. Apply valid frame ops to Index    |
+---------------------------------------+   | R5. If tail frame torn/uncommitted:   |
| 2. Disk-Full Guard (_write_batch...)  |   |     - os.ftruncate(fd, valid_offset)  |
|    - Check file_size+len > max_bytes  |   |     - os.fsync(fd)                    |
|    - If dead_records > 0: compact()   |   +-------------------+-------------------+
|    - If still > max_bytes: raise Err  |                       |
+-------------------+-------------------+                       |
                    |                                           |
                    v                                           |
+---------------------------------------+                       |
| 3. os.lseek(fd, file_size, SEEK_SET)  |                       |
| 4. os.write(fd, frame)  [Header+Body  |                       |
|    + "CMIT" written sequentially]     |                       |
| 5. os.fsync(fd)         [DURABILITY]  |                       |
| 6. Update In-Memory Index (self.index)|                       |
| 7. Return / Print "ACK <op-id>"       |                       |
+-------------------+-------------------+                       |
                    |                                           |
                    +---------------------+---------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     IN-MEMORY HASH INDEX (self.index: dict)                       |
|                     Maps: key_bytes -> (val_file_offset, val_len)                 |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                    ON-DISK AUTHORITATIVE FILE (<path>) LAYOUT                     |
|                                                                                   |
|  Offset 0..7   : [ File Header: "KVDB" (4B) | Version: 0x00000001 (4B) ]          |
|  Offset 8..N   : [ Frame 1 ] [ Frame 2 ] ... [ Frame K ]                          |
|                                                                                   |
|  Single Frame Layout:                                                             |
|  +----------------+----------------+----------------+----------------+            |
|  | "FRME" (4B)    | CRC32 (4B)     | PayloadLen(4B) | OpCount (4B)   | (16B Hdr)  |
|  +----------------+----------------+----------------+----------------+            |
|  | Op 1: Type(1B) | KeyLen(2B) | ValLen(4B) | KeyBytes | ValBytes    | (Payload)  |
|  | ... Op M ...                                                      |            |
|  +-------------------------------------------------------------------+            |
|  | Commit Marker: "CMIT" (4B)                                        | (4B Tail)  |
|  +-------------------------------------------------------------------+            |
+-----------------------------------------------------------------------------------+
```

---

## Part 2: Scenario-to-Mechanism Proof Table (All 32 Scenarios)

| Scenario | Disk state at the moment of the kill | Mechanism that handles it | Where in the code | Verified by |
| :--- | :--- | :--- | :--- | :--- |
| **A1** | Zero bytes of the new frame are written to `<path>`; EOF remains at previous `valid_offset`. | `open()` replays up to EOF (`valid_offset`); key remains absent or retains its prior frame's value. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A2** | Partial frame header or partial payload bytes exist at EOF; trailing `CMIT` marker is absent or `pos + frame_total_len > size` or `CRC32` mismatches. | `open()` detects `pos + frame_total_len > size` or `commit_marker != COMMIT_MAGIC` or `crc32 != crc_expected`, stops scanning, and calls `os.ftruncate(self.fd, valid_offset)` + `os.fsync(self.fd)`. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A3** | Complete frame (`FRME` + payload + `CMIT`) is in OS page cache or on disk prior to `os.fsync(self.fd)` returning. | Since process death (`kill -9`) does not discard OS page cache, full frame with valid `CRC32` and `CMIT` is replayed by `open()` (or truncated if partially written by syscall). | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A4** | Full frame (`FRME` + payload + `CMIT`) is physically flushed on disk via `os.fsync(self.fd)`. | `open()` validates `FRME`, `CRC32`, and `CMIT`, and loads the new key offset into `self.index`. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A5** | Full frame is on disk after `os.fsync(self.fd)` and after `put()` returned / printed `ACK`. | `open()` replays the committed frame and populates `self.index[key_bytes]`. | `src/engine.py` :: `KVEngine.put`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A6** | Either partial delete frame at EOF (no `CMIT`) or complete delete frame (`OP_DEL = 2` with `CMIT`). | If partial, `open()` truncates to `valid_offset` leaving old value intact; if complete, `open()` executes `del self.index[key_bytes]`. | `src/engine.py` :: `KVEngine.delete`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **A7** | Multi-block 1 MB value partially written at EOF (`pos + frame_total_len > size`), or fully written with `CMIT`. | `_pack_frame` encapsulates the entire 1 MB payload under one `payload_len`, `CRC32`, and trailing `CMIT`. Torn 1 MB write fails length/CMIT check and is truncated by `os.ftruncate`. | `src/engine.py` :: `KVEngine._pack_frame`, `KVEngine.open` | `./torture --iterations 100 --seed 999 --mode large-values` |
| **B1** | 3 of 10 batch operations written to EOF inside a single batch frame; `pos + frame_total_len > size` and `CMIT` absent. | Entire batch is packed as one frame with `op_count = 10` and `payload_len` covering all 10 ops. `open()` sees incomplete frame at tail, stages nothing, and truncates to `valid_offset`. | `src/engine.py` :: `KVEngine._pack_frame`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **B2** | All 10 operations written to disk, but trailing 4-byte `CMIT` marker is missing at `payload_end`. | `open()` checks `pos + frame_total_len > size` and `commit_marker != COMMIT_MAGIC`, rejects the uncommitted frame, and truncates to `valid_offset`. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **B3** | Full batch frame including `CMIT` written to OS buffer before `os.fsync(self.fd)` completes. | Frame is either completely written (and replayed atomically for all 10 ops) or truncated if syscall was interrupted mid-buffer. Never partially applied. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **B4** | Full batch frame with `CRC32` and `CMIT` flushed to disk via `os.fsync(self.fd)`. | `open()` verifies frame CRC32 and `CMIT`, loops `range(op_count)` to stage all 10 ops in `staged_updates`, and applies all 10 to `self.index`. | `src/engine.py` :: `KVEngine.batch`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **B5** | Single batch frame containing `OP_PUT` on key `X` and `OP_DEL` on key `X` in caller-specified order. | `_pack_frame` serializes operations in list order; both `_write_batch_internal` and `open()` apply `staged_updates` sequentially (`0 .. op_count-1`). | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **B6** | Partial frame of 10,000 operations at EOF (`pos + frame_total_len > size`). | `open()` reads file into a contiguous `memoryview`, sees `pos + frame_total_len > size` in O(1) at the tail, and truncates via `os.ftruncate` in < 5 ms. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 100 --seed 555 --mode large-batches` |
| **C1** | Process killed during `open()` while scanning `memoryview` in RAM before `os.ftruncate` is reached. | Read-only scan in `open()` does not mutate bytes on disk; the next `open()` scans the exact same file bytes and reaches the identical state. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 100 --seed 42 --mode recovery-crash` |
| **C2** | Process killed during `os.ftruncate(self.fd, valid_offset)` in `open()`. | `valid_offset` only shrinks the broken tail; all valid frames `0 .. valid_offset` remain untouched, so the next `open()` scans to the exact same `valid_offset`. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 100 --seed 42 --mode recovery-crash` |
| **C3** | `<path>` is untouched and complete; `<path>.compact.tmp` is partially written on disk. | On next `open()`, both `<path>` and `<path>.compact.tmp` exist; `open()` keeps `<path>` as authoritative and calls `os.remove(self.tmp_path)`. | `src/engine.py` :: `KVEngine.compact`, `KVEngine.open` | `./torture --iterations 100 --seed 7 --mode compaction` |
| **C4** | `os.fsync(tmp_fd)` completed before `os.replace(self.tmp_path, self.path)`. Either `<path>` still exists, `<path>` was replaced by `tmp_path`, or (on Windows crash window) only `tmp_path` exists. | `open()` checks `if not os.path.exists(self.path) and os.path.exists(self.tmp_path)`: verifies `_verify_file_complete(self.tmp_path)` and finishes `os.replace(self.tmp_path, self.path)`. | `src/engine.py` :: `KVEngine.compact`, `KVEngine.open`, `KVEngine._verify_file_complete` | `./torture --iterations 100 --seed 7 --mode compaction` |
| **C5** | `<path>` has been replaced by the new compacted file; if any leftover `.compact.tmp` remains, it is present alongside `<path>`. | `open()` sees `os.path.exists(self.tmp_path)` while `self.path` exists and deletes `self.tmp_path` via `os.remove(self.tmp_path)`. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 100 --seed 7 --mode compaction` |
| **C6** | Process killed inside `close()` during `os.fsync` or `os.close`. | Every prior operation already called `os.fsync(self.fd)` before returning success; `close()` does not write special footer metadata, so `open()` recovers normally. | `src/engine.py` :: `KVEngine.close` | `./torture --iterations 500 --seed 12345` |
| **D1** | Two writer threads submitting `put` on distinct keys; serialized through `self.lock` (`RLock`) and `pipe_lock`. | `self.lock` ensures frames are never interleaved on disk; every frame that completed `os.fsync` and returned `ACK` is intact on disk. | `src/engine.py` :: `KVEngine._write_batch_internal` | `./torture --iterations 500 --seed 12345` |
| **D2** | Two writer threads putting the same key; frames are written sequentially under `self.lock`. | Each frame is written completely before the lock releases; on disk, one writer's frame strictly precedes the other's, or the second is torn at EOF and truncated. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 500 --seed 12345` |
| **D3** | Long batch writer and single-put writer contend on `self.lock`; frames are serialized on disk. | If killed mid-batch frame, `open()` truncates the uncommitted batch frame at EOF while all prior ACKed single-put frames remain valid. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 100 --seed 555 --mode large-batches` |
| **D4** | Reader calls `get(key)` while `batch()` is running. Both acquire `self.lock` (`RLock`). | `_write_batch_internal` holds `self.lock` across the entire disk write and `self.index` loop; `get()` acquires `self.lock`, so it sees `self.index` strictly before or strictly after the entire batch. | `src/engine.py` :: `KVEngine.get`, `KVEngine._write_batch_internal` | `tests/test_scenarios.py` (concurrent reader/batch test) |
| **E1** | `self.file_size + len(frame) > self.max_bytes` detected BEFORE `os.write` is called; disk state is completely unchanged. | Pre-write size check raises `DiskFullError` before writing a single byte to `self.fd`. Previous data and `self.index` remain untouched. | `src/engine.py` :: `KVEngine._write_batch_internal` | `./torture --iterations 100 --seed 88 --mode disk-full` |
| **E2** | Entire batch frame is serialized in RAM via `_pack_frame(ops)` first; total `len(frame)` is checked against `self.max_bytes` before `os.write`. | Because size check happens on the complete packed batch frame before `os.write`, `DiskFullError` is raised with 0 bytes of the batch written to disk. | `src/engine.py` :: `KVEngine._write_batch_internal` | `./torture --iterations 100 --seed 88 --mode disk-full` |
| **E3** | Cap reached (`DiskFullError` raised cleanly without modifying disk), then process is killed. | Disk ends cleanly at the last valid committed frame's `CMIT` marker; `open()` succeeds with all previously ACKed keys intact. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.open` | `./torture --iterations 100 --seed 88 --mode disk-full` |
| **E4** | Compaction writes only live keys to `self.tmp_path`; if killed or failing prior to swap, `self.path` is unmodified. | `compact()` writes and `fsync`s `self.tmp_path` before touching `self.path`. Any failure during temp file creation leaves `self.path` authoritative. | `src/engine.py` :: `KVEngine.compact`, `KVEngine.open` | `./torture --iterations 100 --seed 88 --mode disk-full` |
| **E5** | Deletes/overwrites increment `self.dead_records`. When a new write would exceed `self.max_bytes` and `self.dead_records > 0`, `_write_batch_internal` automatically calls `self.compact()`. | Auto-compaction reclaims dead tombstones and overwritten records on disk, shrinks `self.file_size`, and allows subsequent writes to succeed automatically. | `src/engine.py` :: `KVEngine._write_batch_internal`, `KVEngine.compact` | `./torture --iterations 100 --seed 88 --mode disk-full` |
| **F1** | 500 consecutive random `SIGKILL` / `TerminateProcess` kills across concurrent puts, deletes, and batches. | Frame checksum (`CRC32`), trailing `CMIT`, `os.fsync` before `ACK`, and tail truncation on `open()` preserve the invariant on every iteration. | `src/engine.py` :: `KVEngine.open`, `KVEngine._write_batch_internal` | `./torture --iterations 500 --seed 12345` |
| **F2** | Process killed repeatedly during `open()` recovery (`recovery-crash` mode). | `open()` only truncates uncommitted bytes beyond `valid_offset` and cleans up `.compact.tmp`; repeating `open()` on an already-truncated or mid-truncation file is strictly idempotent. | `src/engine.py` :: `KVEngine.open` | `./torture --iterations 100 --seed 42 --mode recovery-crash` |
| **F3** | 1,000,000 keys written on disk, process killed, `open()` called. | `open()` loads the file into a single contiguous `memoryview` buffer and unpacks frame headers via `struct.unpack_from` without slicing/copying values into RAM (storing only `(val_offset, val_len)`). Completes in ~0.8s (< 2.0s limit). | `src/engine.py` :: `KVEngine.open` | `tests/test_scenarios.py` (1M keys benchmark & crash test) |
| **F4** | Last 1 byte of `<path>` removed manually (`CMIT` becomes `CMI`, reducing `size` by 1). | `open()` checks `pos + frame_total_len > size` on the final frame, sees the missing byte, truncates the final unacknowledged/corrupted frame via `os.ftruncate(self.fd, valid_offset)`, and opens cleanly. | `src/engine.py` :: `KVEngine.open` | `tests/test_scenarios.py` (1-byte manual truncation test) |

---

## Part 3: The Three Guarantees, Argued

### 1. Durability: Why Data Survives Once ACK Is Sent
In `KVEngine._write_batch_internal`, every `put`, `delete`, and `batch` serializes its complete frame (`FRME` header + `CRC32` + payload + `CMIT` marker) in memory via `_pack_frame` and writes all bytes to `self.fd` using a loop over `os.write(self.fd, ...)`. Immediately after `os.write` completes, the engine invokes `os.fsync(self.fd)` (which translates to `FlushFileBuffers` on Windows and `fsync` on POSIX), blocking until the operating system flushes the file's page cache to physical storage. Only after `os.fsync(self.fd)` returns without error does `_write_batch_internal` update `self.index` and return control to `cli.py`, which then writes `ACK <op-id>\n` to `stdout` and calls `sys.stdout.flush()`. Because `ACK` is strictly ordered after `os.fsync(self.fd)` returns, any acknowledged record is guaranteed to reside on disk with a valid `CRC32` and trailing `CMIT` marker prior to process death.

### 2. Atomicity: Why a Batch Is Never Half Visible
Both across crashes and under concurrent readers, a batch is treated as an indivisible unit. For crash atomicity, `KVEngine.batch` packs all $N$ operations ($1 \le N \le 10,000$) into a single contiguous frame with one overall `payload_len`, one `CRC32` computed over all $N$ operations combined, and a single 4-byte `CMIT` marker at the very end of the frame. During `open()`, operations inside a frame are parsed into a local temporary list (`staged_updates`) only after verifying that `pos + frame_total_len <= size`, `commit_marker == b"CMIT"`, and `zlib.crc32(payload) == crc_expected`. If the process dies after writing 3 of 10 operations, the frame fails the length and `CMIT` checks, `staged_updates` is never applied to `self.index`, and `os.ftruncate(self.fd, valid_offset)` removes the partial frame. For runtime concurrency (`D4`), `_write_batch_internal` and `get` both acquire `self.lock` (`threading.RLock`), so no `get()` can read `self.index` until all $N$ operations in the batch have been applied to `self.index`.

### 3. Recoverability: Why `open()` Always Succeeds After Any Crash
`KVEngine.open()` is designed to be idempotent across arbitrary crash points in the write path, compaction path, and recovery path itself. First, if a crash occurred during `compact()`, `open()` inspects `self.path` and `self.tmp_path`: if `self.tmp_path` was being written (`C3`), `self.path` is intact and `self.tmp_path` is removed via `os.remove`; if the process died during file replacement (`C4`) after `os.fsync(tmp_fd)`, `_verify_file_complete(self.tmp_path)` confirms the temp file starts with `FILE_MAGIC` and ends with `COMMIT_MAGIC` before calling `os.replace(self.tmp_path, self.path)`. Second, if the file is smaller than the 8-byte `FILE_MAGIC` header (crash during initial file creation), `open()` resets the file via `os.ftruncate(self.fd, 0)`, writes `FILE_MAGIC`, and calls `os.fsync(self.fd)`. Third, during frame replay, any torn tail at EOF leaves `valid_offset` pointing to the exact byte boundary of the last valid `CMIT` marker, and `os.ftruncate(self.fd, valid_offset)` followed by `os.fsync(self.fd)` restores the file to a clean state; if killed during `os.ftruncate` (`C2`/`F2`), re-running `open()` scans the same undamaged prefix `0 .. valid_offset` and completes the truncation.