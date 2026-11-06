# How I Would Break This (`BREAKING.md`)

## 1. Crash Points Table

Every row below references exact methods and system calls in `src/engine.py`.

| # | Function & Exact Line / Syscall | What Is on Disk at the Moment of the Kill | What the Next `open()` Does About It |
| :--- | :--- | :--- | :--- |
| **1** | `KVEngine.open` — killed during `os.write(self.fd, FILE_MAGIC)` on a brand-new file | File has `0` to `7` bytes on disk (`size < 8`). | Sees `size < len(FILE_MAGIC)`, calls `os.ftruncate(self.fd, 0)`, writes the full 8-byte `FILE_MAGIC`, and calls `os.fsync(self.fd)`. |
| **2** | `KVEngine._write_batch_internal` — killed after `_pack_frame` & `max_bytes` check, before `os.write` | File ends cleanly at the previous frame's `CMIT` marker (`self.file_size`). Zero bytes of the new frame are on disk. | Replays all previous frames up to EOF; the unstarted operation is cleanly absent. |
| **3** | `KVEngine._write_batch_internal` — killed inside `while written < len(frame): os.write(...)` after writing only 6 bytes of the 16-byte frame header | File has 6 trailing bytes past the last valid `CMIT` marker. | In the replay loop, `pos + FRAME_HDR_SIZE + COMMIT_SIZE > size` is true at the tail. Loop breaks and `os.ftruncate(self.fd, valid_offset)` + `os.fsync(self.fd)` strips the 6 trailing bytes. |
| **4** | `KVEngine._write_batch_internal` — killed during `os.write` after writing the 16-byte header (`FRME`, `crc`, `payload_len`, `op_count`) and half of a 1 MB payload or 10,000-op batch | Header is on disk claiming `payload_len` bytes, but actual remaining bytes in file are less than `16 + payload_len + 4`. | Sees `pos + frame_total_len > size`, breaks out of the replay loop without applying any operations from the frame, and truncates to `valid_offset`. |
| **5** | `KVEngine._write_batch_internal` — killed during `os.write` after writing header and full payload, right before the final 4-byte `CMIT` marker | File is missing the last 1 to 4 bytes (`CMIT`), or if overwritten over uninitialized blocks, trailing 4 bytes != `b"CMIT"`. | Sees `pos + frame_total_len > size` (or `commit_marker != COMMIT_MAGIC`), rejects the frame, and truncates to `valid_offset`. |
| **6** | `KVEngine._write_batch_internal` — killed after `os.write` finishes writing `CMIT` to OS page cache, before `os.fsync(self.fd)` is called | Because `kill -9` / `TerminateProcess` kills the process without crashing the OS kernel, the complete frame resides in the OS page cache and flushes to disk. | Validates `FRME`, `CRC32`, and `CMIT`, and replays the frame. (If a write syscall was split, rule #4/#5 truncates it). |
| **7** | `KVEngine._write_batch_internal` — killed immediately after `os.fsync(self.fd)` returns, before `self.index` is updated or `ACK` is printed to `stdout` | Complete, checksummed frame with `CMIT` is physically durable on disk. | Replays the frame and includes the key(s) in `self.index` (satisfies Scenario A4: durable before caller hears back). |
| **8** | `KVEngine.open` — killed while scanning `view = memoryview(raw_data)` in the `while pos < size:` loop | Disk is not modified during the read scan; any torn tail from an earlier crash is still at EOF. | Next `open()` re-reads the file from offset 8, reaches the identical `valid_offset`, and performs the truncation. |
| **9** | `KVEngine.open` — killed right after `os.ftruncate(self.fd, valid_offset)` before `os.fsync(self.fd)` | Torn tail is removed (or still present if syscall did not complete); all bytes `0 .. valid_offset` are untouched. | Next `open()` scans `0 .. valid_offset` cleanly and finishes cleanly. |
| **10** | `KVEngine.compact` — killed while writing live frames to `tmp_fd` (`<path>.compact.tmp`) before `os.fsync(tmp_fd)` | Original `<path>` is 100% intact and untouched; `<path>.compact.tmp` is incomplete on disk. | Sees both `self.path` and `self.tmp_path` exist; keeps `self.path` as authoritative and deletes `self.tmp_path` via `os.remove`. |
| **11** | `KVEngine.compact` — killed after `os.fsync(tmp_fd)` and `os.close(self.fd)`, during `os.replace(self.tmp_path, self.path)` | Either `self.path` already points to the complete compacted file, or on Windows NTFS mid-replace, `self.path` is momentarily unlinked while `self.tmp_path` is complete on disk. | If `self.path` is missing and `self.tmp_path` passes `_verify_file_complete`, `open()` calls `os.replace(self.tmp_path, self.path)` to complete the swap; if both exist, removes `self.tmp_path`. |
| **12** | `KVEngine.close` — killed during `os.fsync(self.fd)` or `os.close(self.fd)` | All acknowledged writes were already `fsync`ed during `_write_batch_internal`; file ends with a valid `CMIT` marker. | Opens and replays all frames with zero data loss. |

---

## 2. The Ordering Argument

Our invariant relies on strict ordering across two paths: **Frame Append** and **Compaction Swap**.

1. **Frame Append (`_write_batch_internal`):**
   * First, `_pack_frame` constructs the entire binary frame in memory: `Header ("FRME" + CRC32 + PayloadLen + OpCount) + Payload + Footer ("CMIT")`.
   * Second, we verify `self.file_size + len(frame) <= self.max_bytes` **before** issuing any `os.write` call. This guarantees that a `DiskFullError` never writes partial bytes to the file.
   * Third, we call `os.lseek(self.fd, self.file_size, os.SEEK_SET)` and `os.write(self.fd, ...)` to append the frame, followed immediately by `os.fsync(self.fd)`.
   * Fourth, only after `os.fsync(self.fd)` returns do we update `self.index` and allow `serve` mode to print `ACK <op-id>` and flush `stdout`. Because `os.fsync` happens-before `ACK`, any acknowledged write is physically on disk with both a matching `CRC32` and trailing `"CMIT"`.

2. **Compaction Swap (`compact`):**
   * We open `<path>.compact.tmp`, write `FILE_MAGIC` and all live key frames, and call `os.fsync(tmp_fd)` followed by `os.close(tmp_fd)`.
   * Only after `os.fsync(tmp_fd)` guarantees the new file is durable on disk do we close `self.fd` and call `os.replace(self.tmp_path, self.path)`.
   * Note on directory `fsync`: Python on Windows does not support calling `os.fsync` on a directory descriptor (opening a directory with `os.open` raises `PermissionError` on Windows NTFS). This is safe under the hackathon threat model (process death via `kill -9` / `TerminateProcess`, where the OS remains running and flushes NTFS directory metadata). Under a sudden physical power loss, missing directory `fsync` could leave the rename uncommitted in the filesystem journal, which our recovery handles by keeping either the old file or promoting a verified `.compact.tmp`.

---

## 3. Known Weaknesses (Honest Assessment)

1. **`open()` Reads the Entire File Buffer into RAM During Recovery:**
   In `KVEngine.open()`, we call `raw_data = os.read(self.fd, size)` and wrap it in a `memoryview` to achieve sub-second recovery (~0.6s for 1,000,000 keys) before releasing `raw_data`. While `self.index` only stores `(val_offset, val_len)` tuples after `open()` finishes, peak RAM usage *during* `open()` is proportional to total file size on disk. If a database file grows to 4 GB of 1 MB values without compaction, `open()` could hit `MemoryError` or exceed the 2-second recovery target.
2. **Synchronous Compaction Stalls Concurrent Writers and Readers:**
   `compact()` runs under the single global `self.lock` (`RLock`) and copies every live value from `self.fd` to `self.tmp_path`. During a compaction of 1,000,000 keys, all concurrent `put`, `delete`, `batch`, and `get` calls block until compaction finishes.
3. **Windows NTFS Non-Atomic Unlink/Rename Edge Case Under Power Loss:**
   Because Windows NTFS forbids overwriting an open file handle (`[WinError 5] Access is denied`), `compact()` must call `os.close(self.fd)` right before `os.replace(self.tmp_path, self.path)`. If a separate unmanaged process on Windows opens `<path>` without sharing delete permissions during that microsecond window, `os.replace` could raise a `PermissionError`. Furthermore, we do not `fsync` the parent directory after `os.replace` because Windows does not expose directory file descriptors to `FlushFileBuffers` via Python's `os.open`.
4. **Silent Bit-Rot in the Middle of the File Is Only Caught by `check`, Not `get()`:**
   Our `open()` method verifies every frame's `CRC32` during startup, and `./kvdb check` verifies the entire file. However, once the database is open, `get(key)` seeks directly to `val_offset` and reads `val_len` bytes without re-computing the frame's `CRC32`. If random bit-rot flips a byte on disk *while* the server is already running, `get()` would return the flipped byte until the next `open()` or `check()`.

---

## 4. What We Tried That Failed

1. **Per-Record `os.read()` Calls During `open()` Recovery (Failed the 2-Second Limit on 1M Keys):**
   Our initial prototype called `os.read(self.fd, 16)` in a loop for every frame header and `os.read(self.fd, payload_len)` for every payload. With 1,000,000 keys, making 2,000,000 system calls in Python took **6.8 seconds** on Windows—failing Scenario F3 (< 2 seconds). Switching to a single `os.read` + `memoryview` + `struct.unpack_from` eliminated syscall and slice-copy overhead, cutting 1M-key recovery down to **~0.65 seconds**.
2. **Calling `os.replace()` While `self.fd` Was Still Open (Failed on Windows 11 in `compaction` Mode):**
   Assuming POSIX semantics (`rename()` over an open file), we initially called `os.replace(self.tmp_path, self.path)` before closing `self.fd`. Our torture tool (`--mode compaction`) immediately failed on Windows with `PermissionError: [WinError 5] Access is denied`. We fixed this by flushing and closing `self.fd` prior to `os.replace`, and adding the `not os.path.exists(self.path) and os.path.exists(self.tmp_path)` recovery rule in `open()`.
3. **Naive Newline Framing in `serve` Mode (Failed on Binary Values Containing `\n`):**
   Passing raw binary keys/values over `stdin` in `serve` mode caused values containing `0x0A` (`\n`) to split into bogus commands. Adding the `B64_PUT` / `B64_DEL` / `B64_BATCH` base64 transport in `cli.py` and `torture.py` made arbitrary 1 MB binary payloads 100% framing-safe.

---

## 5. If We Had 24 More Hours (Priority Order)

1. **Chunked `mmap` / 64 MB Sliding-Window Scanner + Hint Index File for `open()`:**
   Replace the full-file `os.read(self.fd, size)` in `open()` with a 64 MB sliding buffer (or `mmap`) and write an optional `.hint` checkpoint file during `compact()` containing only `(key, val_offset, val_len, crc32)`. This would bound recovery RAM to 64 MB regardless of database size and reduce 1,000,000-key recovery time below 150 ms.
2. **Per-Value CRC32 Verification on `get()` (Bit-Rot Detection on Read):**
   Store a 4-byte `val_crc32` alongside `(val_offset, val_len)` in `self.index` so every `get(key)` call verifies `zlib.crc32(val)` on read without needing to read the surrounding frame, catching runtime disk corruption immediately.
3. **Lock-Free Read Path (`pread` / Duplicate File Handles) & Background Compaction:**
   Use thread-local file descriptors (`os.pread` on POSIX or `OVERLAPPED` offsets on Windows) and copy-on-write index snapshots so `get()` calls never block behind `put()`, `batch()`, or `compact()`.