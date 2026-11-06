# NeuroNex KVDB — Crash-Safe Embedded Key-Value Storage Engine

A single-process, crash-safe embedded key-value storage engine built from scratch in Python (standard library only) with an atomic append-only frame log, CRC32 payload verification, explicit commit markers, and crash-safe compaction.

---

## 1. Prerequisites & Build Instructions

Requires **Python 3.10+** (standard library only; zero external dependencies).

### Build / Verify Installation (One Command)
Run from the repository root (`NeuroNex/`):

```bash
python -m py_compile src/engine.py src/cli.py torture/torture.py
```

---

## 2. Running the CLI (`./kvdb`)

All 8 mandatory CLI commands run directly from the repository root (`./kvdb` works on PowerShell, CMD via `kvdb.bat`, and Bash via `kvdb`):

```bash
# 1. Open or create a database and run crash recovery (with optional max_bytes limit)
./kvdb open <path> [--max-bytes N]

# 2. Put a key-value pair (durable before returning)
./kvdb put <path> <key> <value>

# 3. Get a key's value (prints value or NOTFOUND)
./kvdb get <path> <key>

# 4. Delete a key (durable tombstone; deleting missing key is a no-op success)
./kvdb del <path> <key>

# 5. Apply an atomic batch from a file (each line: "put <k> <v>" or "del <k>")
./kvdb batch <path> <file>

# 6. Dump all live keys and values sorted lexicographically by key
./kvdb dump <path>

# 7. Verify full file integrity (prints OK or error reason)
./kvdb check <path>

# 8. Long-running server mode (reads commands from stdin, prints "ACK <op-id>" after fsync)
./kvdb serve <path> [--max-bytes N]
```

### Intra-Batch Ordering Rule (Scenario B5)
When a `batch` contains multiple operations on the same key (for example, `put X v1` followed by `del X`, or `del X` followed by `put X v2`), operations inside the batch are evaluated **strictly sequentially from first to last** (index `0` to `op_count - 1`). Both the live write path and `open()` recovery replay apply operations in this exact order.

---

## 3. Running the Torture Tool (`./torture`)

```bash
# Default 500-iteration random kill test
./torture --iterations 500 --seed 12345

# Targeted modes
./torture --iterations 100 --seed 999 --mode large-values
./torture --iterations 100 --seed 555 --mode large-batches
./torture --iterations 100 --seed 7 --mode compaction
./torture --iterations 100 --seed 42 --mode recovery-crash
./torture --iterations 100 --seed 88 --mode disk-full
```

---

## 4. On-Disk Binary Format

All authoritative state lives in a single file (`<path>`). Multi-byte integers are encoded in **little-endian** (`<`) byte order.

```text
===============================================================================
FILE HEADER (8 Bytes, Offset 0)
===============================================================================
+-------------------------------+-------------------------------+
| Magic Bytes (4B): "KVDB"      | Format Version (4B): 1        |
| 0x4B 0x56 0x44 0x42           | 0x00 0x00 0x00 0x01           |
+-------------------------------+-------------------------------+

===============================================================================
ATOMIC LOG FRAME (Repeated sequentially from Offset 8 to EOF)
Every put(), delete(), and batch() is written as one unified Atomic Frame.
===============================================================================
+-------------------------------+-------------------------------+
| Frame Magic (4B): "FRME"      | Payload CRC32 (4B, uint32)    |
| 0x46 0x52 0x4D 0x45           | zlib.crc32(Payload)           |
+-------------------------------+-------------------------------+
| Payload Length (4B, uint32)   | Op Count (4B, uint32)         |
| Byte length of Payload        | Number of ops in this frame   |
+-------------------------------+-------------------------------+
|PAYLOAD (Variable Length = Payload Length bytes)               |
|  For each operation i in 0 .. Op Count - 1:                   |
|  +----------------------------+----------------------------+  |
|  | Op Type (1B, uint8)        | Key Length (2B, uint16)    |  |
|  | 1 = PUT, 2 = DEL           | 1 .. 1024 bytes            |  |
|  +----------------------------+----------------------------+  |
|  | Value Length (4B, uint32)  | Key Bytes (Key Length B)   |  |
|  | 0 .. 1,048,576 bytes       | Raw key bytes              |  |
|  +----------------------------+----------------------------+  |
|  | Value Bytes (Value Length B, 0 bytes for DEL)           |  |
|  +---------------------------------------------------------+  |
+---------------------------------------------------------------+
| Commit Marker (4B): "CMIT" (0x43 0x4D 0x49 0x54)              |
+---------------------------------------------------------------+
```

---

## 5. Recovery Algorithm in Plain English (`open()`)

1. **Acquire Global Lock & Resolve Compaction Residuals:**
   * If `<path>` is absent and `<path>.compact.tmp` exists (process was killed on Windows during the brief file swap window in `compact()`), `open()` verifies that `<path>.compact.tmp` has a valid `KVDB` header and ends with a complete `CMIT` marker, then renames `<path>.compact.tmp` to `<path>`.
   * If both `<path>` and `<path>.compact.tmp` exist (process was killed while building the temp file before swap), `<path>` is still authoritative and `<path>.compact.tmp` is deleted.
2. **Validate or Initialize Header:**
   * If the file size is less than 8 bytes (brand-new file or killed during initial header write), `open()` truncates the file to `0`, writes the 8-byte `KVDB\x00\x00\x00\x01` header, calls `os.fsync()`, and finishes.
   * Otherwise, it verifies the first 8 bytes equal `KVDB\x00\x00\x00\x01`.
3. **Sequential Frame Scan & Validation:**
   * Starting at byte offset `8`, `open()` reads each frame header (`FRME`, `crc_expected`, `payload_len`, `op_count`).
   * It checks three conditions before accepting a frame:
     1. Enough bytes remain in the file for the full frame (`pos + 16 + payload_len + 4 <= file_size`).
     2. The 4 bytes immediately following the payload equal `"CMIT"`.
     3. `zlib.crc32(payload)` matches `crc_expected`, and all internal operation lengths sum to `payload_len`.
   * If valid, all operations in the frame are applied in order to the in-memory hash index (`key -> (val_offset, val_len)`), and `valid_offset` advances to the end of the frame.
4. **Tail Truncation of Torn Writes:**
   * If any check fails at the tail of the log (due to a torn write, missing commit marker, or manual byte truncation), scanning stops immediately.
   * `open()` calls `os.ftruncate(fd, valid_offset)` followed by `os.fsync(fd)` to strip the uncommitted tail cleanly.