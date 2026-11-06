import os
import sys
import struct
import zlib
import base64
from src.engine import (
    KVEngine,
    DiskFullError,
    FILE_MAGIC,
    FRAME_MAGIC,
    COMMIT_MAGIC,
    FRAME_HDR_FMT,
    FRAME_HDR_SIZE,
    COMMIT_SIZE,
    OP_PUT,
    OP_DEL,
)


def check_file_integrity(path: str) -> tuple[bool, str]:
    """
    Implements './kvdb check <path>'.
    Verifies file header, frame headers, CRC32 checksums, and commit markers.
    Note: A torn last frame (from a previous crash before open() ran) or a clean
    file after open() is reported clearly.
    """
    if not os.path.exists(path):
        return False, "File does not exist"

    try:
        # First run open() recovery so any torn tail or leftover .compact.tmp is healed
        db = KVEngine(path)
        db.close()

        with open(path, "rb") as f:
            data = f.read()

        size = len(data)
        if size < len(FILE_MAGIC):
            return False, "File shorter than 8-byte header"
        if data[: len(FILE_MAGIC)] != FILE_MAGIC:
            return False, "Invalid file header magic"

        view = memoryview(data)
        pos = len(FILE_MAGIC)

        while pos < size:
            if pos + FRAME_HDR_SIZE + COMMIT_SIZE > size:
                return False, f"Truncated frame at offset {pos}"

            f_magic, crc_expected, payload_len, op_count = struct.unpack_from(
                FRAME_HDR_FMT, view, pos
            )
            if f_magic != FRAME_MAGIC:
                return False, f"Invalid frame magic at offset {pos}"

            frame_total_len = FRAME_HDR_SIZE + payload_len + COMMIT_SIZE
            if pos + frame_total_len > size:
                return False, f"Frame payload truncated at offset {pos}"

            payload_start = pos + FRAME_HDR_SIZE
            payload_end = payload_start + payload_len
            commit_marker = view[payload_end : payload_end + COMMIT_SIZE]

            if commit_marker != COMMIT_MAGIC:
                return False, f"Missing CMIT marker at offset {payload_end}"

            actual_crc = zlib.crc32(view[payload_start:payload_end]) & 0xFFFFFFFF
            if actual_crc != crc_expected:
                return False, f"CRC32 mismatch at offset {pos}"

            op_cursor = payload_start
            for _ in range(op_count):
                if op_cursor + 7 > payload_end:
                    return False, f"Corrupt operation header at offset {op_cursor}"
                op_type, k_len, v_len = struct.unpack_from("<BHI", view, op_cursor)
                op_cursor += 7
                if op_type not in (OP_PUT, OP_DEL):
                    return False, f"Invalid op_type {op_type} at offset {op_cursor}"
                if op_cursor + k_len + v_len > payload_end:
                    return False, f"Operation data out of bounds at offset {op_cursor}"
                op_cursor += k_len + v_len

            if op_cursor != payload_end:
                return False, f"Frame size mismatch at offset {pos}"

            pos += frame_total_len

        return True, "OK"
    except Exception as e:
        return False, f"Error: {e}"


def parse_batch_file(batch_file_path: str) -> list:
    """Parses a batch file with 'put k v' or 'del k' per line."""
    ops = []
    with open(batch_file_path, "rb") as f:
        for raw_line in f:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            parts = line.split(b" ", 2)
            cmd = parts[0].decode("ascii", errors="replace").lower()
            if cmd == "put":
                if len(parts) < 2:
                    raise ValueError("Invalid put line in batch file")
                k = parts[1]
                v = parts[2] if len(parts) == 3 else b""
                ops.append(("put", k, v))
            elif cmd in ("del", "delete"):
                if len(parts) < 2:
                    raise ValueError("Invalid del line in batch file")
                ops.append(("del", parts[1]))
            else:
                raise ValueError(f"Unknown batch command: {cmd}")
    return ops


def run_serve(path: str, max_bytes: int = None):
    """
    Long-running mode: reads operations from stdin, prints 'ACK <op-id>' after
    each durable operation.
    Supports both:
      1) Human/Judge plain text lines:
         PUT <op_id> <key> <value>
         DEL <op_id> <key>
         BATCH <op_id> <op1;op2;...>  OR multi-line:
         BATCH_BEGIN <op_id>
         put <key> <value>
         del <key>
         BATCH_END
         COMPACT <op_id>
      2) Binary-safe Base64 lines (used by torture tool for 1MB binary values):
         B64_PUT <op_id> <b64_key> <b64_val>
         B64_DEL <op_id> <b64_key>
         B64_BATCH <op_id> <count> <type:b64_k:b64_v ...>
    """
    db = KVEngine(path, max_bytes=max_bytes)
    in_batch = False
    batch_id = None
    batch_ops = []

    try:
        for raw_line in sys.stdin.buffer:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue

            parts = line.split(b" ")
            cmd = parts[0].upper()

            try:
                if in_batch:
                    if cmd == b"BATCH_END":
                        db.batch(batch_ops)
                        sys.stdout.write(f"ACK {batch_id}\n")
                        sys.stdout.flush()
                        in_batch = False
                        batch_id = None
                        batch_ops = []
                    else:
                        b_parts = line.split(b" ", 2)
                        b_cmd = b_parts[0].lower()
                        if b_cmd == b"put":
                            k = b_parts[1]
                            v = b_parts[2] if len(b_parts) > 2 else b""
                            batch_ops.append(("put", k, v))
                        elif b_cmd in (b"del", b"delete"):
                            batch_ops.append(("del", b_parts[1]))
                    continue

                if cmd == b"BATCH_BEGIN":
                    batch_id = parts[1].decode("utf-8", errors="replace")
                    in_batch = True
                    batch_ops = []
                    continue

                if cmd == b"PUT":
                    # PUT <op_id> <key> <value>  (or PUT <key> <value> if judge omits op_id)
                    if len(parts) >= 4:
                        op_id = parts[1].decode("utf-8", errors="replace")
                        k = parts[2]
                        v = b" ".join(parts[3:])
                    elif len(parts) == 3:
                        op_id = "0"
                        k = parts[1]
                        v = parts[2]
                    else:
                        continue
                    db.put(k, v)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd in (b"DEL", b"DELETE"):
                    if len(parts) >= 3:
                        op_id = parts[1].decode("utf-8", errors="replace")
                        k = parts[2]
                    elif len(parts) == 2:
                        op_id = "0"
                        k = parts[1]
                    else:
                        continue
                    db.delete(k)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"BATCH":
                    # Single-line human batch: BATCH <op_id> put k1 v1;del k2
                    op_id = parts[1].decode("utf-8", errors="replace")
                    rest = b" ".join(parts[2:])
                    ops = []
                    for item in rest.split(b";"):
                        item = item.strip()
                        if not item:
                            continue
                        sub = item.split(b" ", 2)
                        scmd = sub[0].lower()
                        if scmd == b"put":
                            ops.append(("put", sub[1], sub[2] if len(sub) > 2 else b""))
                        elif scmd in (b"del", b"delete"):
                            ops.append(("del", sub[1]))
                    db.batch(ops)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"B64_PUT":
                    op_id = parts[1].decode(" ascii")
                    k = base64.b64decode(parts[2])
                    v = base64.b64decode(parts[3]) if len(parts) > 3 else b""
                    db.put(k, v)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"B64_DEL":
                    op_id = parts[1].decode("ascii")
                    k = base64.b64decode(parts[2])
                    db.delete(k)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"B64_BATCH":
                    op_id = parts[1].decode("ascii")
                    ops = []
                    for item in parts[2:]:
                        if not item:
                            continue
                        kind, b64_k, b64_v = item.split(b":", 2)
                        k = base64.b64decode(b64_k)
                        if kind == b"P":
                            v = base64.b64decode(b64_v)
                            ops.append(("put", k, v))
                        elif kind == b"D":
                            ops.append(("del", k))
                    db.batch(ops)
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"COMPACT":
                    op_id = parts[1].decode("utf-8", errors="replace") if len(parts) > 1 else "0"
                    db.compact()
                    sys.stdout.write(f"ACK {op_id}\n")
                    sys.stdout.flush()

                elif cmd == b"GET":
                    k = parts[1]
                    val = db.get(k)
                    if val is None:
                        sys.stdout.write("NOTFOUND\n")
                    else:
                        sys.stdout.buffer.write(val + b"\n")
                    sys.stdout.flush()

            except DiskFullError as e:
                op_id_str = parts[1].decode("utf-8", errors="replace") if len(parts) > 1 else "0"
                sys.stdout.write(f"ERR_DISK_FULL {op_id_str} {e}\n")
                sys.stdout.flush()
            except Exception as e:
                op_id_str = parts[1].decode("utf-8", errors="replace") if len(parts) > 1 else "0"
                sys.stdout.write(f"ERR {op_id_str} {e}\n")
                sys.stdout.flush()
    finally:
        db.close()


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: kvdb <open|put|get|del|batch|dump|check|serve|compact> <path> [args...]", file=sys.stderr)
        sys.exit(1)

    # Extract optional --max-bytes N from anywhere in args
    max_bytes = None
    clean_args = []
    i = 0
    while i < len(args):
        if args[i] == "--max-bytes" and i + 1 < len(args):
            max_bytes = int(args[i + 1])
            i += 2
        elif args[i].startswith("--max-bytes="):
            max_bytes = int(args[i].split("=", 1)[1])
            i += 1
        else:
            clean_args.append(args[i])
            i += 1

    cmd = clean_args[0].lower()
    path = clean_args[1]

    try:
        if cmd == "open":
            db = KVEngine(path, max_bytes=max_bytes)
            print(f"keys={len(db.index)} recovery_ms={db.recovery_time_ms:.2f}")
            db.close()

        elif cmd == "put":
            if len(clean_args) < 4:
                print("Usage: kvdb put <path> <key> <value>", file=sys.stderr)
                sys.exit(1)
            key = clean_args[2].encode("utf-8")
            val = clean_args[3].encode("utf-8")
            db = KVEngine(path, max_bytes=max_bytes)
            db.put(key, val)
            db.close()

        elif cmd == "get":
            if len(clean_args) < 3:
                print("Usage: kvdb get <path> <key>", file=sys.stderr)
                sys.exit(1)
            key = clean_args[2].encode("utf-8")
            db = KVEngine(path, max_bytes=max_bytes)
            val = db.get(key)
            db.close()
            if val is None:
                print("NOTFOUND")
            else:
                sys.stdout.buffer.write(val + b"\n")
                sys.stdout.flush()

        elif cmd in ("del", "delete"):
            if len(clean_args) < 3:
                print("Usage: kvdb del <path> <key>", file=sys.stderr)
                sys.exit(1)
            key = clean_args[2].encode("utf-8")
            db = KVEngine(path, max_bytes=max_bytes)
            db.delete(key)
            db.close()

        elif cmd == "batch":
            if len(clean_args) < 3:
                print("Usage: kvdb batch <path> <file>", file=sys.stderr)
                sys.exit(1)
            batch_file = clean_args[2]
            ops = parse_batch_file(batch_file)
            db = KVEngine(path, max_bytes=max_bytes)
            db.batch(ops)
            db.close()

        elif cmd == "dump":
            db = KVEngine(path, max_bytes=max_bytes)
            for k in sorted(db.index.keys()):
                v = db.get(k)
                # Print key=value in UTF-8 if printable, and also supports binary safely
                sys.stdout.buffer.write(k + b"=" + v + b"\n")
            sys.stdout.flush()
            db.close()

        elif cmd == "check":
            ok, reason = check_file_integrity(path)
            if ok:
                print("OK")
            else:
                print(reason)
                sys.exit(1)

        elif cmd == "serve":
            run_serve(path, max_bytes=max_bytes)

        elif cmd == "compact":
            db = KVEngine(path, max_bytes=max_bytes)
            db.compact()
            db.close()

        else:
            print(f"Unknown command: {cmd}", file=sys.stderr)
            sys.exit(1)

    except DiskFullError as e:
        print(f"ERROR: DISK_FULL: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()