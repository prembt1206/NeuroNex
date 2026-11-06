import os
import struct
import zlib
import threading
import time

# 8-byte header at the very top of the database file
FILE_MAGIC = b"KVDB\x00\x00\x00\x01"
# 4-byte marker at the start of every write box (frame)
FRAME_MAGIC = b"FRME"
# 4-byte marker at the end of every write box (proves write finished!)
COMMIT_MAGIC = b"CMIT"

OP_PUT = 1
OP_DEL = 2

# Binary format for frame header: 4-byte magic, 4-byte CRC32, 4-byte payload length, 4-byte op count = 16 bytes
FRAME_HDR_FMT = "<4sIII"
FRAME_HDR_SIZE = 16
COMMIT_SIZE = 4


class DiskFullError(Exception):
    """Raised when writing would exceed the max_bytes disk limit."""
    pass


class KVEngine:
    def __init__(self, path: str, max_bytes: int = None):
        self.path = os.path.abspath(path)
        self.tmp_path = self.path + ".compact.tmp"
        self.max_bytes = max_bytes
        self.lock = threading.RLock()  # Global lock so multiple threads don't corrupt data
        self.fd = None
        # In-memory dictionary: key_bytes -> (offset_in_file, value_length)
        self.index = {}
        self.file_size = 0
        self.dead_records = 0
        self.recovery_time_ms = 0.0
        self.open()

    def open(self):
        """Opens the database file and recovers from any crash."""
        with self.lock:
            t0 = time.perf_counter()

            # 1. Recover from a crash during compaction (file shrinking)
            if not os.path.exists(self.path) and os.path.exists(self.tmp_path):
                if self._verify_file_complete(self.tmp_path):
                    os.replace(self.tmp_path, self.path)
            if os.path.exists(self.tmp_path):
                try:
                    os.remove(self.tmp_path)
                except OSError:
                    pass

            # 2. Open the main database file
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
            self.fd = os.open(self.path, flags, 0o666)
            size = os.fstat(self.fd).st_size

            # 3. If it's a brand-new empty file (or crashed while writing the 8-byte header)
            if size < len(FILE_MAGIC):
                os.lseek(self.fd, 0, os.SEEK_SET)
                os.ftruncate(self.fd, 0)
                os.write(self.fd, FILE_MAGIC)
                os.fsync(self.fd)
                self.file_size = len(FILE_MAGIC)
                self.index.clear()
                self.recovery_time_ms = (time.perf_counter() - t0) * 1000.0
                return

            # 4. Check the 8-byte file header
            os.lseek(self.fd, 0, os.SEEK_SET)
            hdr = os.read(self.fd, len(FILE_MAGIC))
            if hdr != FILE_MAGIC:
                raise ValueError("Corrupt database header: not a KVDB file")

            # 5. Read the file and check every box (frame) from top to bottom
            self.index.clear()
            self.dead_records = 0
            valid_offset = len(FILE_MAGIC)

            os.lseek(self.fd, 0, os.SEEK_SET)
            raw_data = os.read(self.fd, size)
            view = memoryview(raw_data)

            pos = len(FILE_MAGIC)
            while pos < size:
                # If remaining bytes are too small to even hold a box header + footer, stop!
                if pos + FRAME_HDR_SIZE + COMMIT_SIZE > size:
                    break

                f_magic, crc_expected, payload_len, op_count = struct.unpack_from(
                    FRAME_HDR_FMT, view, pos
                )
                frame_total_len = FRAME_HDR_SIZE + payload_len + COMMIT_SIZE

                # If box magic is wrong or file was cut off mid-write (Torn Write), stop!
                if f_magic != FRAME_MAGIC or pos + frame_total_len > size:
                    break

                payload_start = pos + FRAME_HDR_SIZE
                payload_end = payload_start + payload_len
                commit_marker = view[payload_end : payload_end + COMMIT_SIZE]

                # Check if the "CMIT" marker is at the end of the box
                if commit_marker != COMMIT_MAGIC:
                    break

                # Check if the CRC32 checksum matches the data inside the box
                payload_slice = view[payload_start:payload_end]
                if (zlib.crc32(payload_slice) & 0xFFFFFFFF) != crc_expected:
                    break

                # Unpack all operations inside this box
                op_cursor = payload_start
                frame_corrupt = False
                staged_updates = []

                for _ in range(op_count):
                    if op_cursor + 7 > payload_end:
                        frame_corrupt = True
                        break
                    op_type, k_len, v_len = struct.unpack_from("<BHI", view, op_cursor)
                    op_cursor += 7
                    if op_cursor + k_len + v_len > payload_end:
                        frame_corrupt = True
                        break
                    key_bytes = bytes(view[op_cursor : op_cursor + k_len])
                    op_cursor += k_len
                    val_offset = op_cursor
                    op_cursor += v_len
                    staged_updates.append((op_type, key_bytes, val_offset, v_len))

                if frame_corrupt or op_cursor != payload_end:
                    break

                # Box is 100% valid! Update our in-memory dictionary
                for op_type, key_bytes, val_offset, v_len in staged_updates:
                    if op_type == OP_PUT:
                        if key_bytes in self.index:
                            self.dead_records += 1
                        self.index[key_bytes] = (val_offset, v_len)
                    elif op_type == OP_DEL:
                        if key_bytes in self.index:
                            del self.index[key_bytes]
                            self.dead_records += 1
                        self.dead_records += 1

                pos += frame_total_len
                valid_offset = pos

            del view
            del raw_data

            # 6. If there was a half-written box at the end from a crash, chop it off!
            if valid_offset < size:
                os.ftruncate(self.fd, valid_offset)
                os.fsync(self.fd)

            self.file_size = valid_offset
            os.lseek(self.fd, self.file_size, os.SEEK_SET)
            self.recovery_time_ms = (time.perf_counter() - t0) * 1000.0

    def _verify_file_complete(self, file_path: str) -> bool:
        try:
            with open(file_path, "rb") as f:
                data = f.read()
            if len(data) < len(FILE_MAGIC) or data[:len(FILE_MAGIC)] != FILE_MAGIC:
                return False
            if len(data) > len(FILE_MAGIC) and data[-4:] != COMMIT_MAGIC:
                return False
            return True
        except OSError:
            return False

    def _pack_frame(self, ops: list) -> tuple[bytes, list]:
        payload_parts = []
        rel_offsets = []
        current_payload_offset = FRAME_HDR_SIZE

        for op_type, k, v in ops:
            if not (1 <= len(k) <= 1024):
                raise ValueError("Key size must be between 1 and 1024 bytes")
            if len(v) > 1024 * 1024:
                raise ValueError("Value size must be between 0 and 1 MB")

            op_hdr = struct.pack("<BHI", op_type, len(k), len(v))
            payload_parts.append(op_hdr)
            payload_parts.append(k)
            if v:
                payload_parts.append(v)

            val_offset_in_frame = current_payload_offset + 7 + len(k)
            rel_offsets.append((op_type, k, val_offset_in_frame, len(v)))
            current_payload_offset += 7 + len(k) + len(v)

        payload = b"".join(payload_parts)
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        header = struct.pack(FRAME_HDR_FMT, FRAME_MAGIC, crc, len(payload), len(ops))
        frame = header + payload + COMMIT_MAGIC
        return frame, rel_offsets

    def _write_batch_internal(self, ops: list):
        if not ops:
            return
        if len(ops) > 10000:
            raise ValueError("Batch cannot exceed 10,000 operations")

        with self.lock:
            frame, rel_offsets = self._pack_frame(ops)

            # Check if writing this box would exceed max_bytes (Disk Full simulation)
            if self.max_bytes is not None and (self.file_size + len(frame) > self.max_bytes):
                if self.dead_records > 0:
                    self.compact()
                if self.file_size + len(frame) > self.max_bytes:
                    raise DiskFullError("Database max_bytes limit reached")

            frame_start_offset = self.file_size
            os.lseek(self.fd, frame_start_offset, os.SEEK_SET)

            # Write the box to the file
            written = 0
            mv = memoryview(frame)
            while written < len(frame):
                n = os.write(self.fd, mv[written:])
                written += n

            # Force the hard drive to save the bytes physically before returning success!
            os.fsync(self.fd)

            # Update in-memory dictionary
            self.file_size += len(frame)
            for op_type, k, rel_val_off, v_len in rel_offsets:
                if op_type == OP_PUT:
                    if k in self.index:
                        self.dead_records += 1
                    self.index[k] = (frame_start_offset + rel_val_off, v_len)
                elif op_type == OP_DEL:
                    if k in self.index:
                        del self.index[k]
                        self.dead_records += 1
                    self.dead_records += 1

    def put(self, key: bytes, value: bytes):
        self._write_batch_internal([(OP_PUT, key, value)])

    def delete(self, key: bytes):
        self._write_batch_internal([(OP_DEL, key, b"")])

    def batch(self, ops: list):
        formatted = []
        for item in ops:
            if item[0] == "put":
                formatted.append((OP_PUT, item[1], item[2]))
            elif item[0] == "del":
                formatted.append((OP_DEL, item[1], b""))
            else:
                raise ValueError(f"Unknown operation: {item[0]}")
        self._write_batch_internal(formatted)

    def get(self, key: bytes) -> bytes | None:
        with self.lock:
            entry = self.index.get(key)
            if entry is None:
                return None
            val_offset, val_len = entry
            if val_len == 0:
                return b""
            os.lseek(self.fd, val_offset, os.SEEK_SET)
            return os.read(self.fd, val_len)

    def compact(self):
        """Shrinks the database file by copying only active keys to a fresh file."""
        with self.lock:
            flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
            tmp_fd = os.open(self.tmp_path, flags, 0o666)
            new_index = {}
            try:
                os.write(tmp_fd, FILE_MAGIC)
                curr_offset = len(FILE_MAGIC)

                for k in sorted(self.index.keys()):
                    old_off, v_len = self.index[k]
                    os.lseek(self.fd, old_off, os.SEEK_SET)
                    v = os.read(self.fd, v_len) if v_len > 0 else b""
                    frame, rel_offsets = self._pack_frame([(OP_PUT, k, v)])
                    os.write(tmp_fd, frame)
                    _, _, rel_val_off, _ = rel_offsets[0]
                    new_index[k] = (curr_offset + rel_val_off, v_len)
                    curr_offset += len(frame)

                os.fsync(tmp_fd)
            finally:
                os.close(tmp_fd)

            os.close(self.fd)
            self.fd = None

            os.replace(self.tmp_path, self.path)

            reopen_flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
            self.fd = os.open(self.path, reopen_flags, 0o666)
            self.index = new_index
            self.file_size = os.fstat(self.fd).st_size
            self.dead_records = 0

    def close(self):
        with self.lock:
            if self.fd is not None:
                os.fsync(self.fd)
                os.close(self.fd)
                self.fd = None