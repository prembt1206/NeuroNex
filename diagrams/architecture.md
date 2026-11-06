# NeuroNex KVDB Architecture Diagram

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