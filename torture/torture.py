import argparse
import base64
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time


def hard_kill(proc: subprocess.Popen):
    """Kills a child process immediately with no chance for cleanup (SIGKILL / TerminateProcess)."""
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            # Windows equivalent of kill -9
            proc.kill()
        else:
            os.kill(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass


class OracleTracker:
    """
    Independent truth tracker owned by the torture tool.
    Tracks confirmed (ACKed) operations and in-flight (un-ACKed at moment of kill) operations.
    """
    def __init__(self, oracle_path: str):
        self.oracle_path = oracle_path
        self.lock = threading.Lock()
        self.op_counter = 0
        self.ops_by_id = {}       # op_id -> {"type": ..., "ops": [(kind, k_bytes, v_bytes)], "seq": int}
        self.acked_ids = set()    # op_ids that received ACK from stdout
        self.send_order = []      # ordered list of op_ids sent to stdin
        self.confirmed_state = {} # key_bytes -> val_bytes (only strictly ACKed prior to current iteration)

    def register_op(self, kind: str, ops_list: list) -> str:
        with self.lock:
            self.op_counter += 1
            op_id = f"op_{self.op_counter}"
            self.ops_by_id[op_id] = {
                "type": kind,
                "ops": ops_list,
                "seq": len(self.send_order),
            }
            self.send_order.append(op_id)
            return op_id

    def record_ack(self, op_id: str):
        with self.lock:
            if op_id in self.ops_by_id:
                self.acked_ids.add(op_id)

    def save_oracle_log(self):
        """Persists ACKed truth to disk before running verification."""
        with self.lock:
            serializable = {
                "acked_count": len(self.acked_ids),
                "total_sent": len(self.send_order),
            }
            with open(self.oracle_path, "w", encoding="utf-8") as f:
                json.dump(serializable, f)


def encode_op_line(op_id: str, kind: str, ops_list: list) -> bytes:
    """Encodes an operation into a single atomic line for ./kvdb serve."""
    if kind == "put":
        _, k, v = ops_list[0]
        b64_k = base64.b64encode(k).decode("ascii")
        b64_v = base64.b64encode(v).decode("ascii")
        return f"B64_PUT {op_id} {b64_k} {b64_v}\n".encode("ascii")
    elif kind == "del":
        _, k, _ = ops_list[0]
        b64_k = base64.b64encode(k).decode("ascii")
        return f"B64_DEL {op_id} {b64_k}\n".encode("ascii")
    elif kind == "batch":
        parts = [f"B64_BATCH {op_id}"]
        for sub_kind, k, v in ops_list:
            b64_k = base64.b64encode(k).decode("ascii")
            if sub_kind == "put":
                b64_v = base64.b64encode(v).decode("ascii")
                parts.append(f"P:{b64_k}:{b64_v}")
            else:
                parts.append(f"D:{b64_k}:")
        return (" ".join(parts) + "\n").encode("ascii")
    elif kind == "compact":
        return f"COMPACT {op_id}\n".encode("ascii")
    raise ValueError(f"Unknown kind: {kind}")


def apply_ops_to_dict(state: dict, ops_list: list):
    for sub_kind, k, v in ops_list:
        if sub_kind == "put":
            state[k] = v
        elif sub_kind == "del":
            state.pop(k, None)


def generate_workload_op(rng: random.Random, mode: str, key_pool: list) -> tuple[str, list]:
    """Generates a random operation according to the active test mode."""
    if mode == "large-values":
        # 1 MB values to force multi-block disk writes and torn writes (Scenario A7)
        k = rng.choice(key_pool[:20])
        size = rng.choice([262144, 524288, 1048576])  # 256KB to 1MB
        v = (f"VAL_{rng.randint(1, 999999)}_".encode("ascii") * (size // 12 + 1))[:size]
        return "put", [("put", k, v)]

    if mode == "large-batches":
        # Up to 10,000 operations in one atomic batch (Scenario B6)
        count = rng.choice([1000, 5000, 10000])
        ops = []
        for i in range(count):
            k = f"bkey_{i % 500}".encode("ascii")
            if rng.random() < 0.8:
                v = f"bval_{rng.randint(1, 100000)}".encode("ascii")
                ops.append(("put", k, v))
            else:
                ops.append(("del", k, b""))
        return "batch", ops

    if mode == "compaction":
        r = rng.random()
        if r < 0.20:
            return "compact", []
        elif r < 0.60:
            k = rng.choice(key_pool[:30])
            v = f"cval_{rng.randint(1, 999999)}".encode("ascii") * 20
            return "put", [("put", k, v)]
        else:
            k = rng.choice(key_pool[:30])
            return "del", [("del", k, b"")]

    # Default / disk-full / recovery-crash mixed workload
    r = rng.random()
    if r < 0.50:
        k = rng.choice(key_pool)
        v = f"v_{rng.randint(1, 1000000)}".encode("ascii")
        return "put", [("put", k, v)]
    elif r < 0.75:
        k = rng.choice(key_pool)
        return "del", [("del", k, b"")]
    else:
        # Batch of 3 to 15 operations (including intra-batch put+del on same key for Scenario B5)
        b_len = rng.randint(3, 15)
        ops = []
        for _ in range(b_len):
            k = rng.choice(key_pool)
            if rng.random() < 0.7:
                v = f"bv_{rng.randint(1, 1000000)}".encode("ascii")
                ops.append(("put", k, v))
            else:
                ops.append(("del", k, b""))
        return "batch", ops


def verify_database_state(
    db_path: str,
    base_state: dict,
    iter_ops: list,
    acked_set: set,
    ops_by_id: dict,
) -> tuple[bool, str, dict]:
    """
    Runs './kvdb open', './kvdb check', and './kvdb dump' and verifies the invariant:
    1. Every ACKed operation is present.
    2. Every un-ACKed operation is either 100% present or 100% absent (in write order).
    3. Every batch is atomic (all-or-nothing).
    4. No corrupted or unknown values exist.
    """
    # 1. Run open
    open_proc = subprocess.run(
        [sys.executable, "kvdb", "open", db_path],
        capture_output=True,
        text=True,
    )
    if open_proc.returncode != 0:
        return False, f"open() failed: {open_proc.stderr}", {}

    # 2. Run check
    check_proc = subprocess.run(
        [sys.executable, "kvdb", "check", db_path],
        capture_output=True,
        text=True,
    )
    if check_proc.returncode != 0 or check_proc.stdout.strip() != "OK":
        return False, f"check() failed: {check_proc.stdout} {check_proc.stderr}", {}

    # 3. Run dump
    dump_proc = subprocess.run(
        [sys.executable, "kvdb", "dump", db_path],
        capture_output=True,
    )
    if dump_proc.returncode != 0:
        return False, f"dump() failed: {dump_proc.stderr.decode('utf-8', 'replace')}", {}

    actual_state = {}
    for raw_line in dump_proc.stdout.splitlines():
        if not raw_line:
            continue
        if b"=" not in raw_line:
            return False, f"Malformed dump line: {raw_line!r}", {}
        k, v = raw_line.split(b"=", 1)
        actual_state[k] = v

    # Because two writer threads submit operations concurrently through a shared stdin lock,
    #iter_ops is in the exact sequential order written to the child process pipe.
    # All ACKed ops MUST be applied. In-flight (un-ACKed) ops at the tail right before SIGKILL
    # may have completed fsync before stdout flushed ACK (Scenario A4).
    # Thus, the valid state on disk must match base_state + prefix of iter_ops that includes ALL acked_set!
    last_acked_idx = -1
    for idx, op_id in enumerate(iter_ops):
        if op_id in acked_set:
            last_acked_idx = idx

    # Compute state up to last_acked_idx
    candidate = dict(base_state)
    for idx in range(last_acked_idx + 1):
        op_id = iter_ops[idx]
        # Note: if an op before last_acked_idx failed due to DiskFullError, it wouldn't be in acked_set
        if op_id in acked_set:
            apply_ops_to_dict(candidate, ops_by_id[op_id]["ops"])

    if actual_state == candidate:
        return True, "OK", actual_state

    # Check if any in-flight operations sent right after last_acked_idx finished fsync before kill (Scenario A4)
    temp_candidate = dict(candidate)
    for idx in range(last_acked_idx + 1, len(iter_ops)):
        op_id = iter_ops[idx]
        apply_ops_to_dict(temp_candidate, ops_by_id[op_id]["ops"])
        if actual_state == temp_candidate:
            return True, "OK (in-flight A4 durable)", actual_state

    # If still not matched, diagnose the exact first mismatch with op_id!
    for k in sorted(set(candidate.keys()) | set(actual_state.keys())):
        exp_v = candidate.get(k)
        act_v = actual_state.get(k)
        if exp_v != act_v:
            # Find the last op_id that touched key k
            touching_ops = [
                oid for oid in iter_ops
                if any(sub_k == k for _, sub_k, _ in ops_by_id[oid]["ops"])
            ]
            return (
                False,
                f"Invariant violation on key {k!r}: expected={exp_v!r}, found={act_v!r}, "
                f"last_acked_idx={last_acked_idx}, touching_ops={touching_ops[-5:]}",
                {},
            )

    return False, "State mismatch", {}


def run_torture(iterations: int, seed: int, mode: str, db_path: str):
    oracle_path = db_path + ".oracle.json"
    for p in (db_path, db_path + ".compact.tmp", oracle_path):
        if os.path.exists(p):
            os.remove(p)

    oracle = OracleTracker(oracle_path)
    key_pool = [f"key_{i:03d}".encode("ascii") for i in range(100)]
    current_truth = {}
    passed = 0

    max_bytes_arg = []
    if mode == "disk-full":
        max_bytes_arg = ["--max-bytes", "65536"]  # 64 KB cap to trigger Scenario E1-E5 repeatedly

    print(f"Starting torture tool: iterations={iterations}, seed={seed}, mode={mode}")

    for it in range(1, iterations + 1):
        # Deterministic per-iteration RNG seeded from master seed + iteration
        iter_rng = random.Random(seed * 1000003 + it)

        cmd = [sys.executable, "kvdb", "serve", db_path] + max_bytes_arg
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        iter_ops = []
        iter_acked = set()
        pipe_lock = threading.Lock()
        stop_writers = threading.Event()

        # Reader thread that records ACKs from child stdout
        def stdout_reader():
            try:
                for raw in proc.stdout:
                    line = raw.decode("utf-8", errors="replace").strip()
                    if line.startswith("ACK "):
                        op_id = line.split(" ", 1)[1].strip()
                        oracle.record_ack(op_id)
                        with pipe_lock:
                            iter_acked.add(op_id)
            except Exception:
                pass

        reader_t = threading.Thread(target=stdout_reader, daemon=True)
        reader_t.start()

        # At least two concurrent writer threads (Deliverable 2 requirement)
        def writer_worker(worker_id: int):
            w_rng = random.Random(seed * 99991 + it * 31 + worker_id)
            while not stop_writers.is_set() and proc.poll() is None:
                kind, ops_list = generate_workload_op(w_rng, mode, key_pool)
                with pipe_lock:
                    if stop_writers.is_set() or proc.poll() is not None:
                        break
                    op_id = oracle.register_op(kind, ops_list)
                    payload = encode_op_line(op_id, kind, ops_list)
                    try:
                        proc.stdin.write(payload)
                        proc.stdin.flush()
                        iter_ops.append(op_id)
                    except OSError:
                        break
                # Tiny jitter so threads interleave
                if mode in ("large-values", "large-batches"):
                    time.sleep(0.002)
                else:
                    time.sleep(0.0005)

        w1 = threading.Thread(target=writer_worker, args=(1,), daemon=True)
        w2 = threading.Thread(target=writer_worker, args=(2,), daemon=True)
        w1.start()
        w2.start()

        # Genuine random delay in milliseconds before SIGKILL / TerminateProcess
        if mode in ("large-values", "large-batches"):
            delay_sec = iter_rng.uniform(0.015, 0.080)
        else:
            delay_sec = iter_rng.uniform(0.005, 0.035)
        time.sleep(delay_sec)

        # Kill child process abruptly while writers are active!
        hard_kill(proc)
        
        # Explicitly close pipe streams to prevent Windows handle finalization warnings
        try:
            if proc.stdin:
                proc.stdin.close()
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
        except OSError:
            pass

        stop_writers.set()
        w1.join(timeout=1.0)
        w2.join(timeout=1.0)
        reader_t.join(timeout=1.0)

        # Special Mode: recovery-crash (Scenario C1, C2, F2)
        # Start 'kvdb open' and kill it mid-recovery, then let verification run open again!
        if mode == "recovery-crash":
            rec_proc = subprocess.Popen(
                [sys.executable, "kvdb", "open", db_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            time.sleep(iter_rng.uniform(0.001, 0.010))
            hard_kill(rec_proc)

        oracle.save_oracle_log()

        ok, reason, new_truth = verify_database_state(
            db_path, current_truth, iter_ops, iter_acked, oracle.ops_by_id
        )

        if not ok:
            print(f"[Iter {it}/{iterations}] FAIL: {reason}")
            print(f"Summary: {passed}/{it} passed before failure (seed={seed}, mode={mode})")
            sys.exit(1)

        current_truth = new_truth
        passed += 1
        print(
            f"[Iter {it}/{iterations}] PASS | sent={len(iter_ops)} acked={len(iter_acked)} "
            f"live_keys={len(current_truth)} delay_ms={delay_sec*1000:.1f}"
        )

    # Cleanup test files on full pass
    for p in (db_path, db_path + ".compact.tmp", oracle_path):
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

    print(f"FINAL RESULT: {passed}/{iterations} PASSED (0 failures) | seed={seed} mode={mode}")


def main():
    parser = argparse.ArgumentParser(description="Torture tool for KVDB")
    parser.add_argument("--iterations", type=int, default=500, help="Number of kill/restart loops")
    parser.add_argument("--seed", type=int, required=True, help="Random seed for reproducibility")
    parser.add_argument(
        "--mode",
        type=str,
        default="default",
        choices=["default", "large-values", "large-batches", "compaction", "recovery-crash", "disk-full"],
        help="Targeted torture scenario mode",
    )
    parser.add_argument("--db", type=str, default="torture_test.db", help="Database file path")
    args = parser.parse_args()

    run_torture(args.iterations, args.seed, args.mode, args.db)


if __name__ == "__main__":
    main()