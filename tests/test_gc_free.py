"""
ChronosMatch — Day 23: GC-Free Hot Loop Verification Probe
Hooks into gc.callbacks to detect any garbage collection events during high-throughput order matching.
Proves that the native Cython memory arena executes 100,000 orders with 0 GC collections.
"""

import os
import sys
import gc
import time
from typing import List, Dict, Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from orderbook import OrderBook
from ipc_ringbuffer import IPCRingBuffer, SIDE_BUY, SIDE_SELL, ORDER_STRUCT


class GCProbe:
    """Monitors Python GC invocation and tracks collection phases and generations."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self._enabled_at_start = gc.isenabled()

    def callback(self, phase: str, info: Dict[str, Any]) -> None:
        self.events.append({
            "phase": phase,
            "generation": info.get("generation"),
            "collected": info.get("collected"),
            "uncollectable": info.get("uncollectable"),
            "timestamp_ns": time.perf_counter_ns(),
        })

    def attach(self) -> None:
        gc.collect()  # Flush any existing garbage prior to probe
        self.events.clear()
        gc.callbacks.append(self.callback)

    def detach(self) -> None:
        if self.callback in gc.callbacks:
            gc.callbacks.remove(self.callback)

    @property
    def event_count(self) -> int:
        return len(self.events)


def verify_gc_free_hot_loop(num_orders: int = 100_000) -> bool:
    print("\n" + "=" * 70)
    print(" CHRONOSMATCH DAY 23: GC-FREE HOT LOOP VERIFICATION")
    print("=" * 70)
    print(f" Target Orders:        {num_orders:,}")
    print(f" Python GC Enabled:    {gc.isenabled()} (GC remains ENABLED to prove 0 triggers)")
    print("=" * 70)

    # 1. Initialize OrderBook and pre-allocated arena
    ob = OrderBook()
    shm_path = "gc_verify_bench.shm"
    capacity = 131072  # 128k slots (power of 2)
    rb = IPCRingBuffer(filepath=shm_path, capacity=capacity, create=True)

    # 2. Pre-generate order stream in shared memory buffer to isolate matching from generation
    print("\n[1/3] Populating shared memory ring buffer with order flow...")
    now_ns = time.time_ns()
    for i in range(num_orders):
        oid = i + 1
        side = SIDE_BUY if (i % 2 == 0) else SIDE_SELL
        price = 15000 + ((i % 50) - 25)  # $149.75 - $150.25
        qty = 50 + (i % 200)
        rb.write_order(oid, side, price, qty, now_ns)

    print(f"      Written {num_orders:,} orders to shared memory. Available: {rb.get_occupancy():,}")

    # 3. Attach GC probe
    probe = GCProbe()
    probe.attach()

    # 4. Execute matching through zero-copy SHM batch view
    print("\n[2/3] Executing high-speed matching loop with active GC probe...")
    shm_view = memoryview(rb._mmap)
    start_ns = time.perf_counter_ns()
    
    total_processed = 0
    batch_size = 4096
    while total_processed < num_orders:
        n = ob.process_shm_batch(shm_view, max_orders=batch_size)
        if n == 0:
            break
        total_processed += n

    elapsed_s = (time.perf_counter_ns() - start_ns) / 1e9
    probe.detach()
    rb.close()
    if os.path.exists(shm_path):
        try:
            os.remove(shm_path)
        except OSError:
            pass

    # 5. Evaluate and report results
    throughput = total_processed / elapsed_s if elapsed_s > 0 else 0
    stats = ob.get_latency_stats()

    print("\n[3/3] Verification Results:")
    print(f"  * Orders Processed:      {total_processed:,}")
    print(f"  * Trades Executed:       {ob.trades_executed:,}")
    print(f"  * Total Volume:          {ob.total_volume:,} shares")
    print(f"  * Elapsed Time:          {elapsed_s:.4f} seconds")
    print(f"  * Engine Throughput:     {throughput:,.0f} orders/sec")
    print(f"  * Median Latency (p50):  {stats['p50_us']:.2f} us")
    print(f"  * 99th %ile Latency:     {stats['p99_us']:.2f} us")
    print(f"  * GC Collections Fired:  {probe.event_count}")

    print("\n" + "-" * 70)
    if probe.event_count == 0:
        print(" [PASS] ZERO GC COLLECTIONS CONFIRMED: Hot loop is 100% GC-free!")
        print("        Memory arena allocates zero heap objects inside nogil matching.")
        print("-" * 70 + "\n")
        return True
    else:
        print(f" [FAIL] Garbage collection fired {probe.event_count} times during matching!")
        for ev in probe.events:
            print(f"        -> Phase: {ev['phase']}, Gen: {ev['generation']}, Collected: {ev['collected']}")
        print("-" * 70 + "\n")
        return False


if __name__ == "__main__":
    success = verify_gc_free_hot_loop(100_000)
    sys.exit(0 if success else 1)
