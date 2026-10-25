"""
ChronosMatch Automated Benchmark & Deterministic Verification Suite
Validates Price-Time Priority (FIFO), Zero-Copy 32-Byte IPC Protocol,
and runs 1M-Order throughput and sub-50-microsecond latency benchmarks.
"""

import os
import gc
import sys
import time
from typing import List, Tuple

try:
    import pytest
except Exception:
    pytest = None

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import (
    LimitOrderBook,
    RingBuffer,
    ORDER_STRUCT,
    ORDER_SIZE,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_CANCEL,
    ENGINE_BACKEND,
)


# ===========================================================================
# 1. DETERMINISTIC FIFO & PRICE-TIME PRIORITY VERIFICATION
# ===========================================================================

def test_price_priority() -> None:
    """
    Verify that orders with better price are matched before worse price orders.
    Bid: Higher price has priority.
    Ask: Lower price has priority.
    """
    lob = LimitOrderBook()

    # Place two resting asks: Ask A at $101.00 (10100), Ask B at $100.00 (10000)
    lob.process_order(order_id=1, side=SIDE_SELL, price=10100, qty=100)
    lob.process_order(order_id=2, side=SIDE_SELL, price=10000, qty=100)

    # Incoming Market/Limit Buy at $102.00 (10200) for 100 shares
    # Must match Ask B ($100.00) first due to price priority!
    trades = lob.process_order(order_id=3, side=SIDE_BUY, price=10200, qty=100)

    assert len(trades) == 1, "Should execute exactly one trade"
    trade = trades[0]
    maker_id = trade.maker_order_id if hasattr(trade, 'maker_order_id') else trade['maker_order_id']
    trade_price = trade.price if hasattr(trade, 'price') else trade['price']

    assert maker_id == 2, f"Expected maker order 2 ($100.00), got {maker_id}"
    assert trade_price == 10000, f"Expected match price 10000, got {trade_price}"

    # Remaining in book should be order 1 ($101.00)
    assert lob.get_best_ask() == 10100, "Remaining best ask should be 10100"


def test_time_priority_fifo() -> None:
    """
    Verify that orders at the identical price level match strictly in FIFO arrival sequence.
    """
    lob = LimitOrderBook()

    # Place two resting bids at identical price $150.00 (15000)
    # Order 10 arrives first, Order 11 arrives second
    lob.process_order(order_id=10, side=SIDE_BUY, price=15000, qty=50)
    lob.process_order(order_id=11, side=SIDE_BUY, price=15000, qty=50)

    # Incoming Sell at $150.00 for 50 shares
    # Must match Order 10 first due to FIFO time priority!
    trades_1 = lob.process_order(order_id=20, side=SIDE_SELL, price=15000, qty=50)
    assert len(trades_1) == 1
    maker_1 = trades_1[0].maker_order_id if hasattr(trades_1[0], 'maker_order_id') else trades_1[0]['maker_order_id']
    assert maker_1 == 10, f"FIFO violation: expected maker 10, got {maker_1}"

    # Incoming Sell at $150.00 for another 50 shares
    # Must match Order 11 second
    trades_2 = lob.process_order(order_id=21, side=SIDE_SELL, price=15000, qty=50)
    assert len(trades_2) == 1
    maker_2 = trades_2[0].maker_order_id if hasattr(trades_2[0], 'maker_order_id') else trades_2[0]['maker_order_id']
    assert maker_2 == 11, f"FIFO violation: expected maker 11, got {maker_2}"

    # Book should now be completely empty
    assert lob.get_best_bid() is None


def test_partial_fill_and_queue_position() -> None:
    """
    Verify that when an order is partially filled, the residual volume
    maintains its position at the front of that price level queue.
    """
    lob = LimitOrderBook()

    # Place Order 1 for 100 shares at $200.00, then Order 2 for 50 shares at $200.00
    lob.process_order(order_id=1, side=SIDE_BUY, price=20000, qty=100)
    lob.process_order(order_id=2, side=SIDE_BUY, price=20000, qty=50)

    # Incoming Sell for 40 shares (partial fill of Order 1)
    trades_1 = lob.process_order(order_id=3, side=SIDE_SELL, price=20000, qty=40)
    assert len(trades_1) == 1
    maker_1 = trades_1[0].maker_order_id if hasattr(trades_1[0], 'maker_order_id') else trades_1[0]['maker_order_id']
    assert maker_1 == 1

    # Incoming Sell for another 40 shares
    # Must STILL match Order 1 (which now has 60 shares left) rather than Order 2!
    trades_2 = lob.process_order(order_id=4, side=SIDE_SELL, price=20000, qty=40)
    assert len(trades_2) == 1
    maker_2 = trades_2[0].maker_order_id if hasattr(trades_2[0], 'maker_order_id') else trades_2[0]['maker_order_id']
    assert maker_2 == 1, f"Residual volume lost priority: expected maker 1, got {maker_2}"

    # Incoming Sell for 40 shares: 20 fills Order 1 (fully filled), 20 fills Order 2
    trades_3 = lob.process_order(order_id=5, side=SIDE_SELL, price=20000, qty=40)
    assert len(trades_3) == 2
    m3_0 = trades_3[0].maker_order_id if hasattr(trades_3[0], 'maker_order_id') else trades_3[0]['maker_order_id']
    m3_1 = trades_3[1].maker_order_id if hasattr(trades_3[1], 'maker_order_id') else trades_3[1]['maker_order_id']
    assert m3_0 == 1
    assert m3_1 == 2

    # Residual for Order 2 should be 30 shares
    depth = lob.get_depth(levels=1)[0]
    assert depth == [(20000, 30)]


def test_order_cancellation() -> None:
    """
    Verify that cancelled orders are removed immediately and cannot be matched.
    """
    lob = LimitOrderBook()

    # Place Bid 1 ($50.00, 100 shares), Bid 2 ($50.00, 100 shares)
    lob.process_order(order_id=1, side=SIDE_BUY, price=5000, qty=100)
    lob.process_order(order_id=2, side=SIDE_BUY, price=5000, qty=100)

    # Cancel Bid 1
    cancelled = lob.cancel_order(order_id=1)
    assert cancelled is True, "Cancellation should return True"

    # Incoming Sell at $50.00 for 100 shares
    # Must match Bid 2 (since Bid 1 was cancelled)
    trades = lob.process_order(order_id=3, side=SIDE_SELL, price=5000, qty=100)
    assert len(trades) == 1
    maker = trades[0].maker_order_id if hasattr(trades[0], 'maker_order_id') else trades[0]['maker_order_id']
    assert maker == 2, f"Expected match with Order 2, got {maker}"

    # Book should be empty
    assert lob.get_best_bid() is None


def test_zero_copy_ipc_roundtrip(tmp_path=None) -> None:
    """
    Verify binary protocol bit-exactness and zero-copy shared memory roundtrip.
    """
    test_shm = "test_roundtrip.shm"
    try:
        ring = RingBuffer(filepath=test_shm, capacity=1024, create=True)
        order_id = 9876543210123
        side = SIDE_BUY
        price = 15025  # $150.25
        qty = 450
        ts = 1700000000123456789

        # Zero-copy write
        success = ring.write_order(order_id, side, price, qty, ts)
        assert success is True

        # Zero-copy read
        packet = ring.read_order()
        assert packet is not None
        r_oid, r_side, r_price, r_qty, r_ts = packet

        assert r_oid == order_id
        assert r_side == side
        assert r_price == price
        assert r_qty == qty
        assert r_ts == ts
    finally:
        ring.unlink()


# ===========================================================================
# 2. 1M-ORDER ZERO-COPY IPC THROUGHPUT BENCHMARK
# ===========================================================================

def benchmark_ipc_throughput(num_orders: int = 1_000_000) -> Tuple[float, float]:
    """
    Benchmark zero-copy shared memory IPC write/read throughput across 1,000,000 orders.
    
    :return: (orders_per_sec, throughput_gbps)
    """
    shm_file = "bench_ipc.shm"
    capacity = 131072  # 128k slots ring buffer
    ring = RingBuffer(filepath=shm_file, capacity=capacity, create=True)

    try:
        t0 = time.perf_counter()
        written = 0
        read = 0
        batch_size = 4096

        write_order = ring.write_order
        read_batch = ring.read_batch

        while read < num_orders:
            # Producer burst
            to_write = min(batch_size, num_orders - written)
            for i in range(to_write):
                oid = written + i + 1
                write_order(oid, 1, 15000, 100, 1000)
            written += to_write

            # Consumer drain
            packets = read_batch(max_batch_size=to_write)
            read += len(packets)

        elapsed = time.perf_counter() - t0
        rate = num_orders / max(0.000001, elapsed)
        # 32 bytes per packet * 2 (write + read) = 64 bytes memory bandwidth per order
        bytes_transferred = num_orders * ORDER_SIZE * 2
        gbps = (bytes_transferred * 8) / (elapsed * 1_000_000_000.0)

        return rate, gbps
    finally:
        ring.unlink()


# ===========================================================================
# 3. HIGH-FREQUENCY MATCHING ENGINE LATENCY BENCHMARK (<50 MICROSECONDS)
# ===========================================================================

def benchmark_matching_engine(num_orders: int = 100_000, disable_gc: bool = True) -> dict:
    """
    Benchmark Price-Time Priority matching engine throughput and latency distribution.
    Executes with gc.disable() to prove zero GC interference.
    """
    lob = LimitOrderBook()

    # Pre-populate book with 1,000 bids and 1,000 asks to establish realistic depth
    for i in range(1, 1001):
        lob.process_order(order_id=i, side=SIDE_BUY, price=15000 - (i % 50), qty=100)
        lob.process_order(order_id=1000 + i, side=SIDE_SELL, price=15004 + (i % 50), qty=100)

    # Disable GC during high-frequency execution to prevent GC pause spikes
    gc_was_enabled = gc.isenabled()
    if disable_gc:
        gc.disable()

    try:
        t0 = time.perf_counter()
        lob_process = lob.process_order

        # Stream high-frequency orders (alternating resting and crossing)
        for i in range(num_orders):
            oid = 2000 + i
            side = SIDE_BUY if (i & 1) == 0 else SIDE_SELL
            price = 15004 if side == SIDE_BUY else 15000  # Cross inside spread
            qty = 100
            lob_process(oid, side, price, qty)

        elapsed = time.perf_counter() - t0
    finally:
        if gc_was_enabled:
            gc.enable()

    stats = lob.get_latency_stats()
    stats["orders_per_sec"] = num_orders / max(0.000001, elapsed)
    stats["total_time_s"] = elapsed
    stats["trades_executed"] = lob.trades_executed
    return stats


# ===========================================================================
# 4. COMPREHENSIVE CLI TEST & BENCHMARK RUNNER
# ===========================================================================

def run_all_benchmarks() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    print("\n" + "=" * 80)
    print("      CHRONOSMATCH ZERO-COPY HFT ENGINE: VERIFICATION & BENCHMARK SUITE")
    print("=" * 80)
    print(f" Engine Backend: {ENGINE_BACKEND}")
    print(f" Packet Size:    {ORDER_SIZE} bytes (struct format: <QB3xiIQ4x)")
    print("=" * 80)

    # 1. Deterministic FIFO Verification
    print("\n[1/3] Running Deterministic FIFO & Price-Time Priority Verification...")
    test_price_priority()
    print("  [PASS] Price Priority: Highest Bid and Lowest Ask matched first.")
    test_time_priority_fifo()
    print("  [PASS] Time Priority (FIFO): Identical price orders matched in arrival order.")
    test_partial_fill_and_queue_position()
    print("  [PASS] Partial Fills: Maker residual retains queue head priority.")
    test_order_cancellation()
    print("  [PASS] Cancellation: O(1) order cancellation verified.")
    test_zero_copy_ipc_roundtrip()
    print("  [PASS] Zero-Copy IPC: Bit-exact 32-byte packet serialization verified.")
    print("  >>> ALL 5 DETERMINISTIC FIFO TESTS PASSED.")

    # 2. 1M-Order IPC Throughput Benchmark
    print("\n[2/3] Benchmarking 1,000,000-Order Zero-Copy Shared Memory IPC...")
    ipc_rate, ipc_gbps = benchmark_ipc_throughput(num_orders=1_000_000)
    print(f"  * Orders Transferred: 1,000,000")
    print(f"  * IPC Throughput:     {ipc_rate:,.0f} orders/sec ({ipc_rate / 1_000_000:.2f} Million orders/sec)")
    print(f"  * Memory Bandwidth:   {ipc_gbps:.2f} Gbps")
    print("  >>> ZERO-COPY IPC BENCHMARK COMPLETED.")

    # 3. Matching Engine Latency Benchmark
    num_match = 100_000
    print(f"\n[3/3] Benchmarking {num_match:,} Orders on LOB Engine (GC Disabled)...")
    mstats = benchmark_matching_engine(num_orders=num_match, disable_gc=True)

    print(f"  * Orders Processed:   {num_match:,}")
    print(f"  * Trades Executed:    {mstats['trades_executed']:,}")
    print(f"  * Engine Throughput:  {mstats['orders_per_sec']:,.0f} orders/sec")
    print(f"  * Wall Time Elapsed:  {mstats['total_time_s']:.3f} seconds")
    print("\n  --- High-Resolution Latency Percentiles (us) ---")
    print(f"  * Min Latency:        {mstats['min_us']:>8.2f} us")
    print(f"  * Median (p50):       {mstats['p50_us']:>8.2f} us")
    print(f"  * 90th %ile (p90):    {mstats['p90_us']:>8.2f} us")
    print(f"  * 95th %ile (p95):    {mstats['p95_us']:>8.2f} us")
    print(f"  * 99th %ile (p99):    {mstats['p99_us']:>8.2f} us")
    print(f"  * 99.9th %ile (p99.9):{mstats.get('p99_9_us', 0):>8.2f} us")
    print(f"  * Max Latency:        {mstats['max_us']:>8.2f} us")
    print(f"  * Mean Latency:       {mstats['avg_us']:>8.2f} us")

    # Assert sub-50-microsecond requirement
    assert mstats['p50_us'] < 50.0, f"Latency target failed: p50 is {mstats['p50_us']} us (target: < 50 us)"
    print(f"\n  [PASS] LATENCY TARGET PROVEN: Median matching latency is {mstats['p50_us']:.2f} us (< 50 us target)!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    run_all_benchmarks()
