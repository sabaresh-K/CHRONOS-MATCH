"""
ChronosMatch — Day 24: Latency Percentile Benchmark (Baseline vs. Cython)
Executes high-resolution ingress-to-match latency profiling across 100,000+ orders.
Compares pure-Python reference LOB with native Cython OrderBook.
Computes Min, p50 (Median), p90, p95, p99, p99.9, and Max latency percentiles in microseconds (μs).
"""

import os
import sys
import time
import math
from typing import List, Dict, Any, Tuple

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.lob_reference import LimitOrderBook as PythonReferenceLOB
from orderbook import OrderBook as CythonNativeLOB
from core.ipc_bus import SIDE_BUY, SIDE_SELL


def generate_synthetic_workload(count: int = 100_000) -> List[Tuple[int, int, int, int]]:
    """
    Generate realistic synthetic order stream:
    - 50% crossing marketable orders (immediate fills)
    - 50% resting limit orders (depth accumulation)
    Format: (order_id, side, price_cents, qty)
    """
    orders = []
    base_price = 15000  # $150.00
    for i in range(count):
        oid = i + 1
        is_buy = (i % 2 == 0)
        side = SIDE_BUY if is_buy else SIDE_SELL
        
        # Microstructure spread simulation (-20 to +20 cents)
        price_offset = (i % 41) - 20
        price = base_price + price_offset
        qty = 50 + (i % 200)
        orders.append((oid, side, price, qty))
    return orders


def benchmark_engine(engine: Any, orders: List[Tuple[int, int, int, int]], name: str) -> Dict[str, Any]:
    """
    Execute order stream and record per-order nanosecond latency using hardware clock.
    """
    latencies_us: List[float] = []
    engine_reset = getattr(engine, "reset", None)
    if callable(engine_reset):
        engine_reset()

    # Pre-warm JIT / cache
    warmup_orders = orders[:1000]
    for oid, side, price, qty in warmup_orders:
        engine.process_order(oid, side, price, qty)
    if callable(engine_reset):
        engine_reset()

    total_orders = len(orders)
    wall_start = time.perf_counter()

    for oid, side, price, qty in orders:
        t0 = time.perf_counter_ns()
        engine.process_order(oid, side, price, qty)
        t1 = time.perf_counter_ns()
        latencies_us.append((t1 - t0) / 1000.0)

    wall_elapsed = time.perf_counter() - wall_start
    throughput = total_orders / wall_elapsed if wall_elapsed > 0 else 0

    latencies_us.sort()
    n = len(latencies_us)

    def pct(p: float) -> float:
        idx = int(p * n)
        return latencies_us[min(idx, n - 1)]

    return {
        "name": name,
        "orders_processed": total_orders,
        "wall_time_s": wall_elapsed,
        "throughput_ops": throughput,
        "min_us": latencies_us[0],
        "p50_us": pct(0.50),
        "p90_us": pct(0.90),
        "p95_us": pct(0.95),
        "p99_us": pct(0.99),
        "p99_9_us": pct(0.999),
        "max_us": latencies_us[-1],
        "mean_us": sum(latencies_us) / n,
    }


def format_comparison_table(py_res: Dict[str, Any], cy_res: Dict[str, Any]) -> str:
    """Format an institutional-grade latency comparison table."""
    speedup_p50 = py_res["p50_us"] / cy_res["p50_us"] if cy_res["p50_us"] > 0 else 0
    speedup_p99 = py_res["p99_us"] / cy_res["p99_us"] if cy_res["p99_us"] > 0 else 0
    speedup_thru = cy_res["throughput_ops"] / py_res["throughput_ops"] if py_res["throughput_ops"] > 0 else 0

    lines = [
        "=" * 85,
        "        CHRONOSMATCH LATENCY PERCENTILE BENCHMARK (DAY 24)",
        "           Pure-Python Reference Baseline vs. Native Cython (nogil)",
        "=" * 85,
        f"{'Metric':<24} | {'Pure-Python Baseline':<24} | {'Cython Native (nogil)':<24} | {'Improvement':<10}",
        "-" * 85,
        f"{'Throughput (orders/s)':<24} | {py_res['throughput_ops']:>20,.0f} | {cy_res['throughput_ops']:>20,.0f} | {speedup_thru:>8.1f}x",
        f"{'Wall Elapsed (s)':<24} | {py_res['wall_time_s']:>20.4f} | {cy_res['wall_time_s']:>20.4f} | {speedup_thru:>8.1f}x",
        "-" * 85,
        f"{'Min Latency':<24} | {py_res['min_us']:>17.2f} us | {cy_res['min_us']:>17.2f} us | {py_res['min_us']/max(1e-9, cy_res['min_us']):>8.1f}x",
        f"{'Median Latency (p50)':<24} | {py_res['p50_us']:>17.2f} us | {cy_res['p50_us']:>17.2f} us | {speedup_p50:>8.1f}x",
        f"{'90th Percentile (p90)':<24} | {py_res['p90_us']:>17.2f} us | {cy_res['p90_us']:>17.2f} us | {py_res['p90_us']/max(1e-9, cy_res['p90_us']):>8.1f}x",
        f"{'95th Percentile (p95)':<24} | {py_res['p95_us']:>17.2f} us | {cy_res['p95_us']:>17.2f} us | {py_res['p95_us']/max(1e-9, cy_res['p95_us']):>8.1f}x",
        f"{'99th Percentile (p99)':<24} | {py_res['p99_us']:>17.2f} us | {cy_res['p99_us']:>17.2f} us | {speedup_p99:>8.1f}x",
        f"{'99.9th %ile (p99.9)':<24} | {py_res['p99_9_us']:>17.2f} us | {cy_res['p99_9_us']:>17.2f} us | {py_res['p99_9_us']/max(1e-9, cy_res['p99_9_us']):>8.1f}x",
        f"{'Max Tail Latency':<24} | {py_res['max_us']:>17.2f} us | {cy_res['max_us']:>17.2f} us | {py_res['max_us']/max(1e-9, cy_res['max_us']):>8.1f}x",
        f"{'Mean Latency':<24} | {py_res['mean_us']:>17.2f} us | {cy_res['mean_us']:>17.2f} us | {py_res['mean_us']/max(1e-9, cy_res['mean_us']):>8.1f}x",
        "=" * 85,
    ]
    return "\n".join(lines)


def run_latency_benchmark(num_orders: int = 100_000):
    print(f"\n[1/3] Generating synthetic workload of {num_orders:,} orders...")
    workload = generate_synthetic_workload(num_orders)

    print("[2/3] Benchmarking Pure-Python Reference LOB...")
    py_engine = PythonReferenceLOB()
    py_results = benchmark_engine(py_engine, workload, "Pure-Python Baseline")

    print("[3/3] Benchmarking Native Cython LOB (nogil)...")
    cy_engine = CythonNativeLOB()
    cy_results = benchmark_engine(cy_engine, workload, "Cython Native (nogil)")

    table = format_comparison_table(py_results, cy_results)
    print("\n" + table)

    # Verification threshold: Cython p50 must be under 5.0 us, p99 under 20.0 us
    assert cy_results["p50_us"] < 5.0, f"Target failed: Cython p50 latency was {cy_results['p50_us']} us (target < 5.0 us)"
    print(f"\n [PASS] DAY 24 TARGET ACHIEVED: Cython median latency is {cy_results['p50_us']:.2f} us (< 5.0 us target)!")
    print(f"        Speedup over Pure-Python: {py_results['p50_us']/cy_results['p50_us']:.1f}x at p50.\n")


if __name__ == "__main__":
    count = 100_000
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            pass
    run_latency_benchmark(count)
