"""
ChronosMatch Market Simulator
High-Throughput Asyncio Market Order Firehose (>100,000 orders/sec).
Generates realistic limit order flow, crossing market orders, and cancellations,
writing directly into the zero-copy shared memory IPC ring buffer.
"""

import os
import sys
import time
import random
import asyncio
import argparse
from typing import Optional, List

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.ipc_bus import RingBuffer, SIDE_BUY, SIDE_SELL, SIDE_CANCEL


class MarketSimulator:
    """
    High-Throughput Market Order Generator.
    
    Streams synthetic order flow into the ChronosMatch IPC bus
    mimicking realistic microstructural order dynamics (random walks,
    spread clustering, Poisson bursts).
    """

    def __init__(
        self,
        shm_path: str = "chronos_ipc.shm",
        capacity: int = 65536,
        base_price: int = 15000,  # $150.00 in fixed-point cents
        spread_ticks: int = 4,    # $0.04 initial spread
    ) -> None:
        self.shm_path = shm_path
        self.capacity = capacity
        self.base_price = base_price
        self.spread_ticks = spread_ticks
        self.ring_buffer: Optional[RingBuffer] = None

        # State tracking
        self.current_mid: int = base_price
        self.order_counter: int = 0
        self.active_order_ids: List[int] = []
        self.orders_emitted: int = 0
        self.orders_dropped: int = 0
        self.start_time: float = 0.0

    def start(self, create: bool = True) -> None:
        """Initialize connection to the IPC ring buffer."""
        self.ring_buffer = RingBuffer(
            filepath=self.shm_path,
            capacity=self.capacity,
            create=create,
        )
        self.start_time = time.perf_counter()

    def generate_single_order(self) -> None:
        """
        Generate a single order packet according to market microstructure probabilities:
        - 46% Limit Bid
        - 46% Limit Ask
        - 6% Aggressive Crossing Taker
        - 2% Order Cancellation
        """
        self.order_counter += 1
        oid = self.order_counter
        roll = random.random()
        now_ns = time.time_ns()

        # Random walk for mid price (mean-reverting around base_price)
        drift = 1 if random.random() > 0.5 else -1
        if abs(self.current_mid - self.base_price) > 200:
            drift = -1 if self.current_mid > self.base_price else 1
        self.current_mid += drift

        half_spread = max(1, self.spread_ticks // 2)

        # 2% Cancellation
        if roll < 0.02 and self.active_order_ids:
            idx = random.randrange(len(self.active_order_ids))
            cancel_id = self.active_order_ids.pop(idx)
            written = self.ring_buffer.write_order(
                order_id=cancel_id,
                side=SIDE_CANCEL,
                price=0,
                qty=0,
                timestamp_ns=now_ns,
            )
            if written:
                self.orders_emitted += 1
            else:
                self.orders_dropped += 1
            return

        # 6% Crossing Market/Taker Order
        if roll < 0.08:
            is_buy = random.random() < 0.5
            side = SIDE_BUY if is_buy else SIDE_SELL
            # Cross inside spread for immediate fill
            price = self.current_mid + half_spread if is_buy else self.current_mid - half_spread
            qty = random.randint(10, 100)
            written = self.ring_buffer.write_order(
                order_id=oid,
                side=side,
                price=price,
                qty=qty,
                timestamp_ns=now_ns,
            )
            if written:
                self.orders_emitted += 1
            else:
                self.orders_dropped += 1
            return

        # 92% Resting Limit Orders (Bids & Asks)
        is_buy = roll < 0.54
        side = SIDE_BUY if is_buy else SIDE_SELL
        qty = random.randint(10, 500)

        # Price tick depth: cluster close to spread
        tick_offset = random.choice([0, 1, 1, 2, 2, 3, 4, 5, 8, 10])
        if is_buy:
            price = self.current_mid - half_spread - tick_offset
        else:
            price = self.current_mid + half_spread + tick_offset

        written = self.ring_buffer.write_order(
            order_id=oid,
            side=side,
            price=price,
            qty=qty,
            timestamp_ns=now_ns,
        )

        if written:
            self.orders_emitted += 1
            # Keep track of active order IDs for cancellations
            if len(self.active_order_ids) < 5000:
                self.active_order_ids.append(oid)
            elif random.random() < 0.1:
                self.active_order_ids[random.randrange(len(self.active_order_ids))] = oid
        else:
            self.orders_dropped += 1

    def generate_batch(self, batch_size: int = 1000) -> int:
        """
        Generate and stream a tight burst of orders directly into shared memory.
        
        :param batch_size: Number of orders in this burst.
        :return: Count of orders successfully emitted.
        """
        initial_emitted = self.orders_emitted
        for _ in range(batch_size):
            self.generate_single_order()
        return self.orders_emitted - initial_emitted

    async def run_firehose_async(
        self,
        target_rate: int = 100_000,
        total_orders: int = 1_000_000,
        batch_size: int = 500,
    ) -> None:
        """
        Asyncio event loop firehose maintaining a target throughput rate.
        
        :param target_rate: Target orders per second.
        :param total_orders: Total orders to stream before terminating.
        :param batch_size: Orders per dispatch chunk.
        """
        if self.ring_buffer is None:
            self.start(create=True)

        interval = batch_size / float(target_rate)
        print(f"[MarketSimulator] Streaming {total_orders:,} orders at target {target_rate:,} orders/sec (chunk: {batch_size})...")

        t0 = time.perf_counter()
        while self.orders_emitted < total_orders:
            cycle_start = time.perf_counter()
            self.generate_batch(batch_size)

            elapsed = time.perf_counter() - cycle_start
            sleep_time = interval - elapsed
            if sleep_time > 0.0005:
                await asyncio.sleep(sleep_time)
            else:
                # Yield to asyncio loop
                await asyncio.sleep(0)

        total_elapsed = time.perf_counter() - t0
        rate = self.orders_emitted / max(0.001, total_elapsed)
        print(f"[MarketSimulator] Finished streaming: {self.orders_emitted:,} orders in {total_elapsed:.3f}s ({rate:,.0f} orders/sec). Dropped: {self.orders_dropped:,}")

    def run_firehose_sync(self, total_orders: int = 1_000_000, batch_size: int = 1000) -> float:
        """
        Synchronous raw-speed burst benchmark.
        
        :return: Sustained orders/sec throughput.
        """
        if self.ring_buffer is None:
            self.start(create=True)

        t0 = time.perf_counter()
        while self.orders_emitted < total_orders:
            remaining = total_orders - self.orders_emitted
            cur_batch = min(batch_size, remaining)
            self.generate_batch(cur_batch)

        elapsed = time.perf_counter() - t0
        rate = self.orders_emitted / max(0.000001, elapsed)
        return rate

    def close(self) -> None:
        """Close ring buffer handle."""
        if self.ring_buffer:
            self.ring_buffer.close()
            self.ring_buffer = None


def main() -> None:
    parser = argparse.ArgumentParser(description="ChronosMatch High-Throughput Market Simulator")
    parser.add_argument("--orders", type=int, default=500_000, help="Total orders to generate")
    parser.add_argument("--rate", type=int, default=100_000, help="Target orders/second")
    parser.add_argument("--shm", type=str, default="chronos_ipc.shm", help="Path to shared memory file")
    parser.add_argument("--burst", action="store_true", help="Execute in unthrottled maximum burst mode")
    args = parser.parse_args()

    sim = MarketSimulator(shm_path=args.shm)
    sim.start(create=True)

    try:
        if args.burst:
            print(f"[MarketSimulator] Running unthrottled burst of {args.orders:,} orders...")
            rate = sim.run_firehose_sync(total_orders=args.orders)
            print(f"[MarketSimulator] Completed: {sim.orders_emitted:,} orders written at {rate:,.0f} orders/sec.")
        else:
            asyncio.run(sim.run_firehose_async(target_rate=args.rate, total_orders=args.orders))
    finally:
        sim.close()


if __name__ == "__main__":
    main()
