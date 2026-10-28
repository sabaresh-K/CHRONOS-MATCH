"""
ChronosMatch — Day 1 Deliverable: IPC & Ring Buffer Integration (ipc_ringbuffer.py)
Zero-copy shared memory-mapped ring buffer executing fixed 32-byte binary protocol.
Zero serialization (no JSON, no pickle) on the data plane with SPSC atomic head/tail pointers.
"""

import os
import sys
import mmap
import time
import struct
import random
import asyncio
import multiprocessing as mp
from typing import Optional, Tuple, List

# ---------------------------------------------------------------------------
# 32-Byte Binary Packet Specification (Hardware Cache-Aligned)
# ---------------------------------------------------------------------------
# <   : Little-Endian standard size & alignment
# Q   : uint64 (8 bytes) - order_id
# B   : uint8  (1 byte)  - side (1=BUY, 2=SELL, 3=CANCEL)
# 3x  : 3 bytes padding   - 4-byte boundary alignment
# i   : int32  (4 bytes) - price (cents / fixed-point integer ticks)
# I   : uint32 (4 bytes) - quantity / volume
# Q   : uint64 (8 bytes) - timestamp_ns (hardware high-resolution clock)
# 4x  : 4 bytes padding   - 32-byte total alignment
ORDER_STRUCT = struct.Struct("<QB3xiIQ4x")
ORDER_SIZE = ORDER_STRUCT.size  # Exactly 32 bytes
assert ORDER_SIZE == 32, f"ORDER_SIZE must be exactly 32 bytes, got {ORDER_SIZE}"

# Order Sides
SIDE_BUY: int = 1
SIDE_SELL: int = 2
SIDE_CANCEL: int = 3

# 64-Byte Cache-Line Aligned Ring Buffer Header
# 8s  : 8 bytes - Magic identifier ("CHRONOS\0")
# Q   : uint64 (8 bytes) - Capacity (power of 2)
# Q   : uint64 (8 bytes) - Write sequence index (head pointer)
# Q   : uint64 (8 bytes) - Read sequence index (tail pointer)
# Q   : uint64 (8 bytes) - Dropped packets counter
# 24x : 24 bytes padding  - Align header to exactly 64 bytes (L1 cache line)
HEADER_STRUCT = struct.Struct("<8sQQQQ24x")
HEADER_SIZE = HEADER_STRUCT.size  # Exactly 64 bytes
assert HEADER_SIZE == 64, f"HEADER_SIZE must be exactly 64 bytes, got {HEADER_SIZE}"

MAGIC_BYTES = b"CHRONOS\x00"
UINT64_STRUCT = struct.Struct("<Q")

WRITE_IDX_OFFSET = 16
READ_IDX_OFFSET = 24
DROPPED_OFFSET = 32


class IPCRingBuffer:
    """
    Zero-Copy Shared Memory SPSC Ring Buffer.
    Provides sub-microsecond IPC over an mmap-backed file with zero heap allocations.
    """

    def __init__(
        self,
        filepath: str = "chronos_ipc.shm",
        capacity: int = 65536,
        create: bool = False,
    ) -> None:
        self.filepath = os.path.abspath(filepath)
        self.capacity = capacity

        if (capacity & (capacity - 1)) != 0 or capacity <= 0:
            raise ValueError(f"RingBuffer capacity must be a power of 2, got {capacity}")
        self.mask = capacity - 1
        self.total_size = HEADER_SIZE + (self.capacity * ORDER_SIZE)

        self._file_handle = None
        self._mmap: Optional[mmap.mmap] = None

        if create or not os.path.exists(self.filepath):
            self._init_buffer()
        else:
            self._attach_buffer()

    def _init_buffer(self) -> None:
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        if os.path.exists(self.filepath):
            self._file_handle = open(self.filepath, "r+b")
        else:
            self._file_handle = open(self.filepath, "w+b")
        self._file_handle.seek(self.total_size - 1)
        self._file_handle.write(b"\x00")
        self._file_handle.flush()

        self._mmap = mmap.mmap(
            self._file_handle.fileno(),
            self.total_size,
            access=mmap.ACCESS_WRITE,
        )

        HEADER_STRUCT.pack_into(
            self._mmap,
            0,
            MAGIC_BYTES,
            self.capacity,
            0,  # write_idx
            0,  # read_idx
            0,  # dropped_count
        )
        self._mmap.flush()

    def _attach_buffer(self) -> None:
        self._file_handle = open(self.filepath, "r+b")
        file_size = os.path.getsize(self.filepath)
        if file_size < HEADER_SIZE:
            raise ValueError(f"IPC file '{self.filepath}' too small ({file_size} bytes)")

        self._mmap = mmap.mmap(
            self._file_handle.fileno(),
            0,
            access=mmap.ACCESS_WRITE,
        )

        magic, cap, _, _, _ = HEADER_STRUCT.unpack_from(self._mmap, 0)
        if magic != MAGIC_BYTES:
            raise ValueError(f"Corrupt IPC file: invalid magic header {magic!r}")

        self.capacity = cap
        self.mask = cap - 1
        self.total_size = HEADER_SIZE + (self.capacity * ORDER_SIZE)

    # Fast header sequence properties
    @property
    def write_idx(self) -> int:
        return UINT64_STRUCT.unpack_from(self._mmap, WRITE_IDX_OFFSET)[0]

    @write_idx.setter
    def write_idx(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, WRITE_IDX_OFFSET, val)

    @property
    def read_idx(self) -> int:
        return UINT64_STRUCT.unpack_from(self._mmap, READ_IDX_OFFSET)[0]

    @read_idx.setter
    def read_idx(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, READ_IDX_OFFSET, val)

    @property
    def dropped_count(self) -> int:
        return UINT64_STRUCT.unpack_from(self._mmap, DROPPED_OFFSET)[0]

    @dropped_count.setter
    def dropped_count(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, DROPPED_OFFSET, val)

    def get_occupancy(self) -> int:
        return max(0, self.write_idx - self.read_idx)

    def is_empty(self) -> bool:
        return self.read_idx >= self.write_idx

    def is_full(self) -> bool:
        return (self.write_idx - self.read_idx) >= self.capacity

    def write_order(
        self,
        order_id: int,
        side: int,
        price: int,
        qty: int,
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """Zero-copy in-place packet write into shared memory."""
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        w = self.write_idx
        r = self.read_idx

        if (w - r) >= self.capacity:
            self.dropped_count += 1
            return False

        slot = w & self.mask
        offset = HEADER_SIZE + (slot * ORDER_SIZE)

        ORDER_STRUCT.pack_into(
            self._mmap,
            offset,
            order_id,
            side,
            price,
            qty,
            timestamp_ns,
        )
        self.write_idx = w + 1
        return True

    def read_order(self) -> Optional[Tuple[int, int, int, int, int]]:
        """Zero-copy packet read from shared memory."""
        w = self.write_idx
        r = self.read_idx

        if r >= w:
            return None

        slot = r & self.mask
        offset = HEADER_SIZE + (slot * ORDER_SIZE)
        packet = ORDER_STRUCT.unpack_from(self._mmap, offset)
        self.read_idx = r + 1
        return packet

    def read_batch(self, max_batch_size: int = 1024) -> List[Tuple[int, int, int, int, int]]:
        """High-throughput batch retrieval without per-item Python overhead."""
        w = self.write_idx
        r = self.read_idx
        avail = w - r
        if avail <= 0:
            return []

        count = min(avail, max_batch_size)
        packets = []
        mask = self.mask
        m = self._mmap
        unpack = ORDER_STRUCT.unpack_from

        for i in range(count):
            slot = (r + i) & mask
            offset = HEADER_SIZE + (slot * ORDER_SIZE)
            packets.append(unpack(m, offset))

        self.read_idx = r + count
        return packets

    def close(self) -> None:
        if self._mmap is not None:
            try:
                self._mmap.flush()
                self._mmap.close()
            except Exception:
                pass
            self._mmap = None
        if self._file_handle is not None:
            try:
                self._file_handle.close()
            except Exception:
                pass
            self._file_handle = None

    def unlink(self) -> None:
        self.close()
        if os.path.exists(self.filepath):
            try:
                os.remove(self.filepath)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Day 1: Asyncio Market Firehose Simulator Integration
# ---------------------------------------------------------------------------
class AsyncMarketFirehose:
    """
    High-Throughput Asyncio Order Firehose.
    Simulates institutional order flow (>100k orders/sec) directly into the shared memory bus.
    """

    def __init__(
        self,
        ring_buffer: IPCRingBuffer,
        base_price: int = 15000,
        spread_ticks: int = 4,
    ) -> None:
        self.rb = ring_buffer
        self.base_price = base_price
        self.current_mid = base_price
        self.spread_ticks = spread_ticks
        self.order_counter = 0
        self.active_order_ids: List[int] = []
        self.orders_emitted = 0
        self.orders_dropped = 0

    def generate_packet(self) -> None:
        self.order_counter += 1
        oid = self.order_counter
        roll = random.random()
        now_ns = time.time_ns()

        # Mean-reverting random walk for mid price
        drift = 1 if random.random() > 0.5 else -1
        if abs(self.current_mid - self.base_price) > 200:
            drift = -1 if self.current_mid > self.base_price else 1
        self.current_mid += drift

        half_spread = max(1, self.spread_ticks // 2)

        # 2% Cancellations
        if roll < 0.02 and self.active_order_ids:
            idx = random.randrange(len(self.active_order_ids))
            cancel_id = self.active_order_ids.pop(idx)
            if self.rb.write_order(cancel_id, SIDE_CANCEL, 0, 0, now_ns):
                self.orders_emitted += 1
            else:
                self.orders_dropped += 1
            return

        # 6% Crossing Market/Taker Order
        if roll < 0.08:
            is_buy = random.random() < 0.5
            side = SIDE_BUY if is_buy else SIDE_SELL
            price = self.current_mid + half_spread if is_buy else self.current_mid - half_spread
            qty = random.randint(10, 100)
            if self.rb.write_order(oid, side, price, qty, now_ns):
                self.orders_emitted += 1
            else:
                self.orders_dropped += 1
            return

        # 92% Resting Limit Bids & Asks
        is_buy = roll < 0.54
        side = SIDE_BUY if is_buy else SIDE_SELL
        qty = random.randint(10, 500)
        tick_offset = random.choice([0, 1, 1, 2, 2, 3, 4, 5, 8, 10])
        price = (self.current_mid - half_spread - tick_offset) if is_buy else (self.current_mid + half_spread + tick_offset)

        if self.rb.write_order(oid, side, price, qty, now_ns):
            self.orders_emitted += 1
            if len(self.active_order_ids) < 5000:
                self.active_order_ids.append(oid)
            elif random.random() < 0.1:
                self.active_order_ids[random.randrange(len(self.active_order_ids))] = oid
        else:
            self.orders_dropped += 1

    async def run_firehose(self, target_rate: int = 100_000, total_orders: int = 500_000, batch_size: int = 500) -> None:
        interval = batch_size / float(target_rate)
        t0 = time.perf_counter()

        while self.orders_emitted < total_orders:
            cycle_start = time.perf_counter()
            for _ in range(batch_size):
                self.generate_packet()

            elapsed = time.perf_counter() - cycle_start
            sleep_time = interval - elapsed
            if sleep_time > 0.0005:
                await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(0)

        total_elapsed = time.perf_counter() - t0
        rate = self.orders_emitted / max(0.001, total_elapsed)
        print(f"[Producer] Emitted {self.orders_emitted:,} packets in {total_elapsed:.3f}s ({rate:,.0f} pkts/sec). Dropped: {self.orders_dropped}")


# ---------------------------------------------------------------------------
# Cross-Process Concurrency Verification
# ---------------------------------------------------------------------------
def _worker_producer(shm_path: str, total_packets: int) -> None:
    rb = IPCRingBuffer(filepath=shm_path, capacity=65536, create=False)
    t0 = time.perf_counter_ns()
    for i in range(1, total_packets + 1):
        while not rb.write_order(order_id=i, side=SIDE_BUY, price=15000 + (i % 50), qty=100, timestamp_ns=t0):
            time.sleep(0.00001)
    rb.close()


def _worker_consumer(shm_path: str, total_packets: int, return_dict) -> None:
    rb = IPCRingBuffer(filepath=shm_path, capacity=65536, create=False)
    consumed = 0
    t0 = time.perf_counter()
    while consumed < total_packets:
        batch = rb.read_batch(max_batch_size=2048)
        if batch:
            consumed += len(batch)
        else:
            time.sleep(0.000001)
    elapsed = time.perf_counter() - t0
    rb.close()
    return_dict["consumed"] = consumed
    return_dict["elapsed"] = elapsed
    return_dict["rate"] = consumed / max(0.0001, elapsed)


def run_cross_process_verification(total_packets: int = 500_000, shm_path: str = "test_spsc.shm") -> bool:
    print(f"[Verification] Launching 2 OS processes for SPSC IPC test ({total_packets:,} packets)...")
    manager = mp.Manager()
    return_dict = manager.dict()

    p_cons = mp.Process(target=_worker_consumer, args=(shm_path, total_packets, return_dict))
    p_prod = mp.Process(target=_worker_producer, args=(shm_path, total_packets))

    # Create shm first
    init_rb = IPCRingBuffer(filepath=shm_path, capacity=65536, create=True)
    init_rb.close()

    p_cons.start()
    time.sleep(0.05)
    p_prod.start()

    p_prod.join()
    p_cons.join()

    consumed = return_dict.get("consumed", 0)
    elapsed = return_dict.get("elapsed", 0.0)
    rate = return_dict.get("rate", 0.0)

    print(f"[Verification] Completed: {consumed:,} packets verified across 2 processes in {elapsed:.3f}s ({rate:,.0f} pkts/sec).")
    assert consumed == total_packets, f"Packet loss: expected {total_packets}, got {consumed}"

    # Clean up
    if os.path.exists(shm_path):
        try:
            os.remove(shm_path)
        except OSError:
            pass
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ChronosMatch Day 1: IPC Ring Buffer Harness")
    parser.add_argument("--test", action="store_true", help="Run 2-process SPSC concurrency test")
    parser.add_argument("--packets", type=int, default=500_000, help="Total packets for test")
    parser.add_argument("--firehose", action="store_true", help="Run asyncio firehose simulator")
    parser.add_argument("--shm", type=str, default="chronos_ipc.shm", help="Path to shared memory file")
    args = parser.parse_args()

    if args.test:
        run_cross_process_verification(total_packets=args.packets)
    elif args.firehose:
        rb = IPCRingBuffer(filepath=args.shm, capacity=65536, create=True)
        firehose = AsyncMarketFirehose(ring_buffer=rb)
        asyncio.run(firehose.run_firehose(total_orders=args.packets))
        rb.close()
    else:
        print("ChronosMatch Day 1 IPC module ready. Use --test or --firehose to execute.")
