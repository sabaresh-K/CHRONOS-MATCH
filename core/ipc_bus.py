"""
ChronosMatch Zero-Copy Inter-Process Communication (IPC) Bus
High-performance shared memory-mapped ring buffer executing fixed 32-byte binary protocol.
Zero serialization (no JSON, no pickle) on the data plane.
"""

import os
import mmap
import time
import struct
from typing import Optional, Tuple, List

# ---------------------------------------------------------------------------
# Binary Protocol Constants
# ---------------------------------------------------------------------------
# 32-Byte Binary Packet Specification:
# <   : Little-Endian standard size & alignment
# Q   : uint64 (8 bytes) - order_id
# B   : uint8  (1 byte)  - side (1=BUY, 2=SELL, 3=CANCEL)
# 3x  : 3 bytes padding   - 4-byte boundary alignment
# i   : int32  (4 bytes) - price (cents / fixed-point integer ticks)
# I   : uint32 (4 bytes) - quantity / volume
# Q   : uint64 (8 bytes) - timestamp_ns (hardware high-resolution clock)
# 4x  : 4 bytes padding   - 8-byte cache alignment padding
ORDER_STRUCT = struct.Struct("<QB3xiIQ4x")
ORDER_SIZE = ORDER_STRUCT.size  # Exactly 32 bytes

assert ORDER_SIZE == 32, f"ORDER_SIZE must be exactly 32 bytes, got {ORDER_SIZE}"

# Side Enumeration
SIDE_BUY: int = 1
SIDE_SELL: int = 2
SIDE_CANCEL: int = 3

# 64-Byte Cache-Line-Aligned Ring Buffer Header Specification:
# 8s  : 8 bytes - Magic identifier ("CHRONOS\0")
# Q   : uint64 (8 bytes) - Capacity (total packet slots)
# Q   : uint64 (8 bytes) - Write sequence index (head counter)
# Q   : uint64 (8 bytes) - Read sequence index (tail counter)
# Q   : uint64 (8 bytes) - Dropped packets counter
# 24x : 24 bytes padding  - Align header to exactly 64 bytes (L1 cache line)
HEADER_STRUCT = struct.Struct("<8sQQQQ24x")
HEADER_SIZE = HEADER_STRUCT.size  # Exactly 64 bytes

assert HEADER_SIZE == 64, f"HEADER_SIZE must be exactly 64 bytes, got {HEADER_SIZE}"

MAGIC_BYTES = b"CHRONOS\x00"

# Direct struct accessors for high-frequency index updates
UINT64_STRUCT = struct.Struct("<Q")
WRITE_IDX_OFFSET = 16
READ_IDX_OFFSET = 24
DROPPED_OFFSET = 32


class RingBuffer:
    """
    Zero-Copy Shared Memory Ring Buffer.
    
    Provides ultra-low-latency Single-Producer Single-Consumer (SPSC) IPC
    over an mmap-backed file with zero heap allocations on packet read/write.
    """

    def __init__(
        self,
        filepath: str = "chronos_ipc.shm",
        capacity: int = 65536,
        create: bool = False,
    ) -> None:
        """
        Initialize or attach to a shared memory ring buffer.
        
        :param filepath: Path to the backing shared memory file.
        :param capacity: Total order slots in the ring buffer (must be power of 2).
        :param create: If True, forces creation and zero-initialization of the buffer.
        """
        self.filepath = os.path.abspath(filepath)
        self.capacity = capacity
        
        # Verify capacity is a power of 2 for fast bitwise modulo
        if (capacity & (capacity - 1)) != 0 or capacity <= 0:
            raise ValueError(f"RingBuffer capacity must be a power of 2, got {capacity}")
        self.mask = capacity - 1

        self.total_size = HEADER_SIZE + (self.capacity * ORDER_SIZE)
        self._file_handle = None
        self._mmap: Optional[mmap.mmap] = None
        self._view: Optional[memoryview] = None

        if create or not os.path.exists(self.filepath):
            self._create_and_init_buffer()
        else:
            self._attach_existing_buffer()

    def _create_and_init_buffer(self) -> None:
        """Allocate backing file and write clean 64-byte header."""
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        self._file_handle = open(self.filepath, "w+b")
        self._file_handle.seek(self.total_size - 1)
        self._file_handle.write(b"\x00")
        self._file_handle.flush()

        self._mmap = mmap.mmap(
            self._file_handle.fileno(),
            self.total_size,
            access=mmap.ACCESS_WRITE,
        )
        self._view = memoryview(self._mmap)

        # Write pristine 64-byte header
        HEADER_STRUCT.pack_into(
            self._mmap,
            0,
            MAGIC_BYTES,
            self.capacity,
            0,  # write_idx
            0,  # read_idx
            0,  # dropped_count
        )

    def _attach_existing_buffer(self) -> None:
        """Map existing file and validate header signature."""
        self._file_handle = open(self.filepath, "r+b")
        file_size = os.path.getsize(self.filepath)
        
        if file_size < HEADER_SIZE:
            raise ValueError(f"IPC file '{self.filepath}' is too small ({file_size} bytes).")

        self._mmap = mmap.mmap(
            self._file_handle.fileno(),
            0,  # Map entire file
            access=mmap.ACCESS_WRITE,
        )
        self._view = memoryview(self._mmap)

        # Validate magic identifier
        magic, cap, _, _, _ = HEADER_STRUCT.unpack_from(self._mmap, 0)
        if magic != MAGIC_BYTES:
            raise ValueError(f"Corrupt IPC file: invalid magic header {magic!r}")

        self.capacity = cap
        self.mask = cap - 1
        self.total_size = HEADER_SIZE + (self.capacity * ORDER_SIZE)

    # -----------------------------------------------------------------------
    # Atomic-Style Fast Header Read/Write
    # -----------------------------------------------------------------------
    @property
    def write_idx(self) -> int:
        """Current monotonic producer write sequence index."""
        return UINT64_STRUCT.unpack_from(self._mmap, WRITE_IDX_OFFSET)[0]

    @write_idx.setter
    def write_idx(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, WRITE_IDX_OFFSET, val)

    @property
    def read_idx(self) -> int:
        """Current monotonic consumer read sequence index."""
        return UINT64_STRUCT.unpack_from(self._mmap, READ_IDX_OFFSET)[0]

    @read_idx.setter
    def read_idx(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, READ_IDX_OFFSET, val)

    @property
    def dropped_count(self) -> int:
        """Count of orders dropped due to buffer overflow."""
        return UINT64_STRUCT.unpack_from(self._mmap, DROPOFF_OFFSET := DROPPED_OFFSET)[0]

    @dropped_count.setter
    def dropped_count(self, val: int) -> None:
        UINT64_STRUCT.pack_into(self._mmap, DROPPED_OFFSET, val)

    # -----------------------------------------------------------------------
    # Buffer Metrics
    # -----------------------------------------------------------------------
    def is_empty(self) -> bool:
        """True if consumer has read all available packets."""
        return self.read_idx >= self.write_idx

    def is_full(self) -> bool:
        """True if unread packet count equals buffer capacity."""
        return (self.write_idx - self.read_idx) >= self.capacity

    def get_occupancy(self) -> int:
        """Number of unconsumed packets residing in the ring buffer."""
        w = self.write_idx
        r = self.read_idx
        return max(0, w - r)

    # -----------------------------------------------------------------------
    # Zero-Copy Fast Packet I/O
    # -----------------------------------------------------------------------
    def write_order(
        self,
        order_id: int,
        side: int,
        price: int,
        qty: int,
        timestamp_ns: Optional[int] = None,
    ) -> bool:
        """
        Zero-copy packet serialization directly into the memory-mapped buffer.
        
        :param order_id: 64-bit unique order ID.
        :param side: 8-bit side (1=BUY, 2=SELL, 3=CANCEL).
        :param price: 32-bit fixed point integer price in cents (e.g. 15025 for $150.25).
        :param qty: 32-bit unsigned integer quantity.
        :param timestamp_ns: Monotonic nanosecond timestamp (auto-generated if None).
        :return: True if written successfully; False if dropped due to buffer saturation.
        """
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        w = self.write_idx
        r = self.read_idx

        # Ring buffer overflow check
        if (w - r) >= self.capacity:
            self.dropped_count += 1
            return False

        # Fast bitwise slot index & byte offset computation
        slot = w & self.mask
        offset = HEADER_SIZE + (slot * ORDER_SIZE)

        # Zero-allocation in-place struct packing directly into memory map
        ORDER_STRUCT.pack_into(
            self._mmap,
            offset,
            order_id,
            side,
            price,
            qty,
            timestamp_ns,
        )

        # Increment producer write index
        self.write_idx = w + 1
        return True

    def read_order(self) -> Optional[Tuple[int, int, int, int, int]]:
        """
        Zero-copy packet deserialization from shared memory.
        
        :return: (order_id, side, price, qty, timestamp_ns) or None if buffer is empty.
        """
        w = self.write_idx
        r = self.read_idx

        if r >= w:
            return None

        slot = r & self.mask
        offset = HEADER_SIZE + (slot * ORDER_SIZE)

        # In-place struct unpack from memoryview
        packet = ORDER_STRUCT.unpack_from(self._mmap, offset)

        # Advance consumer read index
        self.read_idx = r + 1
        return packet  # (order_id, side, price, qty, timestamp_ns)

    def read_batch(self, max_batch_size: int = 1024, batch_size: Optional[int] = None) -> List[Tuple[int, int, int, int, int]]:
        """
        High-throughput batch retrieval of pending orders in a single pass.
        
        :param max_batch_size: Maximum packets to drain in this batch.
        :param batch_size: Alias for max_batch_size.
        :return: List of unpacked order tuples.
        """
        if batch_size is not None:
            max_batch_size = batch_size

        w = self.write_idx
        r = self.read_idx

        available = w - r
        if available <= 0:
            return []

        count = min(available, max_batch_size)
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

    def reset(self) -> None:
        """Reset sequence indices to zero."""
        self.write_idx = 0
        self.read_idx = 0
        self.dropped_count = 0

    def close(self) -> None:
        """Flush and release memory mapping and file descriptors."""
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
        """Close buffer and remove backing file from disk."""
        self.close()
        if os.path.exists(self.filepath):
            try:
                os.remove(self.filepath)
            except OSError:
                pass

    def __enter__(self) -> "RingBuffer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
