"""
ChronosMatch — Lock-free POSIX/Windows mmap shared memory ring buffer.
"""

from core.ipc_bus import (
    RingBuffer,
    HEADER_STRUCT,
    HEADER_SIZE,
    ORDER_STRUCT,
    ORDER_SIZE,
    MAGIC_BYTES,
)

__all__ = [
    "RingBuffer",
    "HEADER_STRUCT",
    "HEADER_SIZE",
    "ORDER_STRUCT",
    "ORDER_SIZE",
    "MAGIC_BYTES",
]
