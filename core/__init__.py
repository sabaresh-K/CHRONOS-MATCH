"""
ChronosMatch Core Package
Zero-Copy High-Frequency Trading Engine Core Primitives
"""

# Attempt to load the Cython compiled native Limit Order Book;
# gracefully and seamlessly fall back to pure-Python reference implementation.
try:
    from .lob_engine import LimitOrderBook  # type: ignore
    ENGINE_BACKEND = "Cython Native (nogil)"
except (ImportError, ModuleNotFoundError):
    from .lob_reference import LimitOrderBook
    ENGINE_BACKEND = "Python Reference (Fallback)"

from .ipc_bus import (
    RingBuffer,
    ORDER_STRUCT,
    ORDER_SIZE,
    HEADER_STRUCT,
    HEADER_SIZE,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_CANCEL,
    SIDE_IOC_BUY,
    SIDE_IOC_SELL,
    SIDE_NAMES,
)

__all__ = [
    "LimitOrderBook",
    "RingBuffer",
    "ORDER_STRUCT",
    "ORDER_SIZE",
    "HEADER_STRUCT",
    "HEADER_SIZE",
    "SIDE_BUY",
    "SIDE_SELL",
    "SIDE_CANCEL",
    "SIDE_IOC_BUY",
    "SIDE_IOC_SELL",
    "SIDE_NAMES",
    "ENGINE_BACKEND",
]
