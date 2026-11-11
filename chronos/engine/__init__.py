"""ChronosMatch Engine Subpackage"""
try:
    from orderbook import OrderBook  # Native Cython
    ENGINE_BACKEND = "Cython Native (nogil)"
except Exception:
    from .baseline_book import BaselineOrderBook as OrderBook
    ENGINE_BACKEND = "Pure-Python Baseline"

from .baseline_book import BaselineOrderBook

__all__ = ["OrderBook", "BaselineOrderBook", "ENGINE_BACKEND"]
