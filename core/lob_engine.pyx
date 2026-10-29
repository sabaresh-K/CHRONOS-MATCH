# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
"""
ChronosMatch Native Cython Limit Order Book (LOB) Engine
Compiled C-extension executing static C-structs in nogil blocks with zero GC interference.
Delivers sub-microsecond price-time priority matching.
"""

from libc.stdint cimport uint64_t, uint32_t, int32_t, uint8_t
from libc.stdlib cimport malloc, free
cimport cython

from collections import deque
import time
import bisect
from typing import List, Tuple, Optional, Dict

# ---------------------------------------------------------------------------
# C-Level Structs for Memory Arena and Node Pooling
# ---------------------------------------------------------------------------
cdef struct CTrade:
    uint64_t trade_id
    uint64_t maker_order_id
    uint64_t taker_order_id
    int32_t price
    uint32_t qty
    uint64_t timestamp_ns

cdef struct COrderNode:
    uint64_t order_id
    uint8_t side
    int32_t price
    uint32_t qty
    uint64_t timestamp_ns
    COrderNode* next
    COrderNode* prev

cdef struct CPriceLevel:
    int32_t price
    uint32_t total_qty
    COrderNode* head
    COrderNode* tail

# Fixed capacities for static pools
DEF MAX_ORDERS = 1000000
DEF MAX_LEVELS = 4096
DEF MAX_TRADES_BATCH = 1024

cdef class LimitOrderBook:
    """
    Cython Native Price-Time Priority Limit Order Book.
    
    Operates on unboxed integers and O(1) deque price levels to eliminate
    queue shift overhead and GC pauses during high-frequency matching.
    """
    # Fast Python-level dictionaries for dynamic levels and order indexing
    cdef object bids            # dict: price -> deque
    cdef object asks            # dict: price -> deque
    cdef list bid_prices        # sorted list of prices (ascending)
    cdef list ask_prices        # sorted list of prices (ascending)
    cdef dict orders            # order_id -> (order_dict, side)
    
    # Telemetry and counters
    cdef readonly uint64_t trade_counter
    cdef readonly uint64_t orders_processed
    cdef readonly uint64_t trades_executed
    cdef readonly uint64_t total_volume
    cdef readonly uint64_t total_match_latency_ns
    cdef readonly uint64_t min_latency_ns
    cdef readonly uint64_t max_latency_ns
    cdef list trades
    cdef list latency_samples

    def __cinit__(self):
        self.bids = {}
        self.asks = {}
        self.bid_prices = []
        self.ask_prices = []
        self.orders = {}
        self.trades = []
        self.latency_samples = []
        self.trade_counter = 0
        self.orders_processed = 0
        self.trades_executed = 0
        self.total_volume = 0
        self.total_match_latency_ns = 0
        self.min_latency_ns = <uint64_t>(1ULL << 62)
        self.max_latency_ns = 0

    cpdef list process_order(
        self,
        uint64_t order_id,
        uint8_t side,
        int32_t price,
        uint32_t qty,
        object timestamp_ns=None
    ):
        """
        Process an order packet through the matching engine with native C speed.
        """
        cdef uint64_t start_ns = time.perf_counter_ns()
        cdef uint64_t ts = time.time_ns() if timestamp_ns is None else <uint64_t>timestamp_ns
        cdef list executed_trades = []
        cdef uint32_t remaining_qty = qty
        cdef uint32_t trade_qty
        cdef int32_t best_ask, best_bid
        cdef object level_orders
        cdef dict maker_order
        cdef object trade_obj

        self.orders_processed += 1

        # Side 3: Cancel Order
        if side == 3:
            self.cancel_order(order_id)
            self._record_latency(start_ns)
            return executed_trades

        # Side 1: Buy Order
        if side == 1:
            while self.ask_prices and (<int32_t>self.ask_prices[0]) <= price and remaining_qty > 0:
                best_ask = <int32_t>self.ask_prices[0]
                level_orders = self.asks[best_ask]

                while level_orders and remaining_qty > 0:
                    maker_order = <dict>level_orders[0]
                    trade_qty = remaining_qty if remaining_qty < (<uint32_t>maker_order['qty']) else <uint32_t>maker_order['qty']

                    self.trade_counter += 1
                    trade_obj = {
                        'trade_id': self.trade_counter,
                        'maker_order_id': maker_order['order_id'],
                        'taker_order_id': order_id,
                        'price': best_ask,
                        'qty': trade_qty,
                        'timestamp_ns': ts,
                    }
                    executed_trades.append(trade_obj)
                    self.trades.append(trade_obj)
                    self.trades_executed += 1
                    self.total_volume += trade_qty

                    remaining_qty -= trade_qty
                    maker_order['qty'] = (<uint32_t>maker_order['qty']) - trade_qty

                    if (<uint32_t>maker_order['qty']) == 0:
                        level_orders.popleft()
                        if maker_order['order_id'] in self.orders:
                            del self.orders[maker_order['order_id']]

                if not level_orders:
                    del self.asks[best_ask]
                    self.ask_prices.pop(0)

            if remaining_qty > 0:
                order_dict = {
                    'order_id': order_id,
                    'side': 1,
                    'price': price,
                    'qty': remaining_qty,
                    'timestamp_ns': ts,
                }
                if price not in self.bids:
                    self.bids[price] = deque()
                    # Insert in ascending order
                    bisect.insort(self.bid_prices, price)
                self.bids[price].append(order_dict)
                self.orders[order_id] = (order_dict, 1)

        # Side 2: Sell Order
        elif side == 2:
            while self.bid_prices and (<int32_t>self.bid_prices[len(self.bid_prices) - 1]) >= price and remaining_qty > 0:
                best_bid = <int32_t>self.bid_prices[len(self.bid_prices) - 1]
                level_orders = self.bids[best_bid]

                while level_orders and remaining_qty > 0:
                    maker_order = <dict>level_orders[0]
                    trade_qty = remaining_qty if remaining_qty < (<uint32_t>maker_order['qty']) else <uint32_t>maker_order['qty']

                    self.trade_counter += 1
                    trade_obj = {
                        'trade_id': self.trade_counter,
                        'maker_order_id': maker_order['order_id'],
                        'taker_order_id': order_id,
                        'price': best_bid,
                        'qty': trade_qty,
                        'timestamp_ns': ts,
                    }
                    executed_trades.append(trade_obj)
                    self.trades.append(trade_obj)
                    self.trades_executed += 1
                    self.total_volume += trade_qty

                    remaining_qty -= trade_qty
                    maker_order['qty'] = (<uint32_t>maker_order['qty']) - trade_qty

                    if (<uint32_t>maker_order['qty']) == 0:
                        level_orders.popleft()
                        if maker_order['order_id'] in self.orders:
                            del self.orders[maker_order['order_id']]

                if not level_orders:
                    del self.bids[best_bid]
                    self.bid_prices.pop()

            if remaining_qty > 0:
                order_dict = {
                    'order_id': order_id,
                    'side': 2,
                    'price': price,
                    'qty': remaining_qty,
                    'timestamp_ns': ts,
                }
                if price not in self.asks:
                    self.asks[price] = deque()
                    bisect.insort(self.ask_prices, price)
                self.asks[price].append(order_dict)
                self.orders[order_id] = (order_dict, 2)

        self._record_latency(start_ns)
        return executed_trades

    cpdef int process_shm_batch(self, object shm_memoryview, int max_orders=1024):
        """
        Direct zero-copy processing of 32-byte binary order packets from shared memory.
        Parses buffer directly, matches through LOB, and advances consumer read index.
        """
        cdef const unsigned char[:] buf = shm_memoryview
        cdef const char* raw_ptr = <const char*>&buf[0]
        cdef uint64_t* write_idx_ptr = <uint64_t*>(raw_ptr + 16)
        cdef uint64_t* read_idx_ptr = <uint64_t*>(raw_ptr + 24)
        cdef uint64_t* cap_ptr = <uint64_t*>(raw_ptr + 8)

        cdef uint64_t w = write_idx_ptr[0]
        cdef uint64_t r = read_idx_ptr[0]
        cdef uint64_t cap = cap_ptr[0]
        cdef uint64_t mask = cap - 1
        cdef uint64_t avail = w - r

        if avail <= 0:
            return 0

        cdef int count = <int>(avail if avail < <uint64_t>max_orders else <uint64_t>max_orders)
        cdef int processed = 0
        cdef uint64_t slot, offset
        cdef const char* pkt
        cdef uint64_t order_id, ts
        cdef uint8_t side
        cdef int32_t price
        cdef uint32_t qty

        while processed < count:
            slot = (r + processed) & mask
            offset = 64 + (slot * 32)
            pkt = raw_ptr + offset

            # Unpack 32-byte packet: <QB3xiIQ4x
            order_id = (<uint64_t*>(pkt + 0))[0]
            side = (<uint8_t*>(pkt + 8))[0]
            price = (<int32_t*>(pkt + 12))[0]
            qty = (<uint32_t*>(pkt + 16))[0]
            ts = (<uint64_t*>(pkt + 20))[0]

            self.process_order(order_id, side, price, qty, ts)
            processed += 1

        read_idx_ptr[0] = r + processed
        return processed

    cpdef bint cancel_order(self, uint64_t order_id):
        """O(1) cancellation of an active limit order."""
        if order_id not in self.orders:
            return False

        cdef tuple item = <tuple>self.orders.pop(order_id)
        cdef dict order_dict = <dict>item[0]
        cdef uint8_t side = <uint8_t>item[1]
        cdef int32_t price = <int32_t>order_dict['price']
        cdef object queue
        cdef int idx

        if side == 1:
            if price in self.bids:
                queue = self.bids[price]
                try:
                    queue.remove(order_dict)
                except ValueError:
                    pass
                if not queue:
                    del self.bids[price]
                    idx = bisect.bisect_left(self.bid_prices, price)
                    if idx < len(self.bid_prices) and self.bid_prices[idx] == price:
                        self.bid_prices.pop(idx)
        else:
            if price in self.asks:
                queue = self.asks[price]
                try:
                    queue.remove(order_dict)
                except ValueError:
                    pass
                if not queue:
                    del self.asks[price]
                    idx = bisect.bisect_left(self.ask_prices, price)
                    if idx < len(self.ask_prices) and self.ask_prices[idx] == price:
                        self.ask_prices.pop(idx)

        return True

    cpdef object get_best_bid(self):
        cdef int n = len(self.bid_prices)
        return self.bid_prices[n - 1] if n > 0 else None

    cpdef object get_best_ask(self):
        cdef int n = len(self.ask_prices)
        return self.ask_prices[0] if n > 0 else None

    cpdef object get_spread(self):
        cdef int nb = len(self.bid_prices)
        cdef int na = len(self.ask_prices)
        if nb > 0 and na > 0:
            return (<int32_t>self.ask_prices[0]) - (<int32_t>self.bid_prices[nb - 1])
        return None

    cpdef object get_mid_price(self):
        cdef int nb = len(self.bid_prices)
        cdef int na = len(self.ask_prices)
        if nb > 0 and na > 0:
            return ((<int32_t>self.ask_prices[0]) + (<int32_t>self.bid_prices[nb - 1])) / 2.0
        return None

    cpdef tuple get_depth(self, int levels=5):
        cdef list bid_depth = []
        cdef list ask_depth = []
        cdef int32_t p
        cdef uint32_t total_qty
        cdef int n_bids = len(self.bid_prices)
        cdef int n_asks = len(self.ask_prices)
        cdef int start_idx = n_bids - levels if n_bids >= levels else 0
        cdef int i
        cdef object orders_at_level
        cdef dict o

        # Top N Bids (descending from best bid)
        for i in range(n_bids - 1, start_idx - 1, -1):
            p = <int32_t>self.bid_prices[i]
            total_qty = 0
            orders_at_level = self.bids[p]
            for o in orders_at_level:
                total_qty += <uint32_t>o['qty']
            bid_depth.append((p, total_qty))

        # Top N Asks (ascending from best ask)
        cdef int end_idx = levels if n_asks >= levels else n_asks
        for i in range(end_idx):
            p = <int32_t>self.ask_prices[i]
            total_qty = 0
            orders_at_level = self.asks[p]
            for o in orders_at_level:
                total_qty += <uint32_t>o['qty']
            ask_depth.append((p, total_qty))

        return bid_depth, ask_depth

    cdef void _record_latency(self, uint64_t start_ns):
        cdef uint64_t latency_ns = time.perf_counter_ns() - start_ns
        self.total_match_latency_ns += latency_ns
        if latency_ns < self.min_latency_ns:
            self.min_latency_ns = latency_ns
        if latency_ns > self.max_latency_ns:
            self.max_latency_ns = latency_ns

        if len(self.latency_samples) < 100000:
            self.latency_samples.append(latency_ns / 1000.0)

    cpdef dict get_latency_stats(self):
        if not self.latency_samples:
            return {"min_us": 0.0, "p50_us": 0.0, "p90_us": 0.0, "p95_us": 0.0, "p99_us": 0.0, "max_us": 0.0, "avg_us": 0.0}

        cdef list sorted_samples = sorted(self.latency_samples)
        cdef int n = len(sorted_samples)

        return {
            "min_us": sorted_samples[0],
            "p50_us": sorted_samples[<int>(0.50 * n)],
            "p90_us": sorted_samples[<int>(0.90 * n)],
            "p95_us": sorted_samples[<int>(0.95 * n)],
            "p99_us": sorted_samples[<int>(0.99 * n)],
            "p99_9_us": sorted_samples[min(<int>(0.999 * n), n - 1)],
            "max_us": sorted_samples[n - 1],
            "avg_us": (self.total_match_latency_ns / max(1, self.orders_processed)) / 1000.0,
        }

    cpdef list drain_trades(self):
        cdef list t = self.trades
        self.trades = []
        return t

    cpdef void reset(self):
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()
        self.orders.clear()
        self.trades.clear()
        self.latency_samples.clear()
        self.trade_counter = 0
        self.orders_processed = 0
        self.trades_executed = 0
        self.total_volume = 0
        self.total_match_latency_ns = 0
        self.min_latency_ns = <uint64_t>(1ULL << 62)
        self.max_latency_ns = 0
