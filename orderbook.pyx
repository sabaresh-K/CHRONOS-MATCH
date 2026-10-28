# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# cython: initializedcheck=False
"""
ChronosMatch — Day 2 Deliverable: Native Cython FIFO Matching Core & GIL Release (orderbook.pyx)
Operates on pre-allocated static C-structs in 'with nogil:' blocks with zero GC interference.
Reads directly from raw shared memory pointers and implements strict Price-Time Priority (FIFO).
"""

from libc.stdint cimport uint64_t, uint32_t, int32_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
cimport cython

import time
from typing import List, Tuple, Optional, Dict

# ---------------------------------------------------------------------------
# Static C-Level Structs for Memory Arena and Zero-Allocation Matching
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
    int next_idx
    int prev_idx
    bint active

cdef struct CPriceLevel:
    int32_t price
    uint32_t total_qty
    int head_idx
    int tail_idx
    int count

# Fixed capacities for static pools to eliminate malloc on hot path
DEF MAX_ORDERS = 500000
DEF MAX_LEVELS = 2048
DEF MAX_TRADES_BATCH = 4096

cdef class OrderBook:
    """
    Ultra-Low Latency Cython Limit Order Book with strict Price-Time Priority (FIFO).
    Executes inside 'with nogil:' blocks reading directly from memory-mapped binary buffers.
    """
    # C-level arena memory pools
    cdef COrderNode* node_pool
    cdef int free_head_idx
    cdef int allocated_count

    # Sorted price levels:
    # Bids sorted DESCENDING by price (bids[0] is best bid)
    # Asks sorted ASCENDING by price (asks[0] is best ask)
    cdef CPriceLevel* bid_levels
    cdef int num_bid_levels
    cdef CPriceLevel* ask_levels
    cdef int num_ask_levels

    # Trade output buffer
    cdef CTrade* trade_buffer
    cdef int trade_buffer_count

    # Performance counters & metrics
    cdef readonly uint64_t trade_counter
    cdef readonly uint64_t orders_processed
    cdef readonly uint64_t trades_executed
    cdef readonly uint64_t total_volume
    cdef readonly uint64_t total_match_latency_ns
    cdef readonly uint64_t min_latency_ns
    cdef readonly uint64_t max_latency_ns

    # Python-level history list
    cdef list executed_trades_history
    cdef list latency_samples

    def __cinit__(self):
        cdef int i
        # Allocate contiguous node arena
        self.node_pool = <COrderNode*>malloc(sizeof(COrderNode) * MAX_ORDERS)
        if not self.node_pool:
            raise MemoryError("Failed to allocate COrderNode memory arena.")

        # Build initial free list
        for i in range(MAX_ORDERS - 1):
            self.node_pool[i].next_idx = i + 1
            self.node_pool[i].prev_idx = -1
            self.node_pool[i].active = False
        self.node_pool[MAX_ORDERS - 1].next_idx = -1
        self.node_pool[MAX_ORDERS - 1].prev_idx = -1
        self.node_pool[MAX_ORDERS - 1].active = False
        self.free_head_idx = 0
        self.allocated_count = 0

        # Allocate price level arrays
        self.bid_levels = <CPriceLevel*>malloc(sizeof(CPriceLevel) * MAX_LEVELS)
        self.ask_levels = <CPriceLevel*>malloc(sizeof(CPriceLevel) * MAX_LEVELS)
        self.num_bid_levels = 0
        self.num_ask_levels = 0

        # Allocate trade buffer
        self.trade_buffer = <CTrade*>malloc(sizeof(CTrade) * MAX_TRADES_BATCH)
        self.trade_buffer_count = 0

        # Metrics
        self.trade_counter = 0
        self.orders_processed = 0
        self.trades_executed = 0
        self.total_volume = 0
        self.total_match_latency_ns = 0
        self.min_latency_ns = <uint64_t>(1ULL << 62)
        self.max_latency_ns = 0

        self.executed_trades_history = []
        self.latency_samples = []

    def __dealloc__(self):
        if self.node_pool:
            free(self.node_pool)
            self.node_pool = NULL
        if self.bid_levels:
            free(self.bid_levels)
            self.bid_levels = NULL
        if self.ask_levels:
            free(self.ask_levels)
            self.ask_levels = NULL
        if self.trade_buffer:
            free(self.trade_buffer)
            self.trade_buffer = NULL

    # -----------------------------------------------------------------------
    # C-Level Internal Helpers (No Python Overhead)
    # -----------------------------------------------------------------------
    cdef inline int _alloc_node(self) noexcept nogil:
        if self.free_head_idx == -1:
            return -1
        cdef int idx = self.free_head_idx
        self.free_head_idx = self.node_pool[idx].next_idx
        self.node_pool[idx].next_idx = -1
        self.node_pool[idx].prev_idx = -1
        self.node_pool[idx].active = True
        self.allocated_count += 1
        return idx

    cdef inline void _free_node(self, int idx) noexcept nogil:
        if idx < 0 or idx >= MAX_ORDERS:
            return
        self.node_pool[idx].active = False
        self.node_pool[idx].next_idx = self.free_head_idx
        self.node_pool[idx].prev_idx = -1
        self.free_head_idx = idx
        self.allocated_count -= 1

    # -----------------------------------------------------------------------
    # Pure C-Level Matching Core (strictly inside with nogil:)
    # -----------------------------------------------------------------------
    cdef int _match_single_order_nogil(
        self,
        uint64_t order_id,
        uint8_t side,
        int32_t price,
        uint32_t qty,
        uint64_t timestamp_ns,
        CTrade* trades_out,
        int max_trades
    ) noexcept nogil:
        """
        Executes order matching inside nogil with strict Price-Time Priority (FIFO).
        Returns count of trades generated.
        """
        cdef int trade_count = 0
        cdef uint32_t remaining_qty = qty
        cdef uint32_t trade_qty = 0
        cdef int level_idx, node_idx, next_node, i, ins_idx
        cdef CPriceLevel* level
        cdef COrderNode* maker

        # Side 3: Cancel Order
        if side == 3:
            self._cancel_order_nogil(order_id)
            return 0

        # Side 1: Buy Order (crosses against Ask levels sorted ascending by price)
        if side == 1:
            while self.num_ask_levels > 0 and self.ask_levels[0].price <= price and remaining_qty > 0:
                level = &self.ask_levels[0]
                node_idx = level.head_idx

                while node_idx != -1 and remaining_qty > 0:
                    maker = &self.node_pool[node_idx]
                    trade_qty = remaining_qty if remaining_qty < maker.qty else maker.qty

                    # Record Trade
                    if trade_count < max_trades:
                        self.trade_counter += 1
                        trades_out[trade_count].trade_id = self.trade_counter
                        trades_out[trade_count].maker_order_id = maker.order_id
                        trades_out[trade_count].taker_order_id = order_id
                        trades_out[trade_count].price = level.price  # Maker price priority
                        trades_out[trade_count].qty = trade_qty
                        trades_out[trade_count].timestamp_ns = timestamp_ns
                        trade_count += 1

                    self.trades_executed += 1
                    self.total_volume += trade_qty
                    remaining_qty -= trade_qty
                    maker.qty -= trade_qty
                    level.total_qty -= trade_qty

                    # If maker order fully filled, pop from FIFO head
                    if maker.qty == 0:
                        next_node = maker.next_idx
                        level.head_idx = next_node
                        if next_node != -1:
                            self.node_pool[next_node].prev_idx = -1
                        else:
                            level.tail_idx = -1
                        level.count -= 1
                        self._free_node(node_idx)
                        node_idx = next_node
                    else:
                        break  # Maker retains priority at head with remaining volume

                # If price level fully exhausted, pop level
                if level.head_idx == -1:
                    for i in range(self.num_ask_levels - 1):
                        self.ask_levels[i] = self.ask_levels[i + 1]
                    self.num_ask_levels -= 1

            # Insert resting Buy into Bid book (FIFO tail of matching price level)
            if remaining_qty > 0:
                self._insert_bid_nogil(order_id, price, remaining_qty, timestamp_ns)

        # Side 2: Sell Order (crosses against Bid levels sorted descending by price)
        elif side == 2:
            while self.num_bid_levels > 0 and self.bid_levels[0].price >= price and remaining_qty > 0:
                level = &self.bid_levels[0]
                node_idx = level.head_idx

                while node_idx != -1 and remaining_qty > 0:
                    maker = &self.node_pool[node_idx]
                    trade_qty = remaining_qty if remaining_qty < maker.qty else maker.qty

                    # Record Trade
                    if trade_count < max_trades:
                        self.trade_counter += 1
                        trades_out[trade_count].trade_id = self.trade_counter
                        trades_out[trade_count].maker_order_id = maker.order_id
                        trades_out[trade_count].taker_order_id = order_id
                        trades_out[trade_count].price = level.price  # Maker price priority
                        trades_out[trade_count].qty = trade_qty
                        trades_out[trade_count].timestamp_ns = timestamp_ns
                        trade_count += 1

                    self.trades_executed += 1
                    self.total_volume += trade_qty
                    remaining_qty -= trade_qty
                    maker.qty -= trade_qty
                    level.total_qty -= trade_qty

                    if maker.qty == 0:
                        next_node = maker.next_idx
                        level.head_idx = next_node
                        if next_node != -1:
                            self.node_pool[next_node].prev_idx = -1
                        else:
                            level.tail_idx = -1
                        level.count -= 1
                        self._free_node(node_idx)
                        node_idx = next_node
                    else:
                        break

                if level.head_idx == -1:
                    for i in range(self.num_bid_levels - 1):
                        self.bid_levels[i] = self.bid_levels[i + 1]
                    self.num_bid_levels -= 1

            # Insert resting Sell into Ask book (FIFO tail of matching price level)
            if remaining_qty > 0:
                self._insert_ask_nogil(order_id, price, remaining_qty, timestamp_ns)

        return trade_count

    cdef void _insert_bid_nogil(self, uint64_t order_id, int32_t price, uint32_t qty, uint64_t timestamp_ns) noexcept nogil:
        """Insert resting Bid into descending price array (highest price first)."""
        cdef int node_idx = self._alloc_node()
        if node_idx == -1:
            return

        cdef COrderNode* n = &self.node_pool[node_idx]
        n.order_id = order_id
        n.side = 1
        n.price = price
        n.qty = qty
        n.timestamp_ns = timestamp_ns
        n.next_idx = -1
        n.prev_idx = -1

        cdef int i, pos = -1
        for i in range(self.num_bid_levels):
            if self.bid_levels[i].price == price:
                pos = i
                break
            elif self.bid_levels[i].price < price:
                pos = i
                break

        if pos != -1 and self.bid_levels[pos].price == price:
            # Append to tail of FIFO queue for time priority
            n.prev_idx = self.bid_levels[pos].tail_idx
            if self.bid_levels[pos].tail_idx != -1:
                self.node_pool[self.bid_levels[pos].tail_idx].next_idx = node_idx
            self.bid_levels[pos].tail_idx = node_idx
            if self.bid_levels[pos].head_idx == -1:
                self.bid_levels[pos].head_idx = node_idx
            self.bid_levels[pos].total_qty += qty
            self.bid_levels[pos].count += 1
        else:
            if self.num_bid_levels >= MAX_LEVELS:
                self._free_node(node_idx)
                return
            if pos == -1:
                pos = self.num_bid_levels
            for i in range(self.num_bid_levels, pos, -1):
                self.bid_levels[i] = self.bid_levels[i - 1]
            self.bid_levels[pos].price = price
            self.bid_levels[pos].total_qty = qty
            self.bid_levels[pos].head_idx = node_idx
            self.bid_levels[pos].tail_idx = node_idx
            self.bid_levels[pos].count = 1
            self.num_bid_levels += 1

    cdef void _insert_ask_nogil(self, uint64_t order_id, int32_t price, uint32_t qty, uint64_t timestamp_ns) noexcept nogil:
        """Insert resting Ask into ascending price array (lowest price first)."""
        cdef int node_idx = self._alloc_node()
        if node_idx == -1:
            return

        cdef COrderNode* n = &self.node_pool[node_idx]
        n.order_id = order_id
        n.side = 2
        n.price = price
        n.qty = qty
        n.timestamp_ns = timestamp_ns
        n.next_idx = -1
        n.prev_idx = -1

        cdef int i, pos = -1
        for i in range(self.num_ask_levels):
            if self.ask_levels[i].price == price:
                pos = i
                break
            elif self.ask_levels[i].price > price:
                pos = i
                break

        if pos != -1 and self.ask_levels[pos].price == price:
            # Append to tail of FIFO queue for time priority
            n.prev_idx = self.ask_levels[pos].tail_idx
            if self.ask_levels[pos].tail_idx != -1:
                self.node_pool[self.ask_levels[pos].tail_idx].next_idx = node_idx
            self.ask_levels[pos].tail_idx = node_idx
            if self.ask_levels[pos].head_idx == -1:
                self.ask_levels[pos].head_idx = node_idx
            self.ask_levels[pos].total_qty += qty
            self.ask_levels[pos].count += 1
        else:
            if self.num_ask_levels >= MAX_LEVELS:
                self._free_node(node_idx)
                return
            if pos == -1:
                pos = self.num_ask_levels
            for i in range(self.num_ask_levels, pos, -1):
                self.ask_levels[i] = self.ask_levels[i - 1]
            self.ask_levels[pos].price = price
            self.ask_levels[pos].total_qty = qty
            self.ask_levels[pos].head_idx = node_idx
            self.ask_levels[pos].tail_idx = node_idx
            self.ask_levels[pos].count = 1
            self.num_ask_levels += 1

    cdef bint _cancel_order_nogil(self, uint64_t order_id) noexcept nogil:
        """Find and remove order node across active price levels."""
        cdef int i, j, node_idx, prev_idx, next_idx
        # Check Bids
        for i in range(self.num_bid_levels):
            node_idx = self.bid_levels[i].head_idx
            while node_idx != -1:
                if self.node_pool[node_idx].order_id == order_id:
                    prev_idx = self.node_pool[node_idx].prev_idx
                    next_idx = self.node_pool[node_idx].next_idx
                    if prev_idx != -1:
                        self.node_pool[prev_idx].next_idx = next_idx
                    else:
                        self.bid_levels[i].head_idx = next_idx
                    if next_idx != -1:
                        self.node_pool[next_idx].prev_idx = prev_idx
                    else:
                        self.bid_levels[i].tail_idx = prev_idx
                    self.bid_levels[i].total_qty -= self.node_pool[node_idx].qty
                    self.bid_levels[i].count -= 1
                    self._free_node(node_idx)
                    if self.bid_levels[i].head_idx == -1:
                        for j in range(i, self.num_bid_levels - 1):
                            self.bid_levels[j] = self.bid_levels[j + 1]
                        self.num_bid_levels -= 1
                    return True
                node_idx = self.node_pool[node_idx].next_idx

        # Check Asks
        for i in range(self.num_ask_levels):
            node_idx = self.ask_levels[i].head_idx
            while node_idx != -1:
                if self.node_pool[node_idx].order_id == order_id:
                    prev_idx = self.node_pool[node_idx].prev_idx
                    next_idx = self.node_pool[node_idx].next_idx
                    if prev_idx != -1:
                        self.node_pool[prev_idx].next_idx = next_idx
                    else:
                        self.ask_levels[i].head_idx = next_idx
                    if next_idx != -1:
                        self.node_pool[next_idx].prev_idx = prev_idx
                    else:
                        self.ask_levels[i].tail_idx = prev_idx
                    self.ask_levels[i].total_qty -= self.node_pool[node_idx].qty
                    self.ask_levels[i].count -= 1
                    self._free_node(node_idx)
                    if self.ask_levels[i].head_idx == -1:
                        for j in range(i, self.num_ask_levels - 1):
                            self.ask_levels[j] = self.ask_levels[j + 1]
                        self.num_ask_levels -= 1
                    return True
                node_idx = self.node_pool[node_idx].next_idx

        return False

    # -----------------------------------------------------------------------
    # Public Python APIs with GIL Release
    # -----------------------------------------------------------------------
    cpdef list process_order(
        self,
        uint64_t order_id,
        uint8_t side,
        int32_t price,
        uint32_t qty,
        object timestamp_ns=None
    ):
        """
        Process single order packet, releasing GIL during core matching loop.
        """
        cdef uint64_t start_ns = time.perf_counter_ns()
        cdef uint64_t ts = time.time_ns() if timestamp_ns is None else <uint64_t>timestamp_ns
        cdef int n_trades = 0
        cdef list executed = []
        cdef int i

        self.orders_processed += 1

        # Release GIL during C-struct matching
        with nogil:
            n_trades = self._match_single_order_nogil(
                order_id,
                side,
                price,
                qty,
                ts,
                self.trade_buffer,
                MAX_TRADES_BATCH
            )

        # Convert generated trades to Python objects if any
        for i in range(n_trades):
            t_obj = {
                'trade_id': self.trade_buffer[i].trade_id,
                'maker_order_id': self.trade_buffer[i].maker_order_id,
                'taker_order_id': self.trade_buffer[i].taker_order_id,
                'price': self.trade_buffer[i].price,
                'qty': self.trade_buffer[i].qty,
                'timestamp_ns': self.trade_buffer[i].timestamp_ns,
            }
            executed.append(t_obj)
            self.executed_trades_history.append(t_obj)

        self._record_latency(start_ns)
        return executed

    cpdef int process_shm_batch(self, object shm_memoryview, int max_orders=1024):
        """
        Read 32-byte packets directly from shared memory pointers with nogil release!
        Zero heap copies: parses directly from memoryview buffer.
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
        cdef uint64_t start_ns = time.perf_counter_ns()

        with nogil:
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

                self._match_single_order_nogil(
                    order_id,
                    side,
                    price,
                    qty,
                    ts,
                    self.trade_buffer,
                    MAX_TRADES_BATCH
                )
                self.orders_processed += 1
                processed += 1

            # Update shared memory read sequence index
            read_idx_ptr[0] = r + processed

        self._record_latency(start_ns, processed)
        return processed

    cpdef bint cancel_order(self, uint64_t order_id):
        cdef bint res
        with nogil:
            res = self._cancel_order_nogil(order_id)
        return res

    cpdef object get_best_bid(self):
        if self.num_bid_levels > 0:
            return self.bid_levels[0].price
        return None

    cpdef object get_best_ask(self):
        if self.num_ask_levels > 0:
            return self.ask_levels[0].price
        return None

    cpdef object get_spread(self):
        if self.num_bid_levels > 0 and self.num_ask_levels > 0:
            return self.ask_levels[0].price - self.bid_levels[0].price
        return None

    cpdef object get_mid_price(self):
        if self.num_bid_levels > 0 and self.num_ask_levels > 0:
            return (self.ask_levels[0].price + self.bid_levels[0].price) / 2.0
        return None

    cpdef tuple get_depth(self, int levels=5):
        cdef list bid_depth = []
        cdef list ask_depth = []
        cdef int i, count

        count = levels if self.num_bid_levels >= levels else self.num_bid_levels
        for i in range(count):
            bid_depth.append((self.bid_levels[i].price, self.bid_levels[i].total_qty))

        count = levels if self.num_ask_levels >= levels else self.num_ask_levels
        for i in range(count):
            ask_depth.append((self.ask_levels[i].price, self.ask_levels[i].total_qty))

        return bid_depth, ask_depth

    cdef void _record_latency(self, uint64_t start_ns, int count=1):
        cdef uint64_t latency_ns = time.perf_counter_ns() - start_ns
        cdef uint64_t c = <uint64_t>count if count > 0 else 1ULL
        cdef double per_order_us = (latency_ns / <double>c) / 1000.0
        self.total_match_latency_ns += latency_ns
        if (latency_ns / c) < self.min_latency_ns:
            self.min_latency_ns = latency_ns / c
        if (latency_ns / c) > self.max_latency_ns:
            self.max_latency_ns = latency_ns / c

        if len(self.latency_samples) < 100000:
            self.latency_samples.append(per_order_us)

    cpdef dict get_latency_stats(self):
        if not self.latency_samples:
            return {"min_us": 0.0, "p50_us": 0.0, "p90_us": 0.0, "p95_us": 0.0, "p99_us": 0.0, "max_us": 0.0, "avg_us": 0.0}

        cdef list s = sorted(self.latency_samples)
        cdef int n = len(s)
        return {
            "min_us": s[0],
            "p50_us": s[<int>(0.50 * n)],
            "p90_us": s[<int>(0.90 * n)],
            "p95_us": s[<int>(0.95 * n)],
            "p99_us": s[<int>(0.99 * n)],
            "p99_9_us": s[min(<int>(0.999 * n), n - 1)],
            "max_us": s[n - 1],
            "avg_us": (self.total_match_latency_ns / max(1, self.orders_processed)) / 1000.0,
        }

    cpdef list drain_trades(self):
        cdef list t = self.executed_trades_history
        self.executed_trades_history = []
        return t

    cpdef void reset(self):
        cdef int i
        # Reset node pool
        for i in range(MAX_ORDERS - 1):
            self.node_pool[i].next_idx = i + 1
            self.node_pool[i].prev_idx = -1
            self.node_pool[i].active = False
        self.node_pool[MAX_ORDERS - 1].next_idx = -1
        self.node_pool[MAX_ORDERS - 1].prev_idx = -1
        self.node_pool[MAX_ORDERS - 1].active = False
        self.free_head_idx = 0
        self.allocated_count = 0

        self.num_bid_levels = 0
        self.num_ask_levels = 0
        self.trade_buffer_count = 0
        self.trade_counter = 0
        self.orders_processed = 0
        self.trades_executed = 0
        self.total_volume = 0
        self.total_match_latency_ns = 0
        self.min_latency_ns = <uint64_t>(1ULL << 62)
        self.max_latency_ns = 0
        self.executed_trades_history.clear()
        self.latency_samples.clear()
