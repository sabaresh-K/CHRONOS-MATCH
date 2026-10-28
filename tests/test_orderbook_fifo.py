"""
Unit tests for Day 2: Native Cython OrderBook (FIFO & GIL Release)
Verifies exact Price-Time Priority, crossing orders, partial fills, and direct SHM batch processing.
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from orderbook import OrderBook
from ipc_ringbuffer import IPCRingBuffer, SIDE_BUY, SIDE_SELL, SIDE_CANCEL


def test_crossing_orders_exact_price():
    """Verify crossing buy and sell orders at exact price levels."""
    ob = OrderBook()

    # Resting Sell: 100 shares @ $150.00 (15000)
    ob.process_order(order_id=1, side=SIDE_SELL, price=15000, qty=100)
    assert ob.get_best_ask() == 15000
    assert ob.get_best_bid() is None

    # Crossing Buy: 100 shares @ $150.00 (15000)
    trades = ob.process_order(order_id=2, side=SIDE_BUY, price=15000, qty=100)
    assert len(trades) == 1
    t = trades[0]
    assert t['maker_order_id'] == 1
    assert t['taker_order_id'] == 2
    assert t['price'] == 15000
    assert t['qty'] == 100

    # Book should be empty
    assert ob.get_best_ask() is None
    assert ob.get_best_bid() is None
    print("  [PASS] test_crossing_orders_exact_price")


def test_price_priority():
    """Verify better prices match first."""
    ob = OrderBook()

    # Two resting asks: order 1 @ $101.00, order 2 @ $100.00
    ob.process_order(order_id=1, side=SIDE_SELL, price=10100, qty=100)
    ob.process_order(order_id=2, side=SIDE_SELL, price=10000, qty=100)

    # Incoming Buy @ $102.00 for 100 shares -> must match order 2 ($100.00) first
    trades = ob.process_order(order_id=3, side=SIDE_BUY, price=10200, qty=100)
    assert len(trades) == 1
    assert trades[0]['maker_order_id'] == 2
    assert trades[0]['price'] == 10000

    # Remaining in book should be order 1 ($101.00)
    assert ob.get_best_ask() == 10100
    print("  [PASS] test_price_priority")


def test_time_priority_fifo():
    """Verify FIFO queue order for orders at identical prices."""
    ob = OrderBook()

    # Resting bids at $150.00: order 10 arrives first, order 11 arrives second
    ob.process_order(order_id=10, side=SIDE_BUY, price=15000, qty=50)
    ob.process_order(order_id=11, side=SIDE_BUY, price=15000, qty=50)

    # First sell for 50 shares -> must match order 10
    t1 = ob.process_order(order_id=20, side=SIDE_SELL, price=15000, qty=50)
    assert len(t1) == 1
    assert t1[0]['maker_order_id'] == 10

    # Second sell for 50 shares -> must match order 11
    t2 = ob.process_order(order_id=21, side=SIDE_SELL, price=15000, qty=50)
    assert len(t2) == 1
    assert t2[0]['maker_order_id'] == 11

    assert ob.get_best_bid() is None
    print("  [PASS] test_time_priority_fifo")


def test_partial_fill_queue_retention():
    """Verify residual maker volume retains priority at head of queue."""
    ob = OrderBook()

    # Order 1: 100 shares @ $150.00; Order 2: 50 shares @ $150.00
    ob.process_order(order_id=1, side=SIDE_BUY, price=15000, qty=100)
    ob.process_order(order_id=2, side=SIDE_BUY, price=15000, qty=50)

    # Taker sells 60 shares -> partially fills Order 1 (40 shares remain)
    t1 = ob.process_order(order_id=3, side=SIDE_SELL, price=15000, qty=60)
    assert len(t1) == 1
    assert t1[0]['maker_order_id'] == 1
    assert t1[0]['qty'] == 60

    # Next taker sells 40 shares -> must finish Order 1 before touching Order 2
    t2 = ob.process_order(order_id=4, side=SIDE_SELL, price=15000, qty=40)
    assert len(t2) == 1
    assert t2[0]['maker_order_id'] == 1
    assert t2[0]['qty'] == 40

    # Best bid depth should now be Order 2 with 50 shares
    bids, _ = ob.get_depth(levels=1)
    assert len(bids) == 1
    assert bids[0] == (15000, 50)
    print("  [PASS] test_partial_fill_queue_retention")


def test_order_cancellation():
    """Verify order cancellation."""
    ob = OrderBook()
    ob.process_order(order_id=99, side=SIDE_BUY, price=15000, qty=100)
    assert ob.get_best_bid() == 15000

    # Cancel order 99
    res = ob.cancel_order(99)
    assert res is True
    assert ob.get_best_bid() is None
    print("  [PASS] test_order_cancellation")


def test_direct_shm_batch_reading():
    """Verify direct zero-copy matching from raw memoryview buffer."""
    shm_path = "test_orderbook_shm.shm"
    rb = IPCRingBuffer(filepath=shm_path, capacity=1024, create=True)

    # Write 2 orders into shared memory: 1 resting ask, 1 crossing buy
    rb.write_order(order_id=101, side=SIDE_SELL, price=15050, qty=200)
    rb.write_order(order_id=102, side=SIDE_BUY, price=15050, qty=150)

    ob = OrderBook()
    view = memoryview(rb._mmap)
    processed = ob.process_shm_batch(view, max_orders=10)

    assert processed == 2
    assert ob.orders_processed == 2
    assert ob.trades_executed == 1
    assert ob.total_volume == 150
    # Remaining ask at 15050 should have 50 shares
    assert ob.get_best_ask() == 15050
    _, asks = ob.get_depth(levels=1)
    assert asks[0] == (15050, 50)

    rb.close()
    if os.path.exists(shm_path):
        try:
            os.remove(shm_path)
        except OSError:
            pass
    print("  [PASS] test_direct_shm_batch_reading")


if __name__ == "__main__":
    print("\n==================================================")
    print(" DAY 2: CYTHON FIFO MATCHING CORE & SHM TESTS")
    print("==================================================")
    test_crossing_orders_exact_price()
    test_price_priority()
    test_time_priority_fifo()
    test_partial_fill_queue_retention()
    test_order_cancellation()
    test_direct_shm_batch_reading()
    print("==================================================")
    print(" ALL DAY 2 CYTHON ORDERBOOK UNIT TESTS PASSED!")
    print("==================================================\n")
