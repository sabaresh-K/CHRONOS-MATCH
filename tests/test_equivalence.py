"""
Unit Tests: Baseline vs. Cython Limit Order Book Equivalence
Ensures Cython Native engine outputs identical executions and states to the reference baseline.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from chronos.engine.baseline_book import BaselineOrderBook
from orderbook import OrderBook as CythonOrderBook
from chronos.ipc.order_record import SIDE_BUY, SIDE_SELL, SIDE_CANCEL


def test_matching_equivalence_simple_crossing():
    py_lob = BaselineOrderBook()
    cy_lob = CythonOrderBook()

    # Resting Sell: 100 @ 150.00
    py_lob.process_order(1, SIDE_SELL, 15000, 100)
    cy_lob.process_order(1, SIDE_SELL, 15000, 100)

    # Crossing Buy: 100 @ 150.00
    t_py = py_lob.process_order(2, SIDE_BUY, 15000, 100)
    t_cy = cy_lob.process_order(2, SIDE_BUY, 15000, 100)

    assert len(t_py) == len(t_cy) == 1
    assert t_py[0].price == t_cy[0]["price"] == 15000
    assert t_py[0].qty == t_cy[0]["qty"] == 100
    assert py_lob.get_best_bid() == cy_lob.get_best_bid() is None
    assert py_lob.get_best_ask() == cy_lob.get_best_ask() is None


def test_matching_equivalence_partial_fills_and_queues():
    py_lob = BaselineOrderBook()
    cy_lob = CythonOrderBook()

    # Add 2 resting sells: 50 @ 150.00 and 100 @ 150.00
    py_lob.process_order(10, SIDE_SELL, 15000, 50)
    py_lob.process_order(11, SIDE_SELL, 15000, 100)

    cy_lob.process_order(10, SIDE_SELL, 15000, 50)
    cy_lob.process_order(11, SIDE_SELL, 15000, 100)

    # Crossing aggressive buy for 80 shares
    t_py = py_lob.process_order(12, SIDE_BUY, 15000, 80)
    t_cy = cy_lob.process_order(12, SIDE_BUY, 15000, 80)

    assert len(t_py) == len(t_cy) == 2
    # First trade: 50 from maker 10
    assert t_py[0].qty == t_cy[0]["qty"] == 50
    # Second trade: 30 from maker 11
    assert t_py[1].qty == t_cy[1]["qty"] == 30

    # Remaining in both books should be 70 shares at 15000
    _, py_asks = py_lob.get_depth(levels=1)
    _, cy_asks = cy_lob.get_depth(levels=1)
    assert py_asks[0] == cy_asks[0] == (15000, 70)


def test_cancellation_equivalence():
    py_lob = BaselineOrderBook()
    cy_lob = CythonOrderBook()

    py_lob.process_order(20, SIDE_BUY, 14900, 500)
    cy_lob.process_order(20, SIDE_BUY, 14900, 500)

    assert py_lob.get_best_bid() == cy_lob.get_best_bid() == 14900

    assert py_lob.cancel_order(20) is True
    assert cy_lob.cancel_order(20) is True

    assert py_lob.get_best_bid() == cy_lob.get_best_bid() is None


if __name__ == "__main__":
    print("\nRunning OrderBook Equivalence Tests...")
    test_matching_equivalence_simple_crossing()
    test_matching_equivalence_partial_fills_and_queues()
    test_cancellation_equivalence()
    print("ALL EQUIVALENCE TESTS PASSED!\n")
