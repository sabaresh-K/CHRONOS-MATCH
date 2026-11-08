"""
Unit test for Day 3: Asynchronous Audit Persistence Daemon (ledger_daemon.py)
Verifies database initialization, WAL mode, high-throughput batching, and non-blocking async queueing.
"""

import os
import sys
import asyncio
import sqlite3
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.ledger_daemon import TradeLedger, LedgerDaemon


def test_trade_ledger_direct_batch():
    test_db = "test_audit_trades.db"
    if os.path.exists(test_db):
        try:
            os.remove(test_db)
        except OSError:
            pass

    ledger = TradeLedger(db_path=test_db)
    
    # Check WAL mode
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode;")
    mode = cur.fetchone()[0]
    conn.close()
    assert mode.lower() == "wal", f"Expected WAL mode, got {mode}"

    # Batch insert 1,000 trades
    trades = [
        {
            "trade_id": i + 1,
            "maker_order_id": 1000 + i,
            "taker_order_id": 2000 + i,
            "price": 15025 + (i % 50),
            "qty": 100,
            "timestamp_ns": time.time_ns(),
        }
        for i in range(1000)
    ]

    inserted = ledger.insert_trade_batch(trades)
    assert inserted == 1000, f"Expected 1000 inserted, got {inserted}"

    summary = ledger.get_summary()
    assert summary["total_trades"] == 1000
    assert summary["total_shares"] == 100_000
    assert summary["total_notional"] > 0

    if os.path.exists(test_db):
        try:
            os.remove(test_db)
            if os.path.exists(f"{test_db}-wal"):
                os.remove(f"{test_db}-wal")
            if os.path.exists(f"{test_db}-shm"):
                os.remove(f"{test_db}-shm")
        except OSError:
            pass
    print("  [PASS] test_trade_ledger_direct_batch")


async def test_async_daemon_drain():
    test_db = "test_daemon_trades.db"
    if os.path.exists(test_db):
        try:
            os.remove(test_db)
        except OSError:
            pass

    daemon = LedgerDaemon(db_path=test_db, batch_size=200, flush_interval_s=0.02)
    daemon_task = asyncio.create_task(daemon.run())

    # Feed 1,500 trades into daemon queue
    trades = [
        {
            "trade_id": i + 1,
            "maker_order_id": 5000 + i,
            "taker_order_id": 6000 + i,
            "price": 15000,
            "qty": 50,
            "timestamp_ns": time.time_ns(),
        }
        for i in range(1500)
    ]
    await daemon.enqueue_trades(trades)

    # Allow worker to flush
    await asyncio.sleep(0.1)
    daemon.stop()
    await daemon_task

    assert daemon.trades_persisted == 1500
    summary = daemon.ledger.get_summary()
    assert summary["total_trades"] == 1500
    assert summary["total_shares"] == 75_000

    if os.path.exists(test_db):
        try:
            os.remove(test_db)
            if os.path.exists(f"{test_db}-wal"):
                os.remove(f"{test_db}-wal")
            if os.path.exists(f"{test_db}-shm"):
                os.remove(f"{test_db}-shm")
        except OSError:
            pass
    print("  [PASS] test_async_daemon_drain")


if __name__ == "__main__":
    print("\n==================================================")
    print(" DAY 3: ASYNC AUDIT PERSISTENCE DAEMON TESTS")
    print("==================================================")
    test_trade_ledger_direct_batch()
    asyncio.run(test_async_daemon_drain())
    print("==================================================")
    print(" ALL DAY 3 LEDGER DAEMON TESTS PASSED!")
    print("==================================================\n")
