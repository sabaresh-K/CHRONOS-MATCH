"""
ChronosMatch — Day 3 Deliverable: Asynchronous Audit Persistence Daemon (ledger_daemon.py)
Non-blocking background consumer worker that reads executed trade records and flushes them
to an asynchronous SQLite audit ledger with batching and zero impact on the hot matching loop.
"""

import os
import sys
import time
import asyncio
import sqlite3
import argparse
from typing import List, Dict, Any, Optional

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.ipc_bus import RingBuffer, SIDE_BUY, SIDE_SELL, SIDE_CANCEL


class TradeLedger:
    """
    High-Performance SQLite Trade Audit Ledger.
    Uses WAL (Write-Ahead Logging) and batched transactions to maximize write throughput.
    """

    def __init__(self, db_path: str = "chronos_trades.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        # High performance PRAGMAs for fast append-only logging
        cur.execute("PRAGMA journal_mode = WAL;")
        cur.execute("PRAGMA synchronous = NORMAL;")
        cur.execute("PRAGMA cache_size = 10000;")
        cur.execute("PRAGMA temp_store = MEMORY;")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_ledger (
                trade_id INTEGER PRIMARY KEY,
                maker_order_id INTEGER NOT NULL,
                taker_order_id INTEGER NOT NULL,
                price_cents INTEGER NOT NULL,
                price_dollars REAL NOT NULL,
                qty INTEGER NOT NULL,
                notional_dollars REAL NOT NULL,
                timestamp_ns INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON trade_ledger(timestamp_ns);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_maker_taker ON trade_ledger(maker_order_id, taker_order_id);")
        conn.commit()
        conn.close()

    def insert_trade_batch(self, trades: List[Dict[str, Any]]) -> int:
        """Batch-insert trades in a single atomic transaction."""
        if not trades:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        rows = [
            (
                t.get("trade_id"),
                t.get("maker_order_id"),
                t.get("taker_order_id"),
                t.get("price"),
                t.get("price", 0) / 100.0,
                t.get("qty"),
                (t.get("price", 0) / 100.0) * t.get("qty", 0),
                t.get("timestamp_ns"),
            )
            for t in trades
        ]
        cur.executemany("""
            INSERT OR IGNORE INTO trade_ledger 
            (trade_id, maker_order_id, taker_order_id, price_cents, price_dollars, qty, notional_dollars, timestamp_ns)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        count = cur.rowcount
        conn.close()
        return len(rows)

    def get_summary(self) -> Dict[str, Any]:
        """Fetch total records, total volume, and notional dollar volume."""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(qty), 0), COALESCE(SUM(notional_dollars), 0.0) FROM trade_ledger;")
        row = cur.fetchone()
        conn.close()
        return {
            "total_trades": row[0],
            "total_shares": row[1],
            "total_notional": row[2],
        }


class LedgerDaemon:
    """
    Non-blocking async persistence daemon that drains trade streams or polls LOB trade buffers.
    """

    def __init__(
        self,
        db_path: str = "chronos_trades.db",
        batch_size: int = 500,
        flush_interval_s: float = 0.05,
    ):
        self.ledger = TradeLedger(db_path)
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)
        self.running = False
        self.trades_persisted = 0
        self.batches_flushed = 0

    async def enqueue_trade(self, trade: Dict[str, Any]) -> None:
        """Enqueue trade non-blockingly."""
        try:
            self.queue.put_nowait(trade)
        except asyncio.QueueFull:
            # Backpressure guard
            await self.queue.put(trade)

    async def enqueue_trades(self, trades: List[Dict[str, Any]]) -> None:
        for t in trades:
            await self.enqueue_trade(t)

    async def run(self) -> None:
        """Background flush loop."""
        self.running = True
        batch: List[Dict[str, Any]] = []
        last_flush = time.perf_counter()

        while self.running:
            try:
                # Wait for next trade with timeout
                item = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval_s)
                batch.append(item)
                self.queue.task_done()
            except asyncio.TimeoutError:
                pass

            now = time.perf_counter()
            should_flush = len(batch) >= self.batch_size or ((now - last_flush) >= self.flush_interval_s and batch)

            if should_flush:
                flushed = self.ledger.insert_trade_batch(batch)
                self.trades_persisted += flushed
                self.batches_flushed += 1
                batch.clear()
                last_flush = now

        # Drain remaining items in queue on shutdown
        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
                batch.append(item)
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break

        if batch:
            flushed = self.ledger.insert_trade_batch(batch)
            self.trades_persisted += flushed
            self.batches_flushed += 1
            batch.clear()

    def stop(self) -> None:
        self.running = False


async def main():
    parser = argparse.ArgumentParser(description="ChronosMatch Async Trade Ledger Persistence Daemon")
    parser.add_argument("--db", type=str, default="chronos_trades.db", help="SQLite database path")
    parser.add_argument("--batch", type=int, default=500, help="Batch commit size")
    args = parser.parse_args()

    daemon = LedgerDaemon(db_path=args.db, batch_size=args.batch)
    print(f"[*] ChronosMatch Ledger Daemon initialized (DB: {args.db}, Batch: {args.batch})")
    print("[*] Listening for trade executions... (Ctrl+C to stop)")

    try:
        await daemon.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        daemon.stop()
        print("\n[*] Ledger daemon gracefully stopped.")


if __name__ == "__main__":
    asyncio.run(main())
