import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Any


DEFAULT_DB_PATH = Path("data/trades.db")


SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY,
    maker_order_id INTEGER NOT NULL,
    taker_order_id INTEGER NOT NULL,
    price INTEGER NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    timestamp_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_timestamp
ON trades(timestamp_ns);

CREATE INDEX IF NOT EXISTS idx_trades_maker_order
ON trades(maker_order_id);

CREATE INDEX IF NOT EXISTS idx_trades_taker_order
ON trades(taker_order_id);
"""


class SQLiteTradeLedger:

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
        )

        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")

        self.initialize_schema()

    def initialize_schema(self) -> None:
        with self.connection:
            self.connection.executescript(SCHEMA)

    def insert_trades(
        self,
        trades: Iterable[Mapping[str, Any]],
    ) -> int:

        rows = [
            (
                int(trade["trade_id"]),
                int(trade["maker_order_id"]),
                int(trade["taker_order_id"]),
                int(trade["price"]),
                int(trade["qty"]),
                int(trade["timestamp_ns"]),
            )
            for trade in trades
        ]

        if not rows:
            return 0

        with self.connection:
            cursor = self.connection.executemany(
                """
                INSERT INTO trades (
                    trade_id,
                    maker_order_id,
                    taker_order_id,
                    price,
                    qty,
                    timestamp_ns
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO NOTHING
                """,
                rows,
            )

            inserted_count = cursor.rowcount

        return inserted_count

    def count_trades(self) -> int:
        cursor = self.connection.execute(
            "SELECT COUNT(*) FROM trades"
        )

        return int(cursor.fetchone()[0])

    def verify_trade_count(self, expected_count: int) -> bool:
        actual_count = self.count_trades()
        return actual_count == expected_count

    def close(self) -> None:
        if self.connection:
            self.connection.close()
