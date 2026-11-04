import sqlite3
import time

import pytest

from trade_flush import TradeFlushWorker


def make_trade(trade_id):
    return {
        "trade_id": trade_id,
        "maker_order_id": 1000 + trade_id,
        "taker_order_id": 2000 + trade_id,
        "price": 50000,
        "qty": 2,
        "timestamp_ns": time.time_ns(),
    }


def read_trade_count(db_path):
    connection = sqlite3.connect(db_path)

    try:
        result = connection.execute(
            "SELECT COUNT(*) FROM trades"
        ).fetchone()
        return result[0]
    finally:
        connection.close()


def test_batch_trades_are_flushed(tmp_path):
    db_path = tmp_path / "trades.db"

    worker = TradeFlushWorker(
        db_path=str(db_path),
        batch_size=3,
        flush_interval=0.05,
    )

    worker.start()

    try:
        submitted = worker.submit_trades(
            [make_trade(i) for i in range(1, 4)]
        )

        assert submitted == 3

        # Wait for automatic background batch flushing.
        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:
            if db_path.exists() and read_trade_count(db_path) == 3:
                break

            time.sleep(0.05)

        assert read_trade_count(db_path) == 3

    finally:
        worker.stop()


def test_shutdown_flushes_pending_trades(tmp_path):
    db_path = tmp_path / "shutdown.db"

    worker = TradeFlushWorker(
        db_path=str(db_path),
        batch_size=100,
        flush_interval=0.05,
    )

    worker.start()

    try:
        worker.submit_trades(
            [make_trade(i) for i in range(1, 6)]
        )
    finally:
        worker.stop()

    assert read_trade_count(db_path) == 5


def test_duplicate_trade_ids_are_not_duplicated(tmp_path):
    db_path = tmp_path / "duplicates.db"

    worker = TradeFlushWorker(
        db_path=str(db_path),
        batch_size=2,
        flush_interval=0.05,
    )

    worker.start()

    try:
        worker.submit_trades([
            make_trade(1),
            make_trade(1),
        ])
    finally:
        worker.stop()

    assert read_trade_count(db_path) == 1


def test_invalid_trade_quantity_is_rejected(tmp_path):
    db_path = tmp_path / "invalid.db"

    worker = TradeFlushWorker(db_path=str(db_path))
    worker.start()

    invalid_trade = make_trade(1)
    invalid_trade["qty"] = 0

    try:
        with pytest.raises(ValueError):
            worker.submit_trades([invalid_trade])
    finally:
        worker.stop()

    assert read_trade_count(db_path) == 0


def test_worker_rejects_trades_after_shutdown(tmp_path):
    db_path = tmp_path / "stopped.db"

    worker = TradeFlushWorker(db_path=str(db_path))

    worker.start()
    worker.stop()

    with pytest.raises(RuntimeError):
        worker.submit_trades([make_trade(1)])
