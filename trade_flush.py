import logging
import queue
import threading
import time

from typing import Any, Iterable, Mapping, Optional

from sqlite_ledger import SQLiteTradeLedger


logger = logging.getLogger(__name__)


class TradeFlushWorker:

    def __init__(
        self,
        db_path: str = "data/trades.db",
        batch_size: int = 500,
        flush_interval: float = 0.1,
        max_queue_size: int = 10000,
        max_retries: int = 5,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        if flush_interval <= 0:
            raise ValueError("flush_interval must be greater than zero")

        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be greater than zero")

        if max_retries <= 0:
            raise ValueError("max_retries must be greater than zero")

        self.db_path = db_path
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries

        self.trade_queue = queue.Queue(maxsize=max_queue_size)

        self._stop_event = threading.Event()
        self._startup_event = threading.Event()
        self._submit_lock = threading.Lock()

        self._thread = None
        self._startup_error = None
        self._worker_error = None
        self._accepting_trades = False

        # Stores trades that could not be written to SQLite.
        self._failed_trades = []

    @property
    def worker_error(self) -> Optional[Exception]:
        with self._submit_lock:
            return self._worker_error

    @property
    def failed_trades(self) -> list:
        with self._submit_lock:
            return list(self._failed_trades)

    @property
    def pending_count(self) -> int:
        with self._submit_lock:
            return (
                len(self._failed_trades)
                + self.trade_queue.qsize()
            )

    def start(self) -> None:

        with self._submit_lock:

            if self._thread is not None and self._thread.is_alive():
                return

            if self._failed_trades:
                raise RuntimeError(
                    "Cannot restart: failed trades require recovery"
                )

            if self.trade_queue.qsize() > 0:
                raise RuntimeError(
                    "Cannot restart while queued trades remain"
                )

            self._stop_event.clear()
            self._startup_event.clear()

            self._startup_error = None
            self._worker_error = None

            self._thread = threading.Thread(
                target=self._run,
                name="TradeFlushWorker",
                daemon=False,
            )

            self._thread.start()

        if not self._startup_event.wait(timeout=10):

            self._stop_event.set()
            self._thread.join(timeout=10)

            raise TimeoutError(
                "Trade flush worker initialization timed out"
            )

        if self._startup_error is not None:
            raise RuntimeError(
                "Trade flush worker initialization failed"
            ) from self._startup_error

        with self._submit_lock:

            if self._worker_error is not None:
                raise RuntimeError(
                    "Trade flush worker failed during startup"
                ) from self._worker_error

            if (
                self._thread is None
                or not self._thread.is_alive()
            ):
                raise RuntimeError(
                    "Trade flush worker is not running"
                )

            self._accepting_trades = True

        logger.info("Trade flush worker started")

    def submit_trades(
        self,
        trades: Iterable[Mapping[str, Any]],
    ) -> int:

        normalized_trades = []

        # Validate the entire input before queueing anything.
        for trade in trades:

            normalized = {
                "trade_id": int(trade["trade_id"]),
                "maker_order_id": int(trade["maker_order_id"]),
                "taker_order_id": int(trade["taker_order_id"]),
                "price": int(trade["price"]),
                "qty": int(trade["qty"]),
                "timestamp_ns": int(trade["timestamp_ns"]),
            }

            if normalized["qty"] <= 0:
                raise ValueError(
                    "Trade quantity must be positive"
                )

            normalized_trades.append(normalized)

        submitted = 0

        with self._submit_lock:

            if not self._accepting_trades:
                raise RuntimeError(
                    "Trade flush worker is not accepting trades"
                )

            if self._worker_error is not None:
                raise RuntimeError(
                    "Trade flush worker has failed"
                ) from self._worker_error

            if (
                self._thread is None
                or not self._thread.is_alive()
            ):
                raise RuntimeError(
                    "Trade flush worker is not running"
                )

            for trade in normalized_trades:

                try:
                    self.trade_queue.put_nowait(trade)
                    submitted += 1

                except queue.Full as exc:
                    raise RuntimeError(
                        f"Trade queue is full. "
                        f"{submitted} trades were accepted; "
                        "the remaining trades were not accepted."
                    ) from exc

        return submitted

    def _run(self) -> None:

        ledger = None
        batch = []

        try:

            # SQLite connection belongs to this worker thread.
            ledger = SQLiteTradeLedger(self.db_path)

        except Exception as exc:

            with self._submit_lock:
                self._startup_error = exc
                self._worker_error = exc
                self._accepting_trades = False

            self._startup_event.set()

            logger.exception(
                "Failed to initialize SQLite ledger"
            )

            return

        self._startup_event.set()

        try:

            while True:

                if (
                    self._stop_event.is_set()
                    and self.trade_queue.empty()
                ):
                    break

                try:

                    first_trade = self.trade_queue.get(
                        timeout=self.flush_interval
                    )

                except queue.Empty:
                    continue

                batch = [first_trade]

                while len(batch) < self.batch_size:

                    try:

                        trade = self.trade_queue.get_nowait()
                        batch.append(trade)

                    except queue.Empty:
                        break

                # Attempt to persist the complete batch.
                self._flush_with_retry(
                    ledger,
                    batch,
                )

                # Mark queue entries complete only after success.
                for _ in batch:
                    self.trade_queue.task_done()

                batch = []

        except Exception as exc:

            logger.exception(
                "Trade worker encountered an unexpected failure"
            )

            self._preserve_failed_trades(
                batch,
                exc,
            )

        finally:

            if ledger is not None:

                try:
                    ledger.close()

                except Exception as exc:

                    with self._submit_lock:

                        if self._worker_error is None:
                            self._worker_error = exc

                        self._accepting_trades = False

                    logger.exception(
                        "Failed to close SQLite ledger"
                    )

            logger.info(
                "Trade flush worker stopped"
            )

    def _preserve_failed_trades(
        self,
        batch: list,
        error: Exception,
    ) -> None:

        with self._submit_lock:

            self._accepting_trades = False
            self._worker_error = error

            # Preserve the batch that failed.
            self._failed_trades.extend(batch)

            # Preserve trades still waiting in the queue.
            while True:

                try:
                    trade = self.trade_queue.get_nowait()

                except queue.Empty:
                    break

                else:
                    self._failed_trades.append(trade)
                    self.trade_queue.task_done()

            # Complete queue accounting for the failed batch.
            for _ in batch:
                self.trade_queue.task_done()

    def _flush_with_retry(
        self,
        ledger: SQLiteTradeLedger,
        batch: list,
    ) -> None:

        retry_delay = 0.5
        max_retry_delay = 10.0

        for attempt in range(
            1,
            self.max_retries + 1,
        ):

            try:

                inserted = ledger.insert_trades(batch)

                logger.info(
                    "Flushed %d trades; inserted %d new records",
                    len(batch),
                    inserted,
                )

                return

            except Exception:

                logger.exception(
                    "Trade insertion failed "
                    "(attempt %d/%d)",
                    attempt,
                    self.max_retries,
                )

                if attempt == self.max_retries:
                    raise

                time.sleep(retry_delay)

                retry_delay = min(
                    retry_delay * 2,
                    max_retry_delay,
                )

    def stop(
        self,
        timeout: float = 30.0,
    ) -> None:

        with self._submit_lock:

            self._accepting_trades = False
            self._stop_event.set()

        if self._thread is None:
            return

        self._thread.join(timeout=timeout)

        if self._thread.is_alive():
            raise TimeoutError(
                "Trade flush worker did not stop in time"
            )

        if self._worker_error is not None:

            raise RuntimeError(
                "Trade flush worker stopped with an error. "
                "Check failed_trades before proceeding."
            ) from self._worker_error

        logger.info(
            "Trade flush worker shutdown complete"
        )
