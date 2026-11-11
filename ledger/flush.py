"""
ChronosMatch — Background SQLite Ledger Persistence (ledger/flush.py)
Non-blocking background consumer worker that reads executed trade records and flushes them
to an asynchronous SQLite audit ledger with WAL mode and zero matching loop impact.
"""

from services.ledger_daemon import TradeLedger, LedgerDaemon, main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
