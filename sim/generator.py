"""
ChronosMatch — Asyncio Market Firehose Simulator (sim/generator.py)
High-throughput order generator (>100,000 orders/sec) streaming directly into shared memory.
"""

from services.market_simulator import MarketSimulator, run_simulator

if __name__ == "__main__":
    run_simulator()
