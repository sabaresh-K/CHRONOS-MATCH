"""
ChronosMatch — Day 3 Deliverable: Latency Telemetry & Curses TUI (dashboard.py)
Real-time curses dashboard displaying order book depth, execution throughput,
and nanosecond-precision matching telemetry from the shared memory bus.
Features hardware nanosecond timestamps (time.perf_counter_ns()) and sub-50us latency metrics.
"""

import os
import re
import sys
import time
import argparse
from typing import Optional, List, Tuple

# Ensure stdout handles UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from ipc_ringbuffer import IPCRingBuffer, SIDE_BUY, SIDE_SELL, SIDE_CANCEL
try:
    from orderbook import OrderBook
    ENGINE_BACKEND = "Cython Native (nogil arena)"
except ImportError:
    from core.lob_engine import LimitOrderBook as OrderBook
    ENGINE_BACKEND = "Cython lob_engine"


class ChronosDashboard:
    """
    Live Telemetry Consumer & Terminal Dashboard.
    Drains packets from the shared memory ring buffer, drives the
    Price-Time Priority Limit Order Book, and renders real-time
    depth-of-book and nanosecond latency distribution.
    """

    def __init__(self, shm_path: str = "chronos_ipc.shm") -> None:
        self.shm_path = shm_path
        self.rb: Optional[IPCRingBuffer] = None
        self.lob: OrderBook = OrderBook()
        self.running: bool = False
        self.start_time: float = 0.0

        # Transit latency tracking (producer timestamp to match time)
        self.transit_latencies_us: List[float] = []

    def start_engine(self) -> None:
        """Attach to the shared memory bus."""
        self.rb = IPCRingBuffer(filepath=self.shm_path, create=False)
        self.start_time = time.perf_counter()
        self.running = True

    def drain_step(self, batch_size: int = 2048) -> int:
        """
        Drain pending orders from shared memory and process in OrderBook with nogil.
        """
        if self.rb is None:
            return 0

        # Fast zero-copy memoryview batch processing if supported
        if hasattr(self.lob, "process_shm_batch"):
            view = memoryview(self.rb._mmap)
            return self.lob.process_shm_batch(view, max_orders=batch_size)

        # Fallback to tuple batch
        packets = self.rb.read_batch(max_batch_size=batch_size)
        if not packets:
            return 0

        now_ns = time.perf_counter_ns()
        for oid, side, price, qty, ts_ns in packets:
            self.lob.process_order(oid, side, price, qty, ts_ns)
            if len(self.transit_latencies_us) < 50000 and ts_ns > 0:
                self.transit_latencies_us.append(max(0.0, (now_ns - ts_ns) / 1000.0))

        return len(packets)

    def format_price(self, price_cents: Optional[int]) -> str:
        if price_cents is None:
            return "—"
        return f"${price_cents / 100.0:8.2f}"

    def render_frame(self) -> str:
        """Generate formatted ASCII/ANSI dashboard string."""
        now = time.perf_counter()
        uptime = max(0.001, now - self.start_time)

        occupancy = self.rb.get_occupancy() if self.rb else 0
        cap = self.rb.capacity if self.rb else 1
        dropped = self.rb.dropped_count if self.rb else 0

        bids, asks = self.lob.get_depth(levels=5)
        bb = self.lob.get_best_bid()
        ba = self.lob.get_best_ask()
        spread = self.lob.get_spread()
        spread_str = f"${spread / 100.0:.2f} ({spread} ticks)" if spread is not None else "N/A"
        mid = self.lob.get_mid_price()
        mid_str = f"${mid / 100.0:.2f}" if mid is not None else "N/A"

        stats = self.lob.get_latency_stats()
        orders = self.lob.orders_processed
        trades = self.lob.trades_executed
        vol = self.lob.total_volume
        rate = orders / uptime

        # ANSI styling
        CYAN = "\033[96m"
        GREEN = "\033[92m"
        RED = "\033[91m"
        YELLOW = "\033[93m"
        BOLD = "\033[1m"
        RESET = "\033[0m"

        lines = [
            f"{BOLD}{CYAN}╔══════════════════════════════════════════════════════════════════════════════════╗{RESET}",
            f"{BOLD}{CYAN}║              CHRONOSMATCH — ZERO-COPY HIGH-FREQUENCY TRADING ENGINE              ║{RESET}",
            f"{BOLD}{CYAN}╚══════════════════════════════════════════════════════════════════════════════════╝{RESET}",
            f" {BOLD}Engine Core:{RESET}    {YELLOW}{ENGINE_BACKEND}{RESET}  │  {BOLD}SHM:{RESET} {os.path.basename(self.shm_path)}  │  {BOLD}Uptime:{RESET} {uptime:.1f}s",
            f" {BOLD}Ring Buffer:{RESET}    {occupancy:,} / {cap:,} slots ({occupancy/cap*100.0:.1f}%)  │  {BOLD}Dropped Packets:{RESET} {RED}{dropped}{RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"{BOLD}                        LIVE LEVEL-2 ORDER BOOK DEPTH                             {RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}{RED}{'ASKS (Sells)':^36}{RESET} │ {BOLD}{GREEN}{'BIDS (Buys)':^36}{RESET}",
            f"   {'Price':^16} │ {'Shares':^16} │ {'Price':^16} │ {'Shares':^16}",
            "   ─────────────────┼──────────────────┼─────────────────┼──────────────────",
        ]

        asks_5 = list(reversed(asks[:5]))
        bids_5 = bids[:5]

        for i in range(5):
            if i < len(asks_5):
                ap, aq = asks_5[i]
                ask_col = f"{RED}{self.format_price(ap):>16}{RESET} │ {RED}{aq:>16,}{RESET}"
            else:
                ask_col = f"{'—':>16} │ {'—':>16}"

            if i < len(bids_5):
                bp, bq = bids_5[i]
                bid_col = f"{GREEN}{self.format_price(bp):>16}{RESET} │ {GREEN}{bq:>16,}{RESET}"
            else:
                bid_col = f"{'—':>16} │ {'—':>16}"

            lines.append(f"   {ask_col} │ {bid_col}")

        lines.extend([
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}Best Bid:{RESET} {GREEN}{self.format_price(bb):>10}{RESET}  │  {BOLD}Best Ask:{RESET} {RED}{self.format_price(ba):>10}{RESET}  │  {BOLD}Spread:{RESET} {spread_str}  │  {BOLD}Mid:{RESET} {mid_str}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"{BOLD}                        MATCHING ENGINE TELEMETRY                                 {RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}Orders Processed:{RESET} {orders:>12,}  │  {BOLD}Throughput:{RESET}   {rate:>12,.0f} ord/sec",
            f"   {BOLD}Trades Executed:{RESET}  {trades:>12,}  │  {BOLD}Matched Vol:{RESET}  {vol:>12,} shares",
            "   ──────────────────────────────────────┼────────────────────────────────────────",
            f"   {BOLD}Matching Latency Distribution (Hardware Nanosecond Resolution):{RESET}",
            f"     • Min Latency:    {stats['min_us']:>8.2f} μs       • Median (p50): {GREEN}{stats['p50_us']:>8.2f} μs{RESET} (<50μs target)",
            f"     • 90th %ile (p90):{stats['p90_us']:>8.2f} μs       • 95th %ile (p95):{stats['p95_us']:>8.2f} μs",
            f"     • 99th %ile (p99):{stats['p99_us']:>8.2f} μs       • 99.9th %ile:    {stats.get('p99_9_us', 0):>8.2f} μs",
            f"     • Max Latency:    {stats['max_us']:>8.2f} μs       • Mean Latency:   {stats['avg_us']:>8.2f} μs",
            "══════════════════════════════════════════════════════════════════════════════════",
            f" Controls: [Q] Quit  │  [R] Reset Engine State & Counters",
        ])
        return "\n".join(lines)

    def run_curses(self) -> None:
        """Run standard curses TUI."""
        try:
            import curses
        except ImportError:
            print("[Dashboard] curses module not available on this platform, falling back to ANSI terminal.")
            self.run_ansi()
            return

        def _loop(stdscr):
            try:
                curses.curs_set(0)
            except Exception:
                pass
            stdscr.nodelay(True)
            stdscr.timeout(50)  # 20 FPS refresh

            self.start_engine()

            while self.running:
                self.drain_step(batch_size=4096)

                try:
                    ch = stdscr.getch()
                    if ch in (ord('q'), ord('Q'), 27):
                        break
                    elif ch in (ord('r'), ord('R')):
                        self.lob.reset()
                except Exception:
                    pass

                frame = self.render_frame()
                clean_frame = re.sub(r'\033\[[0-9;]*m', '', frame)
                stdscr.erase()
                try:
                    stdscr.addstr(0, 0, clean_frame[:curses.LINES * curses.COLS])
                except curses.error:
                    pass
                stdscr.refresh()

        curses.wrapper(_loop)

    def run_ansi(self, refresh_hz: float = 10.0, max_frames: Optional[int] = None) -> None:
        """Run terminal UI with ANSI escape sequences (portable cross-platform)."""
        self.start_engine()
        interval = 1.0 / refresh_hz
        frames = 0

        # Enable ANSI VT100 on Windows console
        if sys.platform == "win32":
            os.system("")

        print("\033[2J", end="")  # Clear screen
        try:
            while self.running:
                self.drain_step(batch_size=4096)
                print("\033[H" + self.render_frame())
                time.sleep(interval)
                frames += 1
                if max_frames and frames >= max_frames:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            print("\n[Dashboard] Shutting down telemetry monitor...")

    def close(self) -> None:
        self.running = False
        if self.rb:
            self.rb.close()
            self.rb = None


def main() -> None:
    parser = argparse.ArgumentParser(description="ChronosMatch Day 3 Telemetry Dashboard")
    parser.add_argument("--shm", type=str, default="chronos_ipc.shm", help="Path to shared memory file")
    parser.add_argument("--curses", action="store_true", help="Launch in curses mode")
    parser.add_argument("--once", action="store_true", help="Print single snapshot frame and exit")
    parser.add_argument("--frames", type=int, default=None, help="Exit after N frames")
    args = parser.parse_args()

    dash = ChronosDashboard(shm_path=args.shm)

    if args.once:
        dash.start_engine()
        dash.drain_step(batch_size=4096)
        print(dash.render_frame())
        dash.close()
        return

    if args.curses:
        dash.run_curses()
    else:
        dash.run_ansi(max_frames=args.frames)


if __name__ == "__main__":
    main()
