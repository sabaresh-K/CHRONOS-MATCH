"""
ChronosMatch Live Latency Monitor & Terminal Depth UI
Real-time curses dashboard displaying order book depth, execution throughput,
and nanosecond-precision matching telemetry from the shared memory bus.
Includes automatic ANSI VT100 fallback for environments without curses.
"""

import os
import sys
import time
import argparse
from typing import Optional, List, Tuple

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import (
    LimitOrderBook,
    RingBuffer,
    ENGINE_BACKEND,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_CANCEL,
)


class LatencyMonitor:
    """
    Live Telemetry Consumer & Visualizer.
    
    Drains packets from the shared memory ring buffer, drives the
    Price-Time Priority Limit Order Book, and renders real-time
    depth-of-book and latency statistics.
    """

    def __init__(self, shm_path: str = "chronos_ipc.shm") -> None:
        self.shm_path = shm_path
        self.ring_buffer: Optional[RingBuffer] = None
        self.lob: LimitOrderBook = LimitOrderBook()
        self.running: bool = False
        self.start_time: float = 0.0

    def start_engine(self) -> None:
        """Attach to the shared memory bus."""
        self.ring_buffer = RingBuffer(filepath=self.shm_path, create=False)
        self.start_time = time.perf_counter()
        self.running = True

    def step(self, batch_size: int = 1024) -> int:
        """
        Drain one batch of orders from IPC and process through matching engine.
        
        :return: Count of orders processed.
        """
        if self.ring_buffer is None:
            return 0

        packets = self.ring_buffer.read_batch(max_batch_size=batch_size)
        if not packets:
            return 0

        lob_process = self.lob.process_order
        for order_id, side, price, qty, timestamp_ns in packets:
            lob_process(order_id, side, price, qty, timestamp_ns)

        return len(packets)

    def format_price(self, price_cents: int) -> str:
        """Format fixed-point integer cents as dollar string."""
        return f"${price_cents / 100.0:8.2f}"

    def render_ansi_frame(self) -> str:
        """
        Render a full ANSI-colored dashboard frame.
        """
        now = time.perf_counter()
        uptime = now - self.start_time
        occupancy = self.ring_buffer.get_occupancy() if self.ring_buffer else 0
        cap = self.ring_buffer.capacity if self.ring_buffer else 1
        dropped = self.ring_buffer.dropped_count if self.ring_buffer else 0

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
        rate = orders / max(0.001, uptime)

        # ANSI Colors
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
            f" {BOLD}Engine Backend:{RESET} {YELLOW}{ENGINE_BACKEND}{RESET}  │  {BOLD}SHM File:{RESET} {self.shm_path}  │  {BOLD}Uptime:{RESET} {uptime:.1f}s",
            f" {BOLD}Ring Buffer:{RESET}    {occupancy:,} / {cap:,} slots ({occupancy/cap*100.1:.1f}%)  │  {BOLD}Dropped Packets:{RESET} {RED}{dropped}{RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"{BOLD}                        LIVE LEVEL-2 ORDER BOOK DEPTH                             {RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}{RED}{'ASKS (Sells)':^36}{RESET} │ {BOLD}{GREEN}{'BIDS (Buys)':^36}{RESET}",
            f"   {'Price':^16} │ {'Shares':^16} │ {'Price':^16} │ {'Shares':^16}",
            "   ─────────────────┼──────────────────┼─────────────────┼──────────────────",
        ]

        # Top 5 asks (rendered inverted: highest ask at top, best ask at bottom)
        asks_5 = list(reversed(asks[:5]))
        bids_5 = bids[:5]

        for i in range(5):
            # Ask side
            if i < len(asks_5):
                ap, aq = asks_5[i]
                ask_col = f"{RED}{self.format_price(ap):>16}{RESET} │ {RED}{aq:>16,}{RESET}"
            else:
                ask_col = f"{'—':>16} │ {'—':>16}"

            # Bid side
            if i < len(bids_5):
                bp, bq = bids_5[i]
                bid_col = f"{GREEN}{self.format_price(bp):>16}{RESET} │ {GREEN}{bq:>16,}{RESET}"
            else:
                bid_col = f"{'—':>16} │ {'—':>16}"

            lines.append(f"   {ask_col} │ {bid_col}")

        lines.extend([
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}Best Bid:{RESET} {GREEN}{self.format_price(bb) if bb else 'None':>10}{RESET}  │  {BOLD}Best Ask:{RESET} {RED}{self.format_price(ba) if ba else 'None':>10}{RESET}  │  {BOLD}Spread:{RESET} {spread_str}  │  {BOLD}Mid:{RESET} {mid_str}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"{BOLD}                        MATCHING ENGINE TELEMETRY                                 {RESET}",
            "──────────────────────────────────────────────────────────────────────────────────",
            f"   {BOLD}Orders Processed:{RESET} {orders:>12,}  │  {BOLD}Throughput:{RESET}   {rate:>12,.0f} ord/sec",
            f"   {BOLD}Trades Executed:{RESET}  {trades:>12,}  │  {BOLD}Matched Vol:{RESET}  {vol:>12,} shares",
            "   ──────────────────────────────────────┼────────────────────────────────────────",
            f"   {BOLD}Matching Latency Distribution:{RESET}",
            f"     • Min:   {stats['min_us']:>8.2f} μs          • Median (p50): {GREEN}{stats['p50_us']:>8.2f} μs{RESET}",
            f"     • p90:   {stats['p90_us']:>8.2f} μs          • p95:          {stats['p95_us']:>8.2f} μs",
            f"     • p99:   {stats['p99_us']:>8.2f} μs          • p99.9:        {stats.get('p99_9_us', 0):>8.2f} μs",
            f"     • Max:   {stats['max_us']:>8.2f} μs          • Avg:          {stats['avg_us']:>8.2f} μs",
            "══════════════════════════════════════════════════════════════════════════════════",
        ])
        return "\n".join(lines)

    def run_curses(self) -> None:
        """Run terminal UI via curses if supported."""
        import curses

        def _curses_loop(stdscr):
            curses.curs_set(0)
            stdscr.nodelay(True)
            stdscr.timeout(50)  # 20 FPS refresh

            self.start_engine()

            while self.running:
                # Drain pending orders
                self.step(batch_size=2048)

                # Check for user input
                try:
                    ch = stdscr.getch()
                    if ch in (ord('q'), ord('Q'), 27):  # 'q' or ESC
                        break
                    elif ch in (ord('r'), ord('R')):
                        self.lob.reset()
                except Exception:
                    pass

                # Render frame
                frame = self.render_ansi_frame()
                stdscr.erase()
                # Curses does not parse ANSI escapes by default, so strip them or print lines
                import re
                clean_frame = re.sub(r'\033\[[0-9;]*m', '', frame)
                try:
                    stdscr.addstr(0, 0, clean_frame[:curses.LINES * curses.COLS])
                except curses.error:
                    pass
                stdscr.refresh()

        curses.wrapper(_curses_loop)

    def run_ansi(self, refresh_hz: float = 10.0, max_iterations: Optional[int] = None) -> None:
        """
        Run terminal UI using ANSI escape codes (works on Windows & Linux).
        """
        self.start_engine()
        interval = 1.0 / refresh_hz
        count = 0

        # Enable Windows VT100 support
        if sys.platform == "win32":
            os.system("")

        print("\033[2J", end="")  # Clear entire screen
        try:
            while self.running:
                self.step(batch_size=4096)
                print("\033[H" + self.render_ansi_frame())
                time.sleep(interval)
                count += 1
                if max_iterations and count >= max_iterations:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            print("\n[LatencyMonitor] Shutting down monitor...")

    def close(self) -> None:
        """Release shared memory."""
        self.running = False
        if self.ring_buffer:
            self.ring_buffer.close()
            self.ring_buffer = None


def main() -> None:
    parser = argparse.ArgumentParser(description="ChronosMatch Live Latency Monitor & Order Book UI")
    parser.add_argument("--shm", type=str, default="chronos_ipc.shm", help="Path to shared memory file")
    parser.add_argument("--curses", action="store_true", help="Force curses mode")
    parser.add_argument("--once", action="store_true", help="Print single snapshot frame and exit")
    parser.add_argument("--iterations", type=int, default=None, help="Exit after N refresh cycles")
    args = parser.parse_args()

    # Create monitor
    monitor = LatencyMonitor(shm_path=args.shm)

    if args.once:
        monitor.start_engine()
        monitor.step(batch_size=4096)
        print(monitor.render_ansi_frame())
        monitor.close()
        return

    # Try curses if requested and available, otherwise use ANSI
    if args.curses:
        try:
            monitor.run_curses()
            return
        except ImportError:
            print("[LatencyMonitor] curses not installed, falling back to ANSI engine...")

    monitor.run_ansi(max_iterations=args.iterations)


if __name__ == "__main__":
    main()
