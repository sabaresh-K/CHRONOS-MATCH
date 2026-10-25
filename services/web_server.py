"""
ChronosMatch Web Gateway & Real-Time WebSocket Server
Bridges the Zero-Copy IPC Ring Buffer and Cython Native LOB with a modern web dashboard.
"""

import os
import sys
import time
import asyncio
import random
from typing import Dict, List, Optional, Set
from contextlib import asynccontextmanager

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from core import (
    LimitOrderBook,
    RingBuffer,
    ENGINE_BACKEND,
    SIDE_BUY,
    SIDE_SELL,
    SIDE_CANCEL,
)


class OrderRequest(BaseModel):
    side: str  # "BUY", "SELL", "CANCEL"
    price: float  # In dollars, e.g. 150.25
    qty: int  # Share quantity
    order_id: Optional[int] = None


class BurstRequest(BaseModel):
    count: int = 50000


class SimulatorConfigRequest(BaseModel):
    active: bool
    rate: int = 50000


class AppState:
    def __init__(self, shm_path: str = "chronos_ipc.shm"):
        self.shm_path = shm_path
        if os.path.exists(shm_path):
            self.ring_buffer: RingBuffer = RingBuffer(filepath=shm_path, capacity=65536, create=False)
        else:
            self.ring_buffer: RingBuffer = RingBuffer(filepath=shm_path, capacity=65536, create=True)
        self.lob: LimitOrderBook = LimitOrderBook()
        self.clients: Set[WebSocket] = set()

        # Simulator state
        self.sim_active: bool = False
        self.sim_rate: int = 50000
        self.sim_mid: int = 15000
        self.sim_order_id: int = 1_000_000
        self.active_sim_order_ids: List[int] = []

        # Trade tracking
        self.recent_trades: List[Dict] = []
        self.max_recent_trades: int = 50

        # Performance tracking
        self.start_time: float = time.perf_counter()
        self.last_orders_count: int = 0
        self.last_rate_calc_time: float = time.perf_counter()
        self.current_rate: float = 0.0

    def generate_sim_order(self) -> None:
        """Generate a microstructural order packet directly into the ring buffer."""
        self.sim_order_id += 1
        oid = self.sim_order_id
        now_ns = time.time_ns()
        roll = random.random()

        # Random walk for mid
        drift = 1 if random.random() > 0.5 else -1
        if abs(self.sim_mid - 15000) > 300:
            drift = -1 if self.sim_mid > 15000 else 1
        self.sim_mid += drift

        # 2% Cancellation
        if roll < 0.02 and self.active_sim_order_ids:
            idx = random.randrange(len(self.active_sim_order_ids))
            cancel_id = self.active_sim_order_ids.pop(idx)
            self.ring_buffer.write_order(
                order_id=cancel_id,
                side=SIDE_CANCEL,
                price=0,
                qty=0,
                timestamp_ns=now_ns,
            )
            return

        # 6% Crossing Market Order
        if roll < 0.08:
            if random.random() < 0.5:
                # Market Buy
                self.ring_buffer.write_order(
                    order_id=oid,
                    side=SIDE_BUY,
                    price=self.sim_mid + 50,
                    qty=random.choice([25, 50, 100, 200]),
                    timestamp_ns=now_ns,
                )
            else:
                # Market Sell
                self.ring_buffer.write_order(
                    order_id=oid,
                    side=SIDE_SELL,
                    price=max(100, self.sim_mid - 50),
                    qty=random.choice([25, 50, 100, 200]),
                    timestamp_ns=now_ns,
                )
            return

        # 46% Limit Bid
        if roll < 0.54:
            offset = random.randint(1, 10)
            price = self.sim_mid - offset
            qty = random.choice([50, 100, 200, 300, 500, 1000])
            if self.ring_buffer.write_order(order_id=oid, side=SIDE_BUY, price=price, qty=qty, timestamp_ns=now_ns):
                if len(self.active_sim_order_ids) < 5000:
                    self.active_sim_order_ids.append(oid)
            return

        # 46% Limit Ask
        offset = random.randint(1, 10)
        price = self.sim_mid + offset
        qty = random.choice([50, 100, 200, 300, 500, 1000])
        if self.ring_buffer.write_order(order_id=oid, side=SIDE_SELL, price=price, qty=qty, timestamp_ns=now_ns):
            if len(self.active_sim_order_ids) < 5000:
                self.active_sim_order_ids.append(oid)


state = AppState()


async def matching_engine_worker():
    """Continuously drains packets from the ring buffer and processes through LOB."""
    while True:
        try:
            packets = state.ring_buffer.read_batch(max_batch_size=4096)
            if packets:
                lob_process = state.lob.process_order
                for order_id, side, price, qty, timestamp_ns in packets:
                    trades = lob_process(order_id, side, price, qty, timestamp_ns)
                    if trades:
                        for tr in trades:
                            trade_dict = {
                                "trade_id": tr.trade_id if hasattr(tr, "trade_id") else tr.get("trade_id", 0),
                                "price": (tr.price if hasattr(tr, "price") else tr.get("price", 0)) / 100.0,
                                "qty": tr.qty if hasattr(tr, "qty") else tr.get("qty", 0),
                                "maker_order_id": tr.maker_order_id if hasattr(tr, "maker_order_id") else tr.get("maker_order_id", 0),
                                "taker_order_id": tr.taker_order_id if hasattr(tr, "taker_order_id") else tr.get("taker_order_id", 0),
                                "timestamp_ns": tr.timestamp_ns if hasattr(tr, "timestamp_ns") else tr.get("timestamp_ns", 0),
                            }
                            state.recent_trades.insert(0, trade_dict)
                        if len(state.recent_trades) > state.max_recent_trades:
                            state.recent_trades = state.recent_trades[:state.max_recent_trades]
            else:
                await asyncio.sleep(0.002)
        except Exception as e:
            await asyncio.sleep(0.01)


async def simulator_worker():
    """Background task generating orders when simulator is active."""
    while True:
        if state.sim_active and state.sim_rate > 0:
            batch_size = max(10, min(1000, state.sim_rate // 50))
            for _ in range(batch_size):
                state.generate_sim_order()
            interval = batch_size / state.sim_rate
            await asyncio.sleep(interval)
        else:
            await asyncio.sleep(0.05)


async def metrics_calculator_worker():
    """Periodically recalculates instantaneous throughput rate."""
    while True:
        await asyncio.sleep(0.5)
        now = time.perf_counter()
        dt = now - state.last_rate_calc_time
        if dt >= 0.5:
            delta_orders = state.lob.orders_processed - state.last_orders_count
            state.current_rate = delta_orders / dt
            state.last_orders_count = state.lob.orders_processed
            state.last_rate_calc_time = now


async def websocket_broadcast_worker():
    """Broadcasts Level-2 order book depth, trades, and telemetry at ~25 FPS."""
    while True:
        await asyncio.sleep(0.04)  # 25 FPS
        if not state.clients:
            continue

        try:
            # Depth (top 10 levels)
            raw_bids, raw_asks = state.lob.get_depth(levels=10)
            bids = [{"price": p / 100.0, "qty": q} for p, q in raw_bids]
            asks = [{"price": p / 100.0, "qty": q} for p, q in raw_asks]

            best_bid = state.lob.get_best_bid()
            best_ask = state.lob.get_best_ask()
            spread = state.lob.get_spread()
            mid = state.lob.get_mid_price()

            latency_stats = state.lob.get_latency_stats()
            occupancy = state.ring_buffer.get_occupancy()
            capacity = state.ring_buffer.capacity
            dropped = state.ring_buffer.dropped_count

            payload = {
                "type": "telemetry",
                "backend": ENGINE_BACKEND,
                "uptime": time.perf_counter() - state.start_time,
                "bids": bids,
                "asks": asks,
                "best_bid": (best_bid / 100.0) if best_bid is not None else None,
                "best_ask": (best_ask / 100.0) if best_ask is not None else None,
                "spread": (spread / 100.0) if spread is not None else None,
                "spread_ticks": spread,
                "mid_price": (mid / 100.0) if mid is not None else None,
                "orders_processed": state.lob.orders_processed,
                "trades_executed": state.lob.trades_executed,
                "total_volume": state.lob.total_volume,
                "throughput": state.current_rate,
                "latency": latency_stats,
                "ring_buffer": {
                    "occupancy": occupancy,
                    "capacity": capacity,
                    "dropped": dropped,
                    "pct": (occupancy / capacity) * 100.0,
                },
                "simulator": {
                    "active": state.sim_active,
                    "rate": state.sim_rate,
                },
                "recent_trades": state.recent_trades[:20],
            }

            disconnected = set()
            for ws in list(state.clients):
                try:
                    await ws.send_json(payload)
                except Exception:
                    disconnected.add(ws)

            for ws in disconnected:
                state.clients.discard(ws)
        except Exception as e:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background workers
    t1 = asyncio.create_task(matching_engine_worker())
    t2 = asyncio.create_task(simulator_worker())
    t3 = asyncio.create_task(metrics_calculator_worker())
    t4 = asyncio.create_task(websocket_broadcast_worker())
    yield
    t1.cancel()
    t2.cancel()
    t3.cancel()
    t4.cancel()
    state.ring_buffer.close()


app = FastAPI(title="ChronosMatch Trading Terminal", lifespan=lifespan)

# Mount static web directory
web_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
os.makedirs(web_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=web_dir), name="static")


@app.get("/")
async def root():
    index_file = os.path.join(web_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "ChronosMatch Web API Active. Static frontend missing."}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    state.clients.add(websocket)
    try:
        while True:
            # Keep socket open and receive any client messages/heartbeats
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        state.clients.discard(websocket)
    except Exception:
        state.clients.discard(websocket)


@app.post("/api/order")
async def submit_order(order: OrderRequest):
    """Submit a manual limit/market order into the IPC bus."""
    side_code = SIDE_BUY if order.side.upper() == "BUY" else SIDE_SELL
    price_cents = int(round(order.price * 100))
    oid = order.order_id or int(time.time_ns() & 0xFFFFFFFF)

    written = state.ring_buffer.write_order(
        order_id=oid,
        side=side_code,
        price=price_cents,
        qty=order.qty,
        timestamp_ns=time.time_ns(),
    )
    return {
        "success": written,
        "order_id": oid,
        "side": order.side.upper(),
        "price": order.price,
        "qty": order.qty,
    }


@app.post("/api/burst")
async def fire_burst(burst: BurstRequest):
    """Fire an unthrottled burst of synthetic orders into the shared memory bus."""
    count = burst.count
    t0 = time.perf_counter()
    for _ in range(count):
        state.generate_sim_order()
    elapsed = time.perf_counter() - t0
    rate = count / max(0.00001, elapsed)

    return {
        "orders_fired": count,
        "elapsed_seconds": round(elapsed, 4),
        "write_throughput": round(rate),
    }


@app.post("/api/simulator/config")
async def configure_simulator(config: SimulatorConfigRequest):
    """Start, stop, or re-rate the background market order simulator."""
    state.sim_active = config.active
    state.sim_rate = max(1000, min(500000, config.rate))
    return {
        "active": state.sim_active,
        "rate": state.sim_rate,
    }


@app.post("/api/reset")
async def reset_engine():
    """Reset the order book and ring buffer."""
    state.lob.reset()
    state.ring_buffer.reset()
    state.recent_trades.clear()
    state.active_sim_order_ids.clear()
    return {"message": "ChronosMatch Engine reset successfully"}


@app.get("/api/status")
async def get_status():
    return {
        "backend": ENGINE_BACKEND,
        "uptime": time.perf_counter() - state.start_time,
        "orders_processed": state.lob.orders_processed,
        "trades_executed": state.lob.trades_executed,
        "total_volume": state.lob.total_volume,
        "occupancy": state.ring_buffer.get_occupancy(),
        "capacity": state.ring_buffer.capacity,
        "dropped": state.ring_buffer.dropped_count,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.web_server:app", host="127.0.0.1", port=8000, reload=False)
