/**
 * ChronosMatch Real-Time HFT Web Terminal Controller
 * WebSocket streaming, DOM rendering, canvas chart generation, and order execution.
 */

// Global Application State
let ws = null;
let currentSide = "BUY";
let simActive = false;
let activeChartTab = "depth";
let lastTelemetry = null;

// DOM Elements
const wsStatus = document.getElementById("wsStatus");
const wsStatusText = document.getElementById("wsStatusText");
const backendText = document.getElementById("backendText");
const uptimeVal = document.getElementById("uptimeVal");
const ringOccupancy = document.getElementById("ringOccupancy");
const ringFill = document.getElementById("ringFill");

// Hero Stats
const statP50 = document.getElementById("statP50");
const statP99 = document.getElementById("statP99");
const statThroughput = document.getElementById("statThroughput");
const statDropped = document.getElementById("statDropped");
const statOrders = document.getElementById("statOrders");
const statTradesCount = document.getElementById("statTradesCount");
const statVolume = document.getElementById("statVolume");

// LOB Elements
const asksContainer = document.getElementById("asksContainer");
const bidsContainer = document.getElementById("bidsContainer");
const spreadVal = document.getElementById("spreadVal");
const spreadTicks = document.getElementById("spreadTicks");
const midVal = document.getElementById("midVal");

// Tape Elements
const tapeBody = document.getElementById("tapeBody");
const tapeCount = document.getElementById("tapeCount");

// Simulator Elements
const rateSlider = document.getElementById("rateSlider");
const rateDisplay = document.getElementById("rateDisplay");
const btnStreamToggle = document.getElementById("btnStreamToggle");

// Ticket Elements
const btnSideBuy = document.getElementById("btnSideBuy");
const btnSideSell = document.getElementById("btnSideSell");
const orderPrice = document.getElementById("orderPrice");
const orderQty = document.getElementById("orderQty");
const btnSubmitOrder = document.getElementById("btnSubmitOrder");
const orderFeedback = document.getElementById("orderFeedback");

// Latency Grid Elements
const latMin = document.getElementById("latMin");
const latP50 = document.getElementById("latP50");
const latP90 = document.getElementById("latP90");
const latP95 = document.getElementById("latP95");
const latP99 = document.getElementById("latP99");
const latP999 = document.getElementById("latP999");
const latMax = document.getElementById("latMax");
const latAvg = document.getElementById("latAvg");

// Canvas
const depthCanvas = document.getElementById("depthCanvas");
const latencyCanvas = document.getElementById("latencyCanvas");
const depthCtx = depthCanvas ? depthCanvas.getContext("2d") : null;
const latencyCtx = latencyCanvas ? latencyCanvas.getContext("2d") : null;


// ==========================================================================
// 1. WEBSOCKET CLIENT & AUTO-RECONNECT
// ==========================================================================

function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        wsStatus.className = "status-pill connected";
        wsStatusText.textContent = "CONNECTED";
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === "telemetry") {
                handleTelemetry(data);
            }
        } catch (err) {
            console.error("Failed to parse WebSocket packet:", err);
        }
    };

    ws.onclose = () => {
        wsStatus.className = "status-pill disconnected";
        wsStatusText.textContent = "RECONNECTING";
        setTimeout(connectWebSocket, 1500);
    };

    ws.onerror = (err) => {
        wsStatus.className = "status-pill disconnected";
        wsStatusText.textContent = "DISCONNECTED";
        ws.close();
    };
}


// ==========================================================================
// 2. TELEMETRY HANDLER & UI DISPATCHER
// ==========================================================================

function handleTelemetry(data) {
    lastTelemetry = data;

    // Header Meta
    if (data.backend) backendText.textContent = data.backend;
    if (data.uptime !== undefined) uptimeVal.textContent = `${data.uptime.toFixed(1)}s`;

    // Ring Buffer
    if (data.ring_buffer) {
        const rb = data.ring_buffer;
        ringOccupancy.textContent = `${rb.occupancy.toLocaleString()} / ${rb.capacity.toLocaleString()} (${rb.pct.toFixed(1)}%)`;
        ringFill.style.width = `${Math.min(100, rb.pct)}%`;
        statDropped.textContent = `${rb.dropped.toLocaleString()} dropped packets`;
    }

    // Hero Stats
    if (data.latency) {
        statP50.innerHTML = `${data.latency.p50_us.toFixed(2)} <span class="unit">μs</span>`;
        statP99.innerHTML = `${data.latency.p99_us.toFixed(2)} <span class="unit">μs</span>`;

        latMin.textContent = `${data.latency.min_us.toFixed(2)} μs`;
        latP50.textContent = `${data.latency.p50_us.toFixed(2)} μs`;
        latP90.textContent = `${data.latency.p90_us.toFixed(2)} μs`;
        latP95.textContent = `${data.latency.p95_us.toFixed(2)} μs`;
        latP99.textContent = `${data.latency.p99_us.toFixed(2)} μs`;
        latP999.textContent = `${(data.latency.p99_9_us || 0).toFixed(2)} μs`;
        latMax.textContent = `${data.latency.max_us.toFixed(2)} μs`;
        latAvg.textContent = `${data.latency.avg_us.toFixed(2)} μs`;
    }

    if (data.throughput !== undefined) {
        statThroughput.innerHTML = `${Math.round(data.throughput).toLocaleString()} <span class="unit">ord/s</span>`;
    }

    if (data.orders_processed !== undefined) {
        statOrders.textContent = data.orders_processed.toLocaleString();
    }

    if (data.trades_executed !== undefined) {
        statTradesCount.textContent = `${data.trades_executed.toLocaleString()} trades executed`;
    }

    if (data.total_volume !== undefined) {
        statVolume.innerHTML = `${data.total_volume.toLocaleString()} <span class="unit">shares</span>`;
    }

    // Spread & Mid
    if (data.spread !== null && data.spread !== undefined) {
        spreadVal.textContent = `$${data.spread.toFixed(2)}`;
        spreadTicks.textContent = `(${data.spread_ticks} ticks)`;
    } else {
        spreadVal.textContent = "—";
        spreadTicks.textContent = "(— ticks)";
    }

    if (data.mid_price !== null && data.mid_price !== undefined) {
        midVal.textContent = `$${data.mid_price.toFixed(2)}`;
        // Update ticket default price if user hasn't touched it recently
        if (!document.activeElement || document.activeElement !== orderPrice) {
            orderPrice.placeholder = data.mid_price.toFixed(2);
        }
    }

    // Render Depth Ladder
    renderOrderBook(data.bids || [], data.asks || []);

    // Render Trades Tape
    renderTrades(data.recent_trades || []);

    // Render Canvas
    if (activeChartTab === "depth") {
        drawDepthChart(data.bids || [], data.asks || []);
    } else {
        drawLatencyHistogram(data.latency);
    }

    // Simulator UI sync
    if (data.simulator) {
        simActive = data.simulator.active;
        if (simActive) {
            btnStreamToggle.textContent = "⏹ Stop Continuous Stream";
            btnStreamToggle.className = "btn btn-stream active";
        } else {
            btnStreamToggle.textContent = "▶ Start Continuous Stream";
            btnStreamToggle.className = "btn btn-stream";
        }
    }
}


// ==========================================================================
// 3. LEVEL-2 ORDER BOOK DEPTH LADDER (DOM)
// ==========================================================================

function renderOrderBook(bids, asks) {
    // 1. Calculate max cumulative depth for proportional width bars
    let maxCumulative = 1;
    let cumBid = 0;
    const bidLevels = bids.map(b => {
        cumBid += b.qty;
        return { price: b.price, qty: b.qty, cum: cumBid };
    });

    let cumAsk = 0;
    const askLevels = asks.map(a => {
        cumAsk += a.qty;
        return { price: a.price, qty: a.qty, cum: cumAsk };
    });

    maxCumulative = Math.max(cumBid, cumAsk, 1);

    // 2. Render Asks (Reverse so highest ask is at top, best ask is right above spread banner)
    const reversedAsks = [...askLevels].reverse();
    if (reversedAsks.length === 0) {
        asksContainer.innerHTML = `<div class="empty-hint">Ask book empty</div>`;
    } else {
        asksContainer.innerHTML = reversedAsks.map(level => {
            const pct = Math.min(100, (level.cum / maxCumulative) * 100);
            return `
                <div class="lob-row" onclick="fillPrice(${level.price})">
                    <div class="depth-bar" style="width: ${pct}%"></div>
                    <span class="row-price">$${level.price.toFixed(2)}</span>
                    <span class="row-size">${level.qty.toLocaleString()}</span>
                    <span class="row-total">${level.cum.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }

    // 3. Render Bids (Descending from best bid down)
    if (bidLevels.length === 0) {
        bidsContainer.innerHTML = `<div class="empty-hint">Bid book empty</div>`;
    } else {
        bidsContainer.innerHTML = bidLevels.map(level => {
            const pct = Math.min(100, (level.cum / maxCumulative) * 100);
            return `
                <div class="lob-row" onclick="fillPrice(${level.price})">
                    <div class="depth-bar" style="width: ${pct}%"></div>
                    <span class="row-price">$${level.price.toFixed(2)}</span>
                    <span class="row-size">${level.qty.toLocaleString()}</span>
                    <span class="row-total">${level.cum.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }
}


// ==========================================================================
// 4. TIME & SALES (TRADE TAPE)
// ==========================================================================

let lastSeenTradeId = -1;

function renderTrades(trades) {
    if (!trades || trades.length === 0) {
        tapeBody.innerHTML = `<div class="tape-empty">No fills yet. Fire orders to see live execution tape.</div>`;
        tapeCount.textContent = "0 Fills";
        return;
    }

    tapeCount.textContent = `${trades.length} Recent Fills`;

    const html = trades.slice(0, 25).map(tr => {
        const d = new Date(tr.timestamp_ns / 1_000_000);
        const timeStr = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}.${(tr.timestamp_ns % 1_000_000_000 / 1_000_000).toFixed(0).padStart(3, '0')}`;
        
        // Randomly identify aggressor or display maker/taker
        const sideClass = (tr.trade_id % 2 === 0) ? "tape-side-buy" : "tape-side-sell";
        const sideText = (tr.trade_id % 2 === 0) ? "BUY" : "SELL";

        return `
            <div class="tape-row">
                <span class="tape-time">${timeStr}</span>
                <span class="${sideClass}">${sideText}</span>
                <span class="tape-price">$${tr.price.toFixed(2)}</span>
                <span class="tape-qty">${tr.qty.toLocaleString()}</span>
                <span class="tape-id">#${tr.maker_order_id}</span>
                <span class="tape-id">#${tr.taker_order_id}</span>
            </div>
        `;
    }).join("");

    tapeBody.innerHTML = html;
}


// ==========================================================================
// 5. INTERACTIVE CANVAS CHARTS
// ==========================================================================

function drawDepthChart(bids, asks) {
    if (!depthCtx) return;
    const w = depthCanvas.width;
    const h = depthCanvas.height;

    depthCtx.clearRect(0, 0, w, h);

    // Background grid
    depthCtx.strokeStyle = "rgba(255, 255, 255, 0.04)";
    depthCtx.lineWidth = 1;
    for (let y = 30; y < h; y += 40) {
        depthCtx.beginPath();
        depthCtx.moveTo(0, y);
        depthCtx.lineTo(w, y);
        depthCtx.stroke();
    }

    if (bids.length === 0 && asks.length === 0) {
        depthCtx.fillStyle = "#64748b";
        depthCtx.font = "11px Inter, sans-serif";
        depthCtx.textAlign = "center";
        depthCtx.fillText("Waiting for order depth data...", w / 2, h / 2);
        return;
    }

    // Cumulative sums
    let cumBids = [];
    let bTotal = 0;
    for (let i = 0; i < bids.length; i++) {
        bTotal += bids[i].qty;
        cumBids.push({ price: bids[i].price, cum: bTotal });
    }

    let cumAsks = [];
    let aTotal = 0;
    for (let i = 0; i < asks.length; i++) {
        aTotal += asks[i].qty;
        cumAsks.push({ price: asks[i].price, cum: aTotal });
    }

    const maxVol = Math.max(bTotal, aTotal, 1);
    const midX = w / 2;

    // Draw Bids (Green curve on Left, from midX backward)
    if (cumBids.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 15);

        for (let i = 0; i < cumBids.length; i++) {
            const x = midX - ((i + 1) / cumBids.length) * (midX - 20);
            const y = (h - 20) - (cumBids[i].cum / maxVol) * (h - 45);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX - (midX - 20);
        depthCtx.lineTo(lastX, h - 15);
        depthCtx.closePath();

        const bidGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        bidGrad.addColorStop(0, "rgba(0, 230, 118, 0.45)");
        bidGrad.addColorStop(1, "rgba(0, 230, 118, 0.02)");
        depthCtx.fillStyle = bidGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#00e676";
        depthCtx.lineWidth = 2;
        depthCtx.stroke();
    }

    // Draw Asks (Red curve on Right, from midX forward)
    if (cumAsks.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 15);

        for (let i = 0; i < cumAsks.length; i++) {
            const x = midX + ((i + 1) / cumAsks.length) * (midX - 20);
            const y = (h - 20) - (cumAsks[i].cum / maxVol) * (h - 45);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX + (midX - 20);
        depthCtx.lineTo(lastX, h - 15);
        depthCtx.closePath();

        const askGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        askGrad.addColorStop(0, "rgba(255, 23, 68, 0.45)");
        askGrad.addColorStop(1, "rgba(255, 23, 68, 0.02)");
        depthCtx.fillStyle = askGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#ff1744";
        depthCtx.lineWidth = 2;
        depthCtx.stroke();
    }

    // Center divider & labels
    depthCtx.strokeStyle = "rgba(0, 242, 254, 0.4)";
    depthCtx.lineWidth = 1;
    depthCtx.beginPath();
    depthCtx.moveTo(midX, 10);
    depthCtx.lineTo(midX, h - 15);
    depthCtx.stroke();

    depthCtx.fillStyle = "#94a3b8";
    depthCtx.font = "10px JetBrains Mono, monospace";
    depthCtx.textAlign = "left";
    depthCtx.fillText("BIDS (Cumulative)", 20, 20);
    depthCtx.textAlign = "right";
    depthCtx.fillText("ASKS (Cumulative)", w - 20, 20);
}

function drawLatencyHistogram(stats) {
    if (!latencyCtx || !stats) return;
    const w = latencyCanvas.width;
    const h = latencyCanvas.height;

    latencyCtx.clearRect(0, 0, w, h);

    const labels = ["Min", "p50", "p90", "p95", "p99", "p99.9"];
    const values = [
        stats.min_us,
        stats.p50_us,
        stats.p90_us,
        stats.p95_us,
        stats.p99_us,
        stats.p99_9_us || stats.p99_us * 1.5,
    ];

    const maxVal = Math.max(...values, 2.0);
    const barWidth = 45;
    const gap = (w - (labels.length * barWidth)) / (labels.length + 1);

    for (let i = 0; i < labels.length; i++) {
        const x = gap + i * (barWidth + gap);
        const barHeight = Math.max(6, (values[i] / maxVal) * (h - 60));
        const y = h - 30 - barHeight;

        // Gradient for bars
        const grad = latencyCtx.createLinearGradient(0, y, 0, y + barHeight);
        if (i <= 1) {
            grad.addColorStop(0, "#00f2fe");
            grad.addColorStop(1, "rgba(0, 242, 254, 0.2)");
        } else if (i <= 4) {
            grad.addColorStop(0, "#00e676");
            grad.addColorStop(1, "rgba(0, 230, 118, 0.2)");
        } else {
            grad.addColorStop(0, "#ffc107");
            grad.addColorStop(1, "rgba(255, 193, 7, 0.2)");
        }

        latencyCtx.fillStyle = grad;
        latencyCtx.fillRect(x, y, barWidth, barHeight);

        // Value text
        latencyCtx.fillStyle = "#f0f4f8";
        latencyCtx.font = "10px JetBrains Mono, monospace";
        latencyCtx.textAlign = "center";
        latencyCtx.fillText(`${values[i].toFixed(2)}μs`, x + barWidth / 2, y - 5);

        // Label text
        latencyCtx.fillStyle = "#94a3b8";
        latencyCtx.font = "10px Inter, sans-serif";
        latencyCtx.fillText(labels[i], x + barWidth / 2, h - 12);
    }
}


// ==========================================================================
// 6. CHART TABS CONTROLLER
// ==========================================================================

const tabDepth = document.getElementById("tabDepth");
const tabLatency = document.getElementById("tabLatency");

tabDepth.addEventListener("click", () => {
    activeChartTab = "depth";
    tabDepth.className = "chart-tab active";
    tabLatency.className = "chart-tab";
    depthCanvas.style.display = "block";
    latencyCanvas.style.display = "none";
    if (lastTelemetry) drawDepthChart(lastTelemetry.bids, lastTelemetry.asks);
});

tabLatency.addEventListener("click", () => {
    activeChartTab = "latency";
    tabLatency.className = "chart-tab active";
    tabDepth.className = "chart-tab";
    latencyCanvas.style.display = "block";
    depthCanvas.style.display = "none";
    if (lastTelemetry) drawLatencyHistogram(lastTelemetry.latency);
});


// ==========================================================================
// 7. FIREHOSE CONTROLS & REST API DISPATCHERS
// ==========================================================================

async function fireBurst(count) {
    try {
        orderFeedback.textContent = `🚀 Firing burst of ${count.toLocaleString()} orders...`;
        const resp = await fetch("/api/burst", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ count }),
        });
        const result = await resp.json();
        orderFeedback.textContent = `✓ Fired ${result.orders_fired.toLocaleString()} orders in ${result.elapsed_seconds}s (${result.write_throughput.toLocaleString()} ord/s)!`;
        setTimeout(() => { orderFeedback.textContent = ""; }, 4000);
    } catch (err) {
        orderFeedback.textContent = `Error: ${err.message}`;
    }
}

async function toggleStream() {
    try {
        simActive = !simActive;
        const rate = parseInt(rateSlider.value, 10);
        await fetch("/api/simulator/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ active: simActive, rate }),
        });
    } catch (err) {
        console.error("Stream toggle failed:", err);
    }
}

rateSlider.addEventListener("input", (e) => {
    const val = parseInt(e.target.value, 10);
    rateDisplay.textContent = `${val.toLocaleString()} ord/s`;
});

rateSlider.addEventListener("change", async (e) => {
    const val = parseInt(e.target.value, 10);
    if (simActive) {
        await fetch("/api/simulator/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ active: true, rate: val }),
        });
    }
});

async function resetEngine() {
    try {
        await fetch("/api/reset", { method: "POST" });
        orderFeedback.textContent = "↺ Limit Order Book & Ring Buffer reset.";
        setTimeout(() => { orderFeedback.textContent = ""; }, 3000);
    } catch (err) {
        orderFeedback.textContent = `Reset failed: ${err.message}`;
    }
}


// ==========================================================================
// 8. MANUAL ORDER ENTRY TICKET
// ==========================================================================

function setSide(side) {
    currentSide = side;
    if (side === "BUY") {
        btnSideBuy.className = "btn-side btn-buy active";
        btnSideSell.className = "btn-side btn-sell";
        btnSubmitOrder.className = "btn btn-submit-order buy-mode";
        btnSubmitOrder.textContent = "SUBMIT BUY ORDER";
    } else {
        btnSideBuy.className = "btn-side btn-buy";
        btnSideSell.className = "btn-side btn-sell active";
        btnSubmitOrder.className = "btn btn-submit-order sell-mode";
        btnSubmitOrder.textContent = "SUBMIT SELL ORDER";
    }
}

function stepPrice(delta) {
    const cur = parseFloat(orderPrice.value) || 150.0;
    orderPrice.value = (Math.max(0.01, cur + delta)).toFixed(2);
}

function setQty(qty) {
    orderQty.value = qty;
}

function fillPrice(price) {
    orderPrice.value = price.toFixed(2);
}

async function submitManualOrder(event) {
    event.preventDefault();
    const price = parseFloat(orderPrice.value);
    const qty = parseInt(orderQty.value, 10);

    if (isNaN(price) || price <= 0 || isNaN(qty) || qty <= 0) {
        orderFeedback.textContent = "Invalid price or quantity";
        return;
    }

    try {
        orderFeedback.textContent = `Sending ${currentSide} ${qty} shs @ $${price.toFixed(2)}...`;
        const resp = await fetch("/api/order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                side: currentSide,
                price: price,
                qty: qty,
            }),
        });

        const result = await resp.json();
        if (result.success) {
            orderFeedback.textContent = `✓ Order #${result.order_id} written to IPC bus!`;
        } else {
            orderFeedback.textContent = `⚠ Buffer saturated, order dropped.`;
        }
        setTimeout(() => { orderFeedback.textContent = ""; }, 3500);
    } catch (err) {
        orderFeedback.textContent = `Order failed: ${err.message}`;
    }
}


// ==========================================================================
// 9. INITIALIZATION
// ==========================================================================
window.addEventListener("DOMContentLoaded", () => {
    setSide("BUY");
    connectWebSocket();
});
