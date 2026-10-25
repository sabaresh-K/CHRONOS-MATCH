/**
 * ChronosMatch Institutional HFT Terminal Controller
 * Professional client-side state machine, WebSocket data stream, DOM ladder diffing,
 * and high-DPI canvas charting.
 */

// Global State
let ws = null;
let currentSide = "BUY";
let currentType = "LIMIT";
let activeTab = "depth";
let simActive = false;
let lastTelemetry = null;

// DOM Elements
const connectionStatus = document.getElementById("connectionStatus");
const connectionText = document.getElementById("connectionText");
const backendText = document.getElementById("backendText");
const uptimeVal = document.getElementById("uptimeVal");

// Header Ticker
const tickerMid = document.getElementById("tickerMid");
const tickerSpread = document.getElementById("tickerSpread");
const tickerVol = document.getElementById("tickerVol");

// Toolbar
const rateSlider = document.getElementById("rateSlider");
const rateDisplay = document.getElementById("rateDisplay");
const btnStreamToggle = document.getElementById("btnStreamToggle");
const statP50 = document.getElementById("statP50");
const statP99 = document.getElementById("statP99");
const statThroughput = document.getElementById("statThroughput");

// Order Book (DOM)
const asksContainer = document.getElementById("asksContainer");
const bidsContainer = document.getElementById("bidsContainer");
const spreadVal = document.getElementById("spreadVal");
const spreadTicks = document.getElementById("spreadTicks");
const midVal = document.getElementById("midVal");

// Tabs & Panes
const tabDepth = document.getElementById("tabDepth");
const tabTape = document.getElementById("tabTape");
const tabLatency = document.getElementById("tabLatency");
const paneDepth = document.getElementById("paneDepth");
const paneTape = document.getElementById("paneTape");
const paneLatency = document.getElementById("paneLatency");
const chipThroughput = document.getElementById("chipThroughput");
const chipTrades = document.getElementById("chipTrades");

// Canvases
const depthCanvas = document.getElementById("depthCanvas");
const latencyCanvas = document.getElementById("latencyCanvas");
const depthCtx = depthCanvas ? depthCanvas.getContext("2d") : null;
const latencyCtx = latencyCanvas ? latencyCanvas.getContext("2d") : null;

// Tape
const tapeBody = document.getElementById("tapeBody");

// Ticket
const btnSideBuy = document.getElementById("btnSideBuy");
const btnSideSell = document.getElementById("btnSideSell");
const typeLimit = document.getElementById("typeLimit");
const typeMarket = document.getElementById("typeMarket");
const priceFieldWrapper = document.getElementById("priceFieldWrapper");
const orderPrice = document.getElementById("orderPrice");
const orderQty = document.getElementById("orderQty");
const orderValueEstimate = document.getElementById("orderValueEstimate");
const btnSubmitOrder = document.getElementById("btnSubmitOrder");
const ticketStatus = document.getElementById("ticketStatus");

// Latency Grid
const latP50 = document.getElementById("latP50");
const latP90 = document.getElementById("latP90");
const latP95 = document.getElementById("latP95");
const latP99 = document.getElementById("latP99");
const latP999 = document.getElementById("latP999");
const latMinMax = document.getElementById("latMinMax");
const latAvg = document.getElementById("latAvg");

// Footer
const footerOccupancy = document.getElementById("footerOccupancy");
const footerOrdersCount = document.getElementById("footerOrdersCount");
const footerTradesCount = document.getElementById("footerTradesCount");
const footerDroppedCount = document.getElementById("footerDroppedCount");


// ==========================================================================
// 1. WEBSOCKET REAL-TIME DATA STREAM
// ==========================================================================

function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        connectionStatus.className = "connection-pill";
        connectionText.textContent = "CONNECTED";
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === "telemetry") {
                handleTelemetry(data);
            }
        } catch (err) {
            console.error("Telemetry parse error:", err);
        }
    };

    ws.onclose = () => {
        connectionStatus.className = "connection-pill disconnected";
        connectionText.textContent = "RECONNECTING";
        setTimeout(connectWebSocket, 1500);
    };

    ws.onerror = () => {
        connectionStatus.className = "connection-pill disconnected";
        connectionText.textContent = "DISCONNECTED";
        ws.close();
    };
}


// ==========================================================================
// 2. TELEMETRY INGESTION & DISPATCH
// ==========================================================================

function handleTelemetry(data) {
    lastTelemetry = data;

    // Header Meta
    if (data.backend) backendText.textContent = data.backend;
    if (data.uptime !== undefined) uptimeVal.textContent = `${data.uptime.toFixed(1)}s`;

    // Mid & Spread
    const midPrice = data.mid_price !== null && data.mid_price !== undefined ? data.mid_price : 150.0;
    const midStr = `$${midPrice.toFixed(2)}`;
    tickerMid.textContent = midStr;
    midVal.textContent = midStr;

    if (data.spread !== null && data.spread !== undefined) {
        const spStr = `$${data.spread.toFixed(2)}`;
        tickerSpread.innerHTML = `${spStr} <span class="sub-ticks">(${data.spread_ticks} ticks)</span>`;
        spreadVal.textContent = spStr;
        spreadTicks.textContent = `(${data.spread_ticks} ticks)`;
    } else {
        tickerSpread.innerHTML = `— <span class="sub-ticks">(—)</span>`;
        spreadVal.textContent = "—";
        spreadTicks.textContent = "(—)";
    }

    if (data.total_volume !== undefined) {
        tickerVol.textContent = `${data.total_volume.toLocaleString()} shs`;
    }

    // Quick Stats Toolbar
    if (data.latency) {
        const l = data.latency;
        statP50.textContent = `${l.p50_us.toFixed(2)} μs`;
        statP99.textContent = `${l.p99_us.toFixed(2)} μs`;

        latP50.textContent = `${l.p50_us.toFixed(2)} μs`;
        latP90.textContent = `${l.p90_us.toFixed(2)} μs`;
        latP95.textContent = `${l.p95_us.toFixed(2)} μs`;
        latP99.textContent = `${l.p99_us.toFixed(2)} μs`;
        latP999.textContent = `${(l.p99_9_us || 0).toFixed(2)} μs`;
        latMinMax.textContent = `${l.min_us.toFixed(2)} / ${l.max_us.toFixed(2)} μs`;
        latAvg.textContent = `${l.avg_us.toFixed(2)} μs`;
    }

    if (data.throughput !== undefined) {
        const rateFormatted = `${Math.round(data.throughput).toLocaleString()} ord/s`;
        statThroughput.textContent = rateFormatted;
        chipThroughput.textContent = rateFormatted;
    }

    if (data.trades_executed !== undefined) {
        chipTrades.textContent = `${data.trades_executed.toLocaleString()} fills`;
    }

    // Render Order Book
    renderOrderBook(data.bids || [], data.asks || []);

    // Render Active Tab Content
    if (activeTab === "depth") {
        drawDepthChart(data.bids || [], data.asks || []);
    } else if (activeTab === "tape") {
        renderTradeTape(data.recent_trades || []);
    } else if (activeTab === "latency") {
        drawLatencyHistogram(data.latency);
    }

    // Simulator State Sync
    if (data.simulator) {
        simActive = data.simulator.active;
        if (simActive) {
            btnStreamToggle.className = "btn btn-action active";
            btnStreamToggle.innerHTML = `
                <svg class="action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="6" y="4" width="4" height="16"/>
                    <rect x="14" y="4" width="4" height="16"/>
                </svg>
                <span>Pause Stream</span>
            `;
        } else {
            btnStreamToggle.className = "btn btn-action";
            btnStreamToggle.innerHTML = `
                <svg class="action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polygon points="5 3 19 12 5 21 5 3"/>
                </svg>
                <span>Start Stream</span>
            `;
        }
    }

    // Footer Metrics
    if (data.ring_buffer) {
        const rb = data.ring_buffer;
        footerOccupancy.textContent = `Occupancy: ${rb.occupancy.toLocaleString()} (${rb.pct.toFixed(1)}%)`;
        footerDroppedCount.textContent = `Dropped: ${rb.dropped.toLocaleString()}`;
    }
    if (data.orders_processed !== undefined) {
        footerOrdersCount.textContent = `Processed: ${data.orders_processed.toLocaleString()} orders`;
    }
    if (data.trades_executed !== undefined) {
        footerTradesCount.textContent = `Executed: ${data.trades_executed.toLocaleString()} trades`;
    }
}


// ==========================================================================
// 3. LEVEL-2 DEPTH LADDER (DOM)
// ==========================================================================

function renderOrderBook(bids, asks) {
    let maxVolume = 1;
    let bSum = 0;
    const bidRows = bids.map(b => {
        bSum += b.qty;
        return { price: b.price, qty: b.qty, total: bSum };
    });

    let aSum = 0;
    const askRows = asks.map(a => {
        aSum += a.qty;
        return { price: a.price, qty: a.qty, total: aSum };
    });

    maxVolume = Math.max(bSum, aSum, 1);

    // Asks Ladder: Inverted so lowest ask (best ask) is at bottom
    const reversedAsks = [...askRows].reverse();
    if (reversedAsks.length === 0) {
        asksContainer.innerHTML = `<div class="ladder-placeholder">No asks in book</div>`;
    } else {
        asksContainer.innerHTML = reversedAsks.map(r => {
            const widthPct = Math.min(100, (r.total / maxVolume) * 100);
            return `
                <div class="ladder-row" onclick="fillPrice(${r.price})">
                    <div class="bar" style="width: ${widthPct}%"></div>
                    <span class="row-price">${r.price.toFixed(2)}</span>
                    <span class="row-size">${r.qty.toLocaleString()}</span>
                    <span class="row-total">${r.total.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }

    // Bids Ladder: Highest bid (best bid) at top
    if (bidRows.length === 0) {
        bidsContainer.innerHTML = `<div class="ladder-placeholder">No bids in book</div>`;
    } else {
        bidsContainer.innerHTML = bidRows.map(r => {
            const widthPct = Math.min(100, (r.total / maxVolume) * 100);
            return `
                <div class="ladder-row" onclick="fillPrice(${r.price})">
                    <div class="bar" style="width: ${widthPct}%"></div>
                    <span class="row-price">${r.price.toFixed(2)}</span>
                    <span class="row-size">${r.qty.toLocaleString()}</span>
                    <span class="row-total">${r.total.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }
}


// ==========================================================================
// 4. TIME & SALES TAPE
// ==========================================================================

function renderTradeTape(trades) {
    if (!trades || trades.length === 0) {
        tapeBody.innerHTML = `<div class="tape-empty">No trade fills recorded yet.</div>`;
        return;
    }

    const html = trades.slice(0, 30).map(t => {
        const d = new Date(t.timestamp_ns / 1_000_000);
        const timeStr = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}.${(t.timestamp_ns % 1_000_000_000 / 1_000_000).toFixed(0).padStart(3, '0')}`;
        const isBuy = t.trade_id % 2 === 0;

        return `
            <div class="tape-row">
                <span class="tape-time">${timeStr}</span>
                <span class="${isBuy ? 'tape-side-buy' : 'tape-side-sell'}">${isBuy ? 'BUY' : 'SELL'}</span>
                <span class="tape-price">$${t.price.toFixed(2)}</span>
                <span class="tape-qty">${t.qty.toLocaleString()}</span>
                <span class="tape-id">#${t.maker_order_id}</span>
                <span class="tape-id">#${t.taker_order_id}</span>
            </div>
        `;
    }).join("");

    tapeBody.innerHTML = html;
}


// ==========================================================================
// 5. CANVAS CHARTS (MARKET DEPTH & LATENCY HISTOGRAM)
// ==========================================================================

function drawDepthChart(bids, asks) {
    if (!depthCtx) return;
    const w = depthCanvas.width;
    const h = depthCanvas.height;

    depthCtx.clearRect(0, 0, w, h);

    // Subtle background grid
    depthCtx.strokeStyle = "rgba(255, 255, 255, 0.03)";
    depthCtx.lineWidth = 1;
    for (let y = 40; y < h; y += 50) {
        depthCtx.beginPath();
        depthCtx.moveTo(0, y);
        depthCtx.lineTo(w, y);
        depthCtx.stroke();
    }

    if (bids.length === 0 && asks.length === 0) {
        depthCtx.fillStyle = "#64748b";
        depthCtx.font = "12px Inter, sans-serif";
        depthCtx.textAlign = "center";
        depthCtx.fillText("No order book depth to display", w / 2, h / 2);
        return;
    }

    // Cumulative calculations
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

    // 1. Draw Bid Cumulative Polygon (Green, Left Side)
    if (cumBids.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 20);

        for (let i = 0; i < cumBids.length; i++) {
            const x = midX - ((i + 1) / cumBids.length) * (midX - 25);
            const y = (h - 25) - (cumBids[i].cum / maxVol) * (h - 60);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX - (midX - 25);
        depthCtx.lineTo(lastX, h - 20);
        depthCtx.closePath();

        const bidGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        bidGrad.addColorStop(0, "rgba(16, 185, 129, 0.25)");
        bidGrad.addColorStop(1, "rgba(16, 185, 129, 0.02)");
        depthCtx.fillStyle = bidGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#10b981";
        depthCtx.lineWidth = 1.5;
        depthCtx.stroke();
    }

    // 2. Draw Ask Cumulative Polygon (Red, Right Side)
    if (cumAsks.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 20);

        for (let i = 0; i < cumAsks.length; i++) {
            const x = midX + ((i + 1) / cumAsks.length) * (midX - 25);
            const y = (h - 25) - (cumAsks[i].cum / maxVol) * (h - 60);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX + (midX - 25);
        depthCtx.lineTo(lastX, h - 20);
        depthCtx.closePath();

        const askGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        askGrad.addColorStop(0, "rgba(244, 63, 94, 0.25)");
        askGrad.addColorStop(1, "rgba(244, 63, 94, 0.02)");
        depthCtx.fillStyle = askGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#f43f5e";
        depthCtx.lineWidth = 1.5;
        depthCtx.stroke();
    }

    // Midline Marker
    depthCtx.strokeStyle = "rgba(255, 255, 255, 0.15)";
    depthCtx.setLineDash([3, 3]);
    depthCtx.beginPath();
    depthCtx.moveTo(midX, 20);
    depthCtx.lineTo(midX, h - 20);
    depthCtx.stroke();
    depthCtx.setLineDash([]);

    // Axis Labels
    depthCtx.fillStyle = "#64748b";
    depthCtx.font = "10.5px JetBrains Mono, monospace";
    depthCtx.textAlign = "left";
    depthCtx.fillText("BIDS (Cumulative)", 25, 25);
    depthCtx.textAlign = "right";
    depthCtx.fillText("ASKS (Cumulative)", w - 25, 25);
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
    const barWidth = 48;
    const gap = (w - (labels.length * barWidth)) / (labels.length + 1);

    for (let i = 0; i < labels.length; i++) {
        const x = gap + i * (barWidth + gap);
        const barHeight = Math.max(6, (values[i] / maxVal) * (h - 70));
        const y = h - 35 - barHeight;

        // Clean subtle fill
        latencyCtx.fillStyle = i <= 1 ? "rgba(56, 189, 248, 0.35)" : (i <= 4 ? "rgba(16, 185, 129, 0.35)" : "rgba(251, 191, 36, 0.35)");
        latencyCtx.fillRect(x, y, barWidth, barHeight);

        // Border
        latencyCtx.strokeStyle = i <= 1 ? "#38bdf8" : (i <= 4 ? "#10b981" : "#fbbf24");
        latencyCtx.lineWidth = 1;
        latencyCtx.strokeRect(x, y, barWidth, barHeight);

        // Value text
        latencyCtx.fillStyle = "#f8fafc";
        latencyCtx.font = "10.5px JetBrains Mono, monospace";
        latencyCtx.textAlign = "center";
        latencyCtx.fillText(`${values[i].toFixed(2)}μs`, x + barWidth / 2, y - 6);

        // Label text
        latencyCtx.fillStyle = "#94a3b8";
        latencyCtx.font = "11px Inter, sans-serif";
        latencyCtx.fillText(labels[i], x + barWidth / 2, h - 14);
    }
}


// ==========================================================================
// 6. TAB CONTROLLER
// ==========================================================================

function switchTab(tab) {
    activeTab = tab;
    tabDepth.className = tab === "depth" ? "seg-btn active" : "seg-btn";
    tabTape.className = tab === "tape" ? "seg-btn active" : "seg-btn";
    tabLatency.className = tab === "latency" ? "seg-btn active" : "seg-btn";

    paneDepth.className = tab === "depth" ? "tab-pane active" : "tab-pane";
    paneTape.className = tab === "tape" ? "tab-pane active" : "tab-pane";
    paneLatency.className = tab === "latency" ? "tab-pane active" : "tab-pane";

    if (lastTelemetry) {
        if (tab === "depth") drawDepthChart(lastTelemetry.bids, lastTelemetry.asks);
        else if (tab === "tape") renderTradeTape(lastTelemetry.recent_trades);
        else if (tab === "latency") drawLatencyHistogram(lastTelemetry.latency);
    }
}


// ==========================================================================
// 7. FIREHOSE SIMULATOR REST ACTIONS
// ==========================================================================

async function fireBurst(count) {
    try {
        ticketStatus.textContent = `Injecting ${count.toLocaleString()} orders into IPC...`;
        const resp = await fetch("/api/burst", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ count }),
        });
        const res = await resp.json();
        ticketStatus.textContent = `✓ Injected ${res.orders_fired.toLocaleString()} orders in ${res.elapsed_seconds}s (${res.write_throughput.toLocaleString()} ord/s)`;
        setTimeout(() => { ticketStatus.textContent = ""; }, 3500);
    } catch (err) {
        ticketStatus.textContent = `Burst error: ${err.message}`;
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
    rateDisplay.textContent = `${val.toLocaleString()} /s`;
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
        ticketStatus.textContent = "✓ Order book and buffer cleared";
        setTimeout(() => { ticketStatus.textContent = ""; }, 2500);
    } catch (err) {
        ticketStatus.textContent = `Reset failed: ${err.message}`;
    }
}


// ==========================================================================
// 8. INTERACTIVE ORDER ENTRY TICKET
// ==========================================================================

function setSide(side) {
    currentSide = side;
    if (side === "BUY") {
        btnSideBuy.className = "side-tab buy active";
        btnSideSell.className = "side-tab sell";
        btnSubmitOrder.className = "btn btn-submit-trade buy";
        btnSubmitOrder.textContent = currentType === "LIMIT" ? "Place Buy Order" : "Market Buy";
    } else {
        btnSideBuy.className = "side-tab buy";
        btnSideSell.className = "side-tab sell active";
        btnSubmitOrder.className = "btn btn-submit-trade sell";
        btnSubmitOrder.textContent = currentType === "LIMIT" ? "Place Sell Order" : "Market Sell";
    }
}

function setType(type) {
    currentType = type;
    if (type === "LIMIT") {
        typeLimit.className = "type-btn active";
        typeMarket.className = "type-btn";
        priceFieldWrapper.style.display = "flex";
        btnSubmitOrder.textContent = currentSide === "BUY" ? "Place Buy Order" : "Place Sell Order";
    } else {
        typeLimit.className = "type-btn";
        typeMarket.className = "type-btn active";
        priceFieldWrapper.style.display = "none";
        btnSubmitOrder.textContent = currentSide === "BUY" ? "Market Buy" : "Market Sell";
    }
    updateOrderEstimate();
}

function stepPrice(delta) {
    const cur = parseFloat(orderPrice.value) || 150.0;
    orderPrice.value = (Math.max(0.01, cur + delta)).toFixed(2);
    updateOrderEstimate();
}

function stepQty(delta) {
    const cur = parseInt(orderQty.value, 10) || 100;
    orderQty.value = Math.max(1, cur + delta);
    updateOrderEstimate();
}

function setQty(qty) {
    orderQty.value = qty;
    updateOrderEstimate();
}

function fillPrice(price) {
    orderPrice.value = price.toFixed(2);
    updateOrderEstimate();
}

function updateOrderEstimate() {
    const qty = parseInt(orderQty.value, 10) || 0;
    let price = parseFloat(orderPrice.value) || 150.0;
    if (currentType === "MARKET" && lastTelemetry && lastTelemetry.mid_price) {
        price = lastTelemetry.mid_price;
    }
    const val = price * qty;
    orderValueEstimate.textContent = `$${val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

async function submitManualOrder(event) {
    event.preventDefault();
    const qty = parseInt(orderQty.value, 10);
    let price = parseFloat(orderPrice.value);

    if (currentType === "MARKET") {
        price = currentSide === "BUY" ? 99999.0 : 0.01;
    }

    if (isNaN(price) || price <= 0 || isNaN(qty) || qty <= 0) {
        ticketStatus.textContent = "Invalid order parameters";
        return;
    }

    try {
        ticketStatus.textContent = `Routing ${currentSide} ${qty} @ $${price.toFixed(2)} to memory map...`;
        const resp = await fetch("/api/order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                side: currentSide,
                price: price,
                qty: qty,
            }),
        });

        const res = await resp.json();
        if (res.success) {
            ticketStatus.textContent = `✓ Order #${res.order_id} filled/queued in IPC`;
        } else {
            ticketStatus.textContent = "⚠ Buffer saturated, dropped";
        }
        setTimeout(() => { ticketStatus.textContent = ""; }, 3000);
    } catch (err) {
        ticketStatus.textContent = `Submission failed: ${err.message}`;
    }
}


// ==========================================================================
// 9. INITIALIZATION
// ==========================================================================
window.addEventListener("DOMContentLoaded", () => {
    setSide("BUY");
    setType("LIMIT");
    updateOrderEstimate();
    connectWebSocket();
});
