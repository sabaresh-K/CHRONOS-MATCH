/**
 * ChronosMatch Institutional HFT Terminal Controller
 * Real-time WebSocket processing, tactile order entry with instant fill feedback,
 * deterministic stream controls, and high-DPI canvas depth visualizer.
 */

// Global State
let ws = null;
let currentSide = "BUY";
let currentOrderType = "LIMIT";
let currentBookFilter = "both";
let activeMainTab = "depth";
let simActive = false;
let currentSimRate = 500;
let lastTelemetry = null;

// DOM Elements
const wsStatus = document.getElementById("wsStatus");
const wsStatusText = document.getElementById("wsStatusText");
const backendText = document.getElementById("backendText");

// Header Stats
const statMid = document.getElementById("statMid");
const statSpread = document.getElementById("statSpread");
const statP50 = document.getElementById("statP50");
const statThroughput = document.getElementById("statThroughput");
const statVolume = document.getElementById("statVolume");

// Stream Control
const btnStreamToggle = document.getElementById("btnStreamToggle");
const streamIcon = document.getElementById("streamIcon");
const streamBtnText = document.getElementById("streamBtnText");
const streamBadge = document.getElementById("streamBadge");

// Ring Buffer Progress
const ringMetaText = document.getElementById("ringMetaText");
const ringDroppedText = document.getElementById("ringDroppedText");
const ringProgressFill = document.getElementById("ringProgressFill");

// Order Book (DOM)
const asksContainer = document.getElementById("asksContainer");
const bidsContainer = document.getElementById("bidsContainer");
const spreadText = document.getElementById("spreadText");
const spreadTicksText = document.getElementById("spreadTicksText");
const midPriceText = document.getElementById("midPriceText");

// Center Panel Tabs
const tabDepth = document.getElementById("tabDepth");
const tabTape = document.getElementById("tabTape");
const tabLatency = document.getElementById("tabLatency");
const paneDepth = document.getElementById("paneDepth");
const paneTape = document.getElementById("paneTape");
const paneLatency = document.getElementById("paneLatency");
const chipOrdersTotal = document.getElementById("chipOrdersTotal");
const chipTradesTotal = document.getElementById("chipTradesTotal");

// Canvases
const depthCanvas = document.getElementById("depthCanvas");
const latencyCanvas = document.getElementById("latencyCanvas");
const depthCtx = depthCanvas ? depthCanvas.getContext("2d") : null;
const latencyCtx = latencyCanvas ? latencyCanvas.getContext("2d") : null;

// Tape
const tapeBody = document.getElementById("tapeBody");

// Order Ticket
const btnSideBuy = document.getElementById("btnSideBuy");
const btnSideSell = document.getElementById("btnSideSell");
const btnTypeLimit = document.getElementById("btnTypeLimit");
const btnTypeMarket = document.getElementById("btnTypeMarket");
const priceGroup = document.getElementById("priceGroup");
const orderPrice = document.getElementById("orderPrice");
const orderQty = document.getElementById("orderQty");
const orderEstValue = document.getElementById("orderEstValue");
const btnSubmitOrder = document.getElementById("btnSubmitOrder");
const orderFeedback = document.getElementById("orderFeedback");
const userOrdersList = document.getElementById("userOrdersList");

// Footer
const footerOrders = document.getElementById("footerOrders");
const footerTrades = document.getElementById("footerTrades");
const footerUptime = document.getElementById("footerUptime");


// ==========================================================================
// 1. WEBSOCKET REAL-TIME DATA STREAM
// ==========================================================================

function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        wsStatus.className = "ws-pill connected";
        wsStatusText.textContent = "LIVE";
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
        wsStatus.className = "ws-pill disconnected";
        wsStatusText.textContent = "OFFLINE";
        setTimeout(connectWebSocket, 1500);
    };

    ws.onerror = () => {
        wsStatus.className = "ws-pill disconnected";
        wsStatusText.textContent = "ERROR";
        ws.close();
    };
}


// ==========================================================================
// 2. TELEMETRY HANDLER
// ==========================================================================

function handleTelemetry(data) {
    lastTelemetry = data;

    // Header Info
    if (data.backend) backendText.textContent = data.backend;
    if (data.uptime !== undefined) footerUptime.textContent = `Uptime: ${data.uptime.toFixed(1)}s`;

    // Mid & Spread
    const mid = data.mid_price !== null && data.mid_price !== undefined ? data.mid_price : 150.0;
    const midStr = `$${mid.toFixed(2)}`;
    statMid.textContent = midStr;
    midPriceText.textContent = midStr;

    if (data.spread !== null && data.spread !== undefined) {
        const spStr = `$${data.spread.toFixed(2)}`;
        statSpread.innerHTML = `${spStr} <span class="dim">(${data.spread_ticks} ticks)</span>`;
        spreadText.textContent = spStr;
        spreadTicksText.textContent = `(${data.spread_ticks} ticks)`;
    } else {
        statSpread.innerHTML = `— <span class="dim">(—)</span>`;
        spreadText.textContent = "—";
        spreadTicksText.textContent = "(—)";
    }

    if (data.latency) {
        statP50.textContent = `${data.latency.p50_us.toFixed(2)} μs`;
    }

    if (data.throughput !== undefined) {
        statThroughput.textContent = `${Math.round(data.throughput).toLocaleString()} ord/s`;
    }

    if (data.total_volume !== undefined) {
        statVolume.textContent = `${data.total_volume.toLocaleString()} shs`;
    }

    if (data.orders_processed !== undefined) {
        chipOrdersTotal.textContent = `${data.orders_processed.toLocaleString()} orders`;
        footerOrders.textContent = `Orders: ${data.orders_processed.toLocaleString()}`;
    }

    if (data.trades_executed !== undefined) {
        chipTradesTotal.textContent = `${data.trades_executed.toLocaleString()} fills`;
        footerTrades.textContent = `Fills: ${data.trades_executed.toLocaleString()}`;
    }

    // Ring Buffer Hardware Telemetry
    if (data.ring_buffer) {
        const rb = data.ring_buffer;
        ringMetaText.textContent = `${rb.occupancy.toLocaleString()} / ${rb.capacity.toLocaleString()} (${rb.pct.toFixed(1)}%)`;
        ringProgressFill.style.width = `${Math.min(100, rb.pct)}%`;
        ringDroppedText.textContent = `${rb.dropped.toLocaleString()} dropped`;
    }

    // Simulator State Sync
    if (data.simulator) {
        syncStreamUI(data.simulator.active);
    }

    // Render Order Book
    renderOrderBook(data.bids || [], data.asks || []);

    // Render Tab Content
    if (activeMainTab === "depth") {
        drawDepthChart(data.bids || [], data.asks || []);
    } else if (activeMainTab === "tape") {
        renderTradeTape(data.recent_trades || []);
    } else if (activeMainTab === "latency") {
        drawLatencyHistogram(data.latency);
    }

    // Render Manual Orders History
    if (data.user_orders) {
        renderUserOrders(data.user_orders);
    }
}


// ==========================================================================
// 3. STREAM CONTROLS (INDEPENDENT & INFALLIBLE)
// ==========================================================================

function syncStreamUI(isActive) {
    simActive = isActive;
    if (simActive) {
        btnStreamToggle.className = "btn btn-stream-toggle stop-mode";
        streamIcon.textContent = "■";
        streamBtnText.textContent = "STOP STREAM";
        streamBadge.className = "stream-status-badge streaming";
        streamBadge.textContent = "STREAMING";
    } else {
        btnStreamToggle.className = "btn btn-stream-toggle start-mode";
        streamIcon.textContent = "▶";
        streamBtnText.textContent = "START STREAM";
        streamBadge.className = "stream-status-badge paused";
        streamBadge.textContent = "PAUSED";
    }
}

async function toggleStream() {
    // Immediately flip UI to give instant tactile feedback
    const targetState = !simActive;
    syncStreamUI(targetState);

    try {
        const endpoint = targetState ? "/api/simulator/start" : "/api/simulator/stop";
        const resp = await fetch(endpoint, { method: "POST" });
        const res = await resp.json();
        syncStreamUI(res.active);
        showFeedback(targetState ? "▶ Market order stream active." : "■ Market order stream paused.", "success-fill");
    } catch (err) {
        // Rollback if request failed
        syncStreamUI(!targetState);
        showFeedback(`Stream toggle failed: ${err.message}`, "error");
    }
}

async function setStreamRate(rate) {
    currentSimRate = rate;
    document.querySelectorAll(".rate-chip").forEach(el => {
        if (el.textContent.includes(`${rate / 1000}k`) || el.textContent === `${rate}/s`) {
            el.className = "rate-chip active";
        } else {
            el.className = "rate-chip";
        }
    });

    try {
        await fetch("/api/simulator/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ active: simActive, rate: rate }),
        });
        showFeedback(`Stream rate set to ${rate.toLocaleString()} orders/sec.`, "success-resting");
    } catch (err) {
        console.error("Set rate error:", err);
    }
}

async function fireBurst(count) {
    try {
        showFeedback(`Injecting burst of ${count.toLocaleString()} orders...`, "success-resting");
        const resp = await fetch("/api/burst", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ count }),
        });
        const res = await resp.json();
        showFeedback(`✓ Injected ${res.orders_fired.toLocaleString()} orders in ${res.elapsed_seconds}s (${res.write_throughput.toLocaleString()} ord/s)`, "success-fill");
    } catch (err) {
        showFeedback(`Burst error: ${err.message}`, "error");
    }
}

async function resetEngine() {
    try {
        await fetch("/api/reset", { method: "POST" });
        syncStreamUI(false);
        showFeedback("✓ Order book, IPC buffer & telemetry reset.", "success-fill");
    } catch (err) {
        showFeedback(`Reset error: ${err.message}`, "error");
    }
}


// ==========================================================================
// 4. ORDER BOOK (DOM) & CLICK-TO-TRADE
// ==========================================================================

function setBookFilter(filter) {
    currentBookFilter = filter;
    document.getElementById("viewBoth").className = filter === "both" ? "tab-chip active" : "tab-chip";
    document.getElementById("viewBids").className = filter === "bids" ? "tab-chip active" : "tab-chip";
    document.getElementById("viewAsks").className = filter === "asks" ? "tab-chip active" : "tab-chip";

    asksContainer.style.display = (filter === "both" || filter === "asks") ? "flex" : "none";
    bidsContainer.style.display = (filter === "both" || filter === "bids") ? "flex" : "none";
}

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
        asksContainer.innerHTML = `<div class="empty-state">No asks resting in book</div>`;
    } else {
        asksContainer.innerHTML = reversedAsks.map(r => {
            const widthPct = Math.min(100, (r.total / maxVolume) * 100);
            return `
                <div class="ladder-row" onclick="choosePrice(${r.price}, ${r.qty})" title="Click to trade @ $${r.price.toFixed(2)}">
                    <div class="row-bar" style="width: ${widthPct}%"></div>
                    <span class="cell-price">$${r.price.toFixed(2)}</span>
                    <span class="cell-size">${r.qty.toLocaleString()}</span>
                    <span class="cell-total">${r.total.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }

    // Bids Ladder: Highest bid (best bid) at top
    if (bidRows.length === 0) {
        bidsContainer.innerHTML = `<div class="empty-state">No bids resting in book</div>`;
    } else {
        bidsContainer.innerHTML = bidRows.map(r => {
            const widthPct = Math.min(100, (r.total / maxVolume) * 100);
            return `
                <div class="ladder-row" onclick="choosePrice(${r.price}, ${r.qty})" title="Click to trade @ $${r.price.toFixed(2)}">
                    <div class="row-bar" style="width: ${widthPct}%"></div>
                    <span class="cell-price">$${r.price.toFixed(2)}</span>
                    <span class="cell-size">${r.qty.toLocaleString()}</span>
                    <span class="cell-total">${r.total.toLocaleString()}</span>
                </div>
            `;
        }).join("");
    }
}

/**
 * Click-to-Trade: Snaps clicked price into ticket instantly without typing!
 */
function choosePrice(price, qty) {
    orderPrice.value = price.toFixed(2);
    updateEstimate();
    showFeedback(`Selected price $${price.toFixed(2)} from Order Book.`, "success-resting");
}


// ==========================================================================
// 5. INTERACTIVE ORDER TICKET
// ==========================================================================

function setSide(side) {
    currentSide = side;
    if (side === "BUY") {
        btnSideBuy.className = "side-btn buy active";
        btnSideSell.className = "side-btn sell";
        btnSubmitOrder.className = "btn btn-submit-order buy";
    } else {
        btnSideBuy.className = "side-btn buy";
        btnSideSell.className = "side-btn sell active";
        btnSubmitOrder.className = "btn btn-submit-order sell";
    }
    updateEstimate();
}

function setOrderType(type) {
    currentOrderType = type;
    if (type === "LIMIT") {
        btnTypeLimit.className = "type-btn active";
        btnTypeMarket.className = "type-btn";
        priceGroup.style.display = "flex";
    } else {
        btnTypeLimit.className = "type-btn";
        btnTypeMarket.className = "type-btn active";
        priceGroup.style.display = "none";
    }
    updateEstimate();
}

function stepPrice(delta) {
    let cur = parseFloat(orderPrice.value) || 150.0;
    cur = Math.max(0.01, cur + delta);
    orderPrice.value = cur.toFixed(2);
    updateEstimate();
}

function snapPrice(target) {
    if (!lastTelemetry) return;
    let price = 150.0;
    if (target === "bid" && lastTelemetry.best_bid) {
        price = lastTelemetry.best_bid;
    } else if (target === "ask" && lastTelemetry.best_ask) {
        price = lastTelemetry.best_ask;
    } else if (target === "mid" && lastTelemetry.mid_price) {
        price = lastTelemetry.mid_price;
    }
    orderPrice.value = price.toFixed(2);
    updateEstimate();
    showFeedback(`Snapped price to ${target.toUpperCase()}: $${price.toFixed(2)}`, "success-resting");
}

function stepQty(delta) {
    const cur = parseInt(orderQty.value, 10) || 100;
    orderQty.value = Math.max(1, cur + delta);
    updateEstimate();
}

function setQty(qty) {
    orderQty.value = qty;
    updateEstimate();
}

function updateEstimate() {
    const qty = parseInt(orderQty.value, 10) || 0;
    let price = parseFloat(orderPrice.value) || 150.0;

    if (currentOrderType === "MARKET" && lastTelemetry && lastTelemetry.mid_price) {
        price = lastTelemetry.mid_price;
    }

    const totalVal = price * qty;
    orderEstValue.textContent = `$${totalVal.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

    const actionText = currentSide === "BUY" ? "BUY" : "SELL";
    const typeText = currentOrderType === "LIMIT" ? "LIMIT" : "MARKET";
    btnSubmitOrder.textContent = `PLACE ${actionText} ${typeText} ($${totalVal.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })})`;
}

async function submitOrder() {
    const qty = parseInt(orderQty.value, 10);
    let price = parseFloat(orderPrice.value);

    if (isNaN(qty) || qty <= 0) {
        showFeedback("Please enter a valid share quantity (e.g. 100)", "error");
        return;
    }

    if (currentOrderType === "LIMIT" && (isNaN(price) || price <= 0)) {
        showFeedback("Please select a valid limit price (e.g. 150.25)", "error");
        return;
    }

    if (currentOrderType === "MARKET") {
        price = currentSide === "BUY" ? 99999.0 : 0.01;
    }

    showFeedback(`Routing ${currentSide} ${qty} shs @ $${price.toFixed(2)}...`, "success-resting");

    try {
        const resp = await fetch("/api/order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                side: currentSide,
                price: price,
                qty: qty,
                order_type: currentOrderType,
            }),
        });

        const res = await resp.json();
        if (res.success) {
            if (res.status === "FILLED") {
                showFeedback(`✓ ${res.message}`, "success-fill");
            } else {
                showFeedback(`✓ ${res.message}`, "success-resting");
            }
        } else {
            showFeedback(`Order error: ${res.message || "Failed"}`, "error");
        }
    } catch (err) {
        showFeedback(`Order submission failed: ${err.message}`, "error");
    }
}

function showFeedback(msg, styleClass) {
    orderFeedback.className = `feedback-banner ${styleClass || ""}`;
    orderFeedback.textContent = msg;
}

function renderUserOrders(orders) {
    if (!orders || orders.length === 0) {
        userOrdersList.innerHTML = `<div class="history-empty">No manual orders placed yet.</div>`;
        return;
    }

    userOrdersList.innerHTML = orders.map(o => {
        const isBuy = o.side === "BUY";
        const statusLower = (o.status || "").toLowerCase();
        const canCancel = o.status === "RESTING" || o.status === "PARTIALLY_FILLED";
        const cancelBtn = canCancel
            ? `<button type="button" class="btn-cancel-order" onclick="cancelOrder(${o.order_id})" title="Cancel Order #${o.order_id}">✕ Cancel</button>`
            : "";

        return `
            <div class="history-item">
                <div class="hist-left">
                    <span class="hist-oid">#${o.order_id}</span>
                    <span class="hist-side ${isBuy ? 'buy' : 'sell'}">${o.side}</span>
                    <span class="hist-price">$${o.price.toFixed(2)}</span>
                    <span class="hist-qty">(${o.qty.toLocaleString()} shs)</span>
                </div>
                <div class="hist-right">
                    <span class="hist-status ${statusLower}">${o.status}</span>
                    ${cancelBtn}
                </div>
            </div>
        `;
    }).join("");
}

async function cancelOrder(orderId) {
    showFeedback(`Cancelling order #${orderId}...`, "success-resting");
    try {
        const resp = await fetch("/api/order/cancel", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ order_id: orderId }),
        });
        const res = await resp.json();
        if (res.success) {
            showFeedback(`✓ Order #${orderId} cancelled`, "success-fill");
        } else {
            showFeedback(`Cancel failed: ${res.message || "Order not active"}`, "error");
        }
    } catch (err) {
        showFeedback(`Cancel failed: ${err.message}`, "error");
    }
}


// ==========================================================================
// 6. CENTER TABS & VISUALIZERS
// ==========================================================================

function switchMainTab(tab) {
    activeMainTab = tab;
    tabDepth.className = tab === "depth" ? "seg-tab active" : "seg-tab";
    tabTape.className = tab === "tape" ? "seg-tab active" : "seg-tab";
    tabLatency.className = tab === "latency" ? "seg-tab active" : "seg-tab";

    paneDepth.className = tab === "depth" ? "tab-content active" : "tab-content";
    paneTape.className = tab === "tape" ? "tab-content active" : "tab-content";
    paneLatency.className = tab === "latency" ? "tab-content active" : "tab-content";

    if (lastTelemetry) {
        if (tab === "depth") drawDepthChart(lastTelemetry.bids, lastTelemetry.asks);
        else if (tab === "tape") renderTradeTape(lastTelemetry.recent_trades);
        else if (tab === "latency") drawLatencyHistogram(lastTelemetry.latency);
    }
}

function drawDepthChart(bids, asks) {
    if (!depthCtx) return;
    const w = depthCanvas.width;
    const h = depthCanvas.height;

    depthCtx.clearRect(0, 0, w, h);

    // Subtle background grid lines
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
        depthCtx.fillText("Waiting for order depth data...", w / 2, h / 2);
        return;
    }

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

    // Green Cumulative Bids (Left Polygon)
    if (cumBids.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 25);

        for (let i = 0; i < cumBids.length; i++) {
            const x = midX - ((i + 1) / cumBids.length) * (midX - 25);
            const y = (h - 25) - (cumBids[i].cum / maxVol) * (h - 70);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX - (midX - 25);
        depthCtx.lineTo(lastX, h - 25);
        depthCtx.closePath();

        const bidGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        bidGrad.addColorStop(0, "rgba(0, 230, 118, 0.35)");
        bidGrad.addColorStop(1, "rgba(0, 230, 118, 0.02)");
        depthCtx.fillStyle = bidGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#00e676";
        depthCtx.lineWidth = 2;
        depthCtx.stroke();
    }

    // Red Cumulative Asks (Right Polygon)
    if (cumAsks.length > 0) {
        depthCtx.beginPath();
        depthCtx.moveTo(midX, h - 25);

        for (let i = 0; i < cumAsks.length; i++) {
            const x = midX + ((i + 1) / cumAsks.length) * (midX - 25);
            const y = (h - 25) - (cumAsks[i].cum / maxVol) * (h - 70);
            depthCtx.lineTo(x, y);
        }

        const lastX = midX + (midX - 25);
        depthCtx.lineTo(lastX, h - 25);
        depthCtx.closePath();

        const askGrad = depthCtx.createLinearGradient(0, 0, 0, h);
        askGrad.addColorStop(0, "rgba(255, 51, 102, 0.35)");
        askGrad.addColorStop(1, "rgba(255, 51, 102, 0.02)");
        depthCtx.fillStyle = askGrad;
        depthCtx.fill();

        depthCtx.strokeStyle = "#ff3366";
        depthCtx.lineWidth = 2;
        depthCtx.stroke();
    }

    // Dashed Mid-Price Divider
    depthCtx.strokeStyle = "rgba(255, 255, 255, 0.2)";
    depthCtx.setLineDash([4, 4]);
    depthCtx.beginPath();
    depthCtx.moveTo(midX, 20);
    depthCtx.lineTo(midX, h - 25);
    depthCtx.stroke();
    depthCtx.setLineDash([]);

    // Text labels
    depthCtx.fillStyle = "#94a3b8";
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
    const barWidth = 52;
    const gap = (w - (labels.length * barWidth)) / (labels.length + 1);

    for (let i = 0; i < labels.length; i++) {
        const x = gap + i * (barWidth + gap);
        const barHeight = Math.max(8, (values[i] / maxVal) * (h - 75));
        const y = h - 35 - barHeight;

        // Clean bars
        latencyCtx.fillStyle = i <= 1 ? "rgba(56, 189, 248, 0.4)" : (i <= 4 ? "rgba(0, 230, 118, 0.4)" : "rgba(251, 191, 36, 0.4)");
        latencyCtx.fillRect(x, y, barWidth, barHeight);

        latencyCtx.strokeStyle = i <= 1 ? "#38bdf8" : (i <= 4 ? "#00e676" : "#fbbf24");
        latencyCtx.lineWidth = 1.5;
        latencyCtx.strokeRect(x, y, barWidth, barHeight);

        // Value text
        latencyCtx.fillStyle = "#f8fafc";
        latencyCtx.font = "11px JetBrains Mono, monospace";
        latencyCtx.textAlign = "center";
        latencyCtx.fillText(`${values[i].toFixed(2)}μs`, x + barWidth / 2, y - 6);

        // Label text
        latencyCtx.fillStyle = "#94a3b8";
        latencyCtx.font = "11px Inter, sans-serif";
        latencyCtx.fillText(labels[i], x + barWidth / 2, h - 14);
    }
}

function renderTradeTape(trades) {
    if (!trades || trades.length === 0) {
        tapeBody.innerHTML = `<div class="tape-placeholder">No fills yet. Fire an order burst to see real-time trade tape.</div>`;
        return;
    }

    tapeBody.innerHTML = trades.slice(0, 30).map(t => {
        const d = new Date(t.timestamp_ns / 1_000_000);
        const timeStr = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}.${(t.timestamp_ns % 1_000_000_000 / 1_000_000).toFixed(0).padStart(3, '0')}`;
        const isBuy = t.trade_id % 2 === 0;

        return `
            <div class="tape-item">
                <span class="tape-time">${timeStr}</span>
                <span class="${isBuy ? 'tape-badge-buy' : 'tape-badge-sell'}">${isBuy ? 'BUY' : 'SELL'}</span>
                <span class="tape-price">$${t.price.toFixed(2)}</span>
                <span class="tape-qty">${t.qty.toLocaleString()}</span>
                <span class="tape-oid">#${t.maker_order_id}</span>
                <span class="tape-oid">#${t.taker_order_id}</span>
            </div>
        `;
    }).join("");
}


// ==========================================================================
// 7. INITIALIZATION
// ==========================================================================
window.addEventListener("DOMContentLoaded", () => {
    setSide("BUY");
    setOrderType("LIMIT");
    updateEstimate();
    syncStreamUI(false);
    connectWebSocket();
});
