import asyncio, time
from contextlib import asynccontextmanager
from typing import Set
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from app.config import settings
from app.database import PredictionLedger
from app.feeds import MarketDataHub, iso_now
from app.models import DashboardSnapshot
from app.orchestrator import PredictorOrchestrator

hub = MarketDataHub()
ledger = PredictionLedger(settings.db_path)
orchestrator = PredictorOrchestrator(hub, ledger)
active_clients: Set[WebSocket] = set()
START_TIME = time.time()

def broadcast_snapshot(snap: DashboardSnapshot) -> None:
    if not active_clients: return
    payload = snap.model_dump_json()
    for ws in list(active_clients):
        asyncio.create_task(_safe_send(ws, payload))

async def _safe_send(ws: WebSocket, payload: str) -> None:
    try: await ws.send_text(payload)
    except Exception: active_clients.discard(ws)

@asynccontextmanager
async def lifespan(app: FastAPI):
    orchestrator.register_listener(broadcast_snapshot)
    await hub.start()
    orch_task = asyncio.create_task(orchestrator.run())
    yield
    orchestrator.stop()
    orch_task.cancel()
    await hub.stop()

app = FastAPI(title="SATSIGNAL BTC 15M API", version="2.0.0", lifespan=lifespan)

@app.get("/api/health")
async def get_health():
    return {"status": "ok", "uptime_seconds": round(time.time() - START_TIME, 1), "server_time_iso": iso_now(), "trading_mode": settings.trading_mode, "has_live_snapshot": orchestrator.latest_snapshot is not None}


@app.get("/api/orchestrator/health")
async def get_orchestrator_health():
    return orchestrator.get_runtime_health()

@app.get("/api/state")
async def get_state():
    if orchestrator.latest_snapshot is None:
        return {"status": "warming_up", "server_time_iso": iso_now(), "feeds": [f.model_dump() for f in hub.get_feed_statuses()], "scoreboard": ledger.get_scoreboard().model_dump()}
    return orchestrator.latest_snapshot.model_dump()

@app.get("/api/prediction")
async def get_prediction():
    if orchestrator.latest_snapshot is None: return {"status": "warming_up", "signal": "WAIT", "confidence": 0.0}
    s = orchestrator.latest_snapshot
    return {"contract_ticker": s.contract.ticker, "seconds_remaining": s.contract.seconds_remaining, "checkpoint_seconds_remaining": s.contract.checkpoint_seconds_remaining, "main_signal": s.evidence.signal, "early_signal": s.early.signal, "final_signal": s.final.signal, "confidence": s.final.confidence, "up_probability": s.final.up_probability, "down_probability": s.final.down_probability, "regime": s.regime.regime, "agreement_status": s.final.agreement_status, "rationale": s.final.rationale}

@app.get("/api/engines")
async def get_engines():
    if orchestrator.latest_snapshot is None: return {"status": "warming_up"}
    s = orchestrator.latest_snapshot
    return {"summary": s.engines.model_dump(), "evidence": s.evidence.model_dump(), "weakness": s.weakness.model_dump(), "regime": s.regime.model_dump(), "early": s.early.model_dump(), "final": s.final.model_dump()}

@app.get("/api/market")
async def get_market():
    if orchestrator.latest_snapshot is None: return {"status": "warming_up"}
    s = orchestrator.latest_snapshot
    return {"contract": s.contract.model_dump(), "live_price": s.evidence.live_price, "strike_price": s.contract.strike_price, "distance_from_strike": s.evidence.distance_from_strike, "buying_pressure_pct": s.evidence.buying_pressure_pct, "selling_pressure_pct": s.evidence.selling_pressure_pct, "trade_delta_60s": s.evidence.trade_delta_60s, "cvd_contract": s.evidence.cvd_contract, "orderbook_imbalance": s.evidence.orderbook_imbalance, "rsi_14": s.evidence.rsi_14, "momentum_score": s.evidence.momentum_score, "volatility_annualized": s.evidence.volatility_annualized}

@app.get("/api/feeds")
async def get_feeds():
    return {"feeds": [f.model_dump() for f in hub.get_feed_statuses()]}

@app.get("/api/multi-exchange")
async def get_multi_exchange():
    snapshot = hub.get_multi_exchange_snapshot()
    contract = orchestrator.active_contract
    snapshot["strike_price"] = contract.strike_price if contract else None
    reference_price = snapshot.get("reference_price")
    snapshot["reference_vs_strike_bps"] = (
        round((float(reference_price) - float(contract.strike_price)) / float(contract.strike_price) * 10000.0, 4)
        if reference_price is not None and contract and contract.strike_price > 0 else None
    )
    return snapshot

@app.get("/api/multi-exchange/history")
async def get_multi_exchange_history(limit: int = 200):
    return {"observations": ledger.get_multi_exchange_reference_history(limit=limit)}

@app.get("/api/history")
async def get_history(limit: int = 50):
    return {"predictions": [p.model_dump() for p in ledger.get_recent_predictions(limit=limit)]}

@app.get("/api/scoreboard")
async def get_scoreboard():
    return ledger.get_scoreboard().model_dump()

@app.get("/api/trades")
async def get_trades(limit: int = 30):
    return {"trades": [t.model_dump() for t in ledger.get_recent_trades(limit=limit)]}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_clients.add(websocket)
    if orchestrator.latest_snapshot is not None:
        await websocket.send_text(orchestrator.latest_snapshot.model_dump_json())
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(websocket)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0" />
<title>SATSIGNAL BTC 15M</title>
<style>
  :root { --bg:#0b0f17; --panel:#131a26; --border:#1f2a3d; --text:#e6edf7; --muted:#8b9bb4; --up:#10b981; --up-bg:rgba(16,185,129,.14); --down:#ef4444; --down-bg:rgba(239,68,68,.14); --wait:#f59e0b; --wait-bg:rgba(245,158,11,.14); }
  * { box-sizing:border-box; margin:0; padding:0; font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",sans-serif; }
  body { background:radial-gradient(ellipse at 15% 0%,#172338 0%,var(--bg) 42%); color:var(--text); padding:14px; max-width:1600px; margin:0 auto; }
  .header { position:sticky; top:8px; z-index:20; backdrop-filter:blur(12px); display:flex; flex-wrap:wrap; justify-content:space-between; align-items:center; gap:10px; padding:14px 16px; background:var(--panel); border:1px solid var(--border); border-radius:10px; margin-bottom:12px; }
  .title { font-size:1.1rem; font-weight:800; } .sub { font-size:.78rem; color:var(--muted); margin-top:2px; }
  .timers { display:flex; gap:16px; align-items:center; } .timer-box { text-align:right; }
  .big-val { font-size:1.65rem; font-weight:800; font-variant-numeric:tabular-nums; }
  .cp-val { font-size:1.15rem; font-weight:700; color:#38bdf8; font-variant-numeric:tabular-nums; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(290px,1fr)); gap:12px; margin-bottom:12px; }
  .card { background:linear-gradient(155deg,rgba(24,33,49,.98),rgba(17,24,37,.98)); border:1px solid var(--border); border-radius:12px; padding:16px; box-shadow:0 8px 24px rgba(0,0,0,.16); transition:border-color .18s ease,transform .18s ease; } .card:hover { border-color:#344761; }
  .card h3 { font-size:.75rem; text-transform:uppercase; letter-spacing:.8px; color:var(--muted); margin-bottom:10px; display:flex; justify-content:space-between; }
  .badge { display:inline-block; padding:5px 12px; border-radius:6px; font-weight:800; font-size:.92rem; }
  .sig-UP { color:var(--up); background:var(--up-bg); border:1px solid var(--up); }
  .sig-DOWN { color:var(--down); background:var(--down-bg); border:1px solid var(--down); }
  .sig-WAIT { color:var(--wait); background:var(--wait-bg); border:1px solid var(--wait); }
  .bar-wrap { margin:8px 0; } .bar-label { display:flex; justify-content:space-between; font-size:.82rem; margin-bottom:4px; }
  .bar-track { height:10px; background:#1e293b; border-radius:99px; overflow:hidden; }
  .bar-fill-up { height:100%; background:var(--up); transition:width .25s ease; }
  .bar-fill-down { height:100%; background:var(--down); transition:width .25s ease; }
  .kv { display:flex; justify-content:space-between; padding:5px 0; border-bottom:1px solid rgba(255,255,255,.04); font-size:.84rem; gap:8px; }
  .kv:last-child { border-bottom:none; } .kv span:first-child { color:var(--muted); } .kv span:last-child { font-weight:600; text-align:right; }
  .feed-dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; }
  .st-LIVE,.st-ACTIVE,.st-ACTIVE_WINDOW { background:var(--up); } .st-FALLBACK,.st-MONITORING { background:var(--wait); } .st-STALE,.st-UNAVAILABLE,.st-DEGRADED_FEED { background:var(--down); }
  table { width:100%; border-collapse:separate; border-spacing:0; font-size:.8rem; } th,td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--border); white-space:nowrap; } th { position:sticky; top:0; z-index:2; background:#182234; color:#b9c8dc; font-size:.74rem; text-transform:uppercase; letter-spacing:.04em; } tbody tr:hover { background:rgba(56,189,248,.055); }
  .table-wrap { overflow-x:auto; } .rationale { margin-top:10px; font-size:.82rem; color:#cbd5e1; line-height:1.4; } a.api-link { color:#38bdf8; text-decoration:none; font-weight:600; }
</style>
</head>
<body>
  <div class="header">
    <div><div class="title">SATSIGNAL BTC 15M</div><div class="sub" id="contract-meta">Connecting... | <a class="api-link" href="/docs" target="_blank">API Docs (/docs)</a></div></div>
    <div class="timers"><div class="timer-box"><div class="cp-val" id="cp-countdown">--:--</div><div class="sub">Checkpoint (3m)</div></div><div class="timer-box"><div class="big-val" id="countdown">--:--</div><div class="sub" id="server-time">Settlement</div></div></div>
  </div>
  <div class="grid">
    <div class="card">
      <h3>Final Decision (Engine E) <span id="ws-status" style="color:var(--wait)">CONNECTING</span></h3>
      <div style="display:flex;align-items:center;justify-content:space-between;"><span id="final-badge" class="badge sig-WAIT">WAIT</span><span class="big-val" id="final-conf">50.0%</span></div>
      <div class="bar-wrap" style="margin-top:12px;"><div class="bar-label"><span style="color:var(--up)">UP Probability</span><span id="up-prob">50.0%</span></div><div class="bar-track"><div id="up-bar" class="bar-fill-up" style="width:50%"></div></div></div>
      <div class="bar-wrap"><div class="bar-label"><span style="color:var(--down)">DOWN Probability</span><span id="down-prob">50.0%</span></div><div class="bar-track"><div id="down-bar" class="bar-fill-down" style="width:50%"></div></div></div>
      <div class="rationale" id="final-rationale">Awaiting initial live market ticks...</div>
    </div>
    <div class="card">
      <h3>Contract & Price Telemetry</h3>
      <div class="kv"><span>Live BTC Price</span><span id="live-price">--</span></div>
      <div class="kv"><span>Official Strike Price</span><span id="strike-price">--</span></div>
      <div class="kv"><span>Strike Source</span><span id="strike-src">--</span></div>
      <div class="kv"><span>Distance vs Strike</span><span id="strike-dist">--</span></div>
      <div class="kv"><span>Market Regime</span><span id="regime-val">--</span></div>
      <div class="kv"><span>Engine Agreement</span><span id="agreement-val">--</span></div>
    </div>
    <div class="card">
      <h3>Five Specialized Intelligence Engines</h3>
      <div class="kv"><span><i class="feed-dot st-ACTIVE"></i>Engine A (Evidence)</span><span id="eng-a">--</span></div>
      <div class="kv"><span><i id="dot-b" class="feed-dot st-ACTIVE"></i>Engine B (Weakness)</span><span id="eng-b">--</span></div>
      <div class="kv"><span><i class="feed-dot st-ACTIVE"></i>Engine C (Regime)</span><span id="eng-c">--</span></div>
      <div class="kv"><span><i class="feed-dot st-ACTIVE"></i>Engine D (Early)</span><span id="eng-d">--</span></div>
      <div class="kv"><span><i class="feed-dot st-ACTIVE"></i>Engine E (Final)</span><span id="eng-e">--</span></div>
      <div class="rationale" id="engine-notes"></div>
    </div>
    <div class="card">
      <h3>Order Flow & Microstructure</h3>
      <div class="bar-wrap"><div class="bar-label"><span style="color:var(--up)">Buying Pressure</span><span id="buy-pct">50%</span></div><div class="bar-track"><div id="buy-bar" class="bar-fill-up" style="width:50%"></div></div></div>
      <div class="bar-wrap"><div class="bar-label"><span style="color:var(--down)">Selling Pressure</span><span id="sell-pct">50%</span></div><div class="bar-track"><div id="sell-bar" class="bar-fill-down" style="width:50%"></div></div></div>
      <div class="kv"><span>60s Delta / Contract CVD</span><span id="delta-cvd">--</span></div>
      <div class="kv"><span>Order-Book Imbalance</span><span id="obi-val">--</span></div>
      <div class="kv"><span>Momentum / RSI(14)</span><span id="mom-rsi">--</span></div>
      <div class="kv"><span>Annualized Volatility</span><span id="vol-val">--</span></div>
    </div>
  </div>
  <div class="grid">
    <div class="card">
      <h3>Performance Scoreboard & Paper Portfolio</h3>
      <div class="kv"><span>Execution Mode</span><span id="sb-mode">PAPER</span></div>
      <div class="kv"><span>Bankroll / Realized P&L</span><span id="sb-pnl">$1,000.00 ($0.00)</span></div>
      <div class="kv"><span>Predictions / Pending</span><span id="sb-total">0 / 0</span></div>
      <div class="kv"><span>Final Engine (W / L / Accuracy)</span><span id="sb-final">0 / 0 / --</span></div>
      <div class="kv"><span>Main Engine (W / L / Accuracy)</span><span id="sb-main">0 / 0 / --</span></div>
      <div class="kv"><span>Early Engine (W / L / Accuracy)</span><span id="sb-early">0 / 0 / --</span></div>
      <div class="kv"><span>Brier Calibration Score</span><span id="sb-brier">Awaiting verified settlements</span></div>
      <div class="kv"><span>Protective WAITs</span><span id="sb-waits">0</span></div>
    </div>
    <div class="card"><h3>Live Data Feed Health</h3><div id="feeds-list"></div></div>
    <div class="card">
      <h3>Multi-Exchange Reference <span id="mx-status" style="color:var(--wait)">SHADOW / WAITING</span></h3>
      <div class="kv"><span>Consolidated USD Reference</span><span id="mx-price">Waiting for 2 USD venues</span></div>
      <div class="kv"><span>Contributing Venues</span><span id="mx-venues">--</span></div>
      <div class="kv"><span>Bid Liquidity (±0.25%)</span><span id="mx-bid-depth">--</span></div>
      <div class="kv"><span>Ask Liquidity (±0.25%)</span><span id="mx-ask-depth">--</span></div>
      <div class="kv"><span>Consolidated Imbalance</span><span id="mx-imbalance">--</span></div>
      <div class="kv"><span>Cross-Venue Aggressor Delta (15s / 60s)</span><span id="mx-flow-delta">--</span></div>
      <div class="kv"><span>60s Buy / Sell Volume (BTC)</span><span id="mx-flow-volume">--</span></div>
      <div class="kv"><span>60s Flow Sources</span><span id="mx-flow-sources">--</span></div>
      <div class="kv"><span>Reference vs Primary Spot</span><span id="mx-primary-diff">--</span></div>
      <div class="kv"><span>Reference vs Official BRTI</span><span id="mx-brti-diff">--</span></div>
      <div class="kv"><span>Reference vs Contract Strike</span><span id="mx-strike-diff">--</span></div>
      <div class="rationale" id="mx-method">Independent reference. Does not influence predictions.</div>
    </div>

  </div>
  <div class="card" style="margin-bottom:12px;"><h3>Paper Execution Ledger</h3><div class="table-wrap"><table><thead><tr><th>Time</th><th>Mode</th><th>Contract</th><th>Side</th><th>Qty</th><th>Entry</th><th>Quote Source</th><th>Fee</th><th>Edge</th><th>Status</th><th>P&L</th></tr></thead><tbody id="trades-body"></tbody></table></div></div>
  <div class="card"><h3>Persistent Prediction History</h3><div class="table-wrap"><table><thead><tr><th>Time (UTC)</th><th>Contract</th><th>Strike</th><th>BTC @ Pred</th><th>Rem</th><th>Main</th><th>Early</th><th>Final</th><th>Prob</th><th>Outcome</th><th>Source</th><th>Result</th></tr></thead><tbody id="history-body"></tbody></table></div></div>
<script>
  const fmtUSD = (v) => v == null ? "--" : "$" + Number(v).toLocaleString("en-US", {minimumFractionDigits:2, maximumFractionDigits:2});
  const sigBadge = (s) => `<span class="badge sig-${s}" style="padding:2px 7px;font-size:.74rem;">${s}</span>`;
  const fmtClock = (sec) => { const s = Math.max(0, Math.floor(sec || 0)); return `${String(Math.floor(s/60)).padStart(2,"0")}:${String(s%60).padStart(2,"0")}`; };
  // Countdown is tied to the contract's absolute close timestamp, not a repeatedly
  // rounded snapshot value. Estimate server/client clock offset on every update.
  let contractCloseEpochMs = null;
  let contractOpenEpochMs = null;
  let serverClockOffsetMs = 0;
  const CHECKPOINT_SECONDS = 180;
  function tickTimers() {
    if (contractCloseEpochMs === null || contractOpenEpochMs === null) return;
    const serverNowMs = Date.now() + serverClockOffsetMs;
    const remaining = Math.max(0, (contractCloseEpochMs - serverNowMs) / 1000);
    const elapsed = Math.max(0, (serverNowMs - contractOpenEpochMs) / 1000);
    const checkpointElapsed = elapsed % CHECKPOINT_SECONDS;
    const checkpointRemaining = checkpointElapsed < 0.5
      ? CHECKPOINT_SECONDS : CHECKPOINT_SECONDS - checkpointElapsed;
    document.getElementById("countdown").textContent = fmtClock(remaining);
    document.getElementById("cp-countdown").textContent = fmtClock(checkpointRemaining);
  }
  function updateUI(d) {
    if (!d || !d.contract) return;
    document.getElementById("server-time").textContent = d.server_time_iso;
    contractCloseEpochMs = Number(d.contract.close_timestamp) * 1000;
    contractOpenEpochMs = Number(d.contract.open_timestamp) * 1000;
    if (d.server_time_iso) {
      const parsedServerTime = Date.parse(d.server_time_iso.replace(" UTC", "Z").replace(" ", "T"));
      if (Number.isFinite(parsedServerTime)) serverClockOffsetMs = parsedServerTime - Date.now();
    }
    document.getElementById("contract-meta").innerHTML = `Contract: <b>${d.contract.ticker}</b> | Kalshi Official: ${d.contract.is_official_kalshi ? "YES" : "NO (UTC Proxy)"} | <a class="api-link" href="/docs" target="_blank">API Docs</a>`;
    tickTimers();
    const fb = document.getElementById("final-badge"); fb.textContent = d.final.signal; fb.className = `badge sig-${d.final.signal}`;
    document.getElementById("final-conf").textContent = `${d.final.confidence.toFixed(1)}%`;
    const upP = (d.final.up_probability*100).toFixed(1), dnP = (d.final.down_probability*100).toFixed(1);
    document.getElementById("up-prob").textContent = `${upP}%`; document.getElementById("down-prob").textContent = `${dnP}%`;
    document.getElementById("up-bar").style.width = `${upP}%`; document.getElementById("down-bar").style.width = `${dnP}%`;
    document.getElementById("final-rationale").textContent = d.final.rationale;
    document.getElementById("live-price").textContent = fmtUSD(d.evidence.live_price);
    document.getElementById("strike-price").textContent = fmtUSD(d.contract.strike_price);
    document.getElementById("strike-src").textContent = d.contract.strike_source;
    document.getElementById("strike-dist").textContent = `${d.evidence.distance_from_strike>=0?"+":""}${d.evidence.distance_from_strike.toFixed(2)} (${d.evidence.distance_pct.toFixed(3)}%)`;
    document.getElementById("regime-val").textContent = d.regime.regime;
    document.getElementById("agreement-val").textContent = d.final.agreement_status;
    document.getElementById("eng-a").innerHTML = `${sigBadge(d.evidence.signal)} Score ${d.evidence.raw_score}`;
    document.getElementById("eng-b").textContent = `Weakness ${d.weakness.weakness_score}/100`;
    document.getElementById("dot-b").className = `feed-dot st-${d.engines.engine_b_weakness.status}`;
    document.getElementById("eng-c").textContent = `${d.regime.regime} (>=${d.regime.min_confidence_required}%)`;
    document.getElementById("eng-d").innerHTML = `${sigBadge(d.early.signal)} ${d.early.confidence}%`;
    document.getElementById("eng-e").innerHTML = `${sigBadge(d.final.signal)} ${d.final.confidence}%`;
    document.getElementById("engine-notes").textContent = d.final.agreement_details.join(" • ");
    document.getElementById("buy-pct").textContent = `${d.evidence.buying_pressure_pct}%`; document.getElementById("sell-pct").textContent = `${d.evidence.selling_pressure_pct}%`;
    document.getElementById("buy-bar").style.width = `${d.evidence.buying_pressure_pct}%`; document.getElementById("sell-bar").style.width = `${d.evidence.selling_pressure_pct}%`;
    document.getElementById("delta-cvd").textContent = `${d.evidence.trade_delta_60s>=0?"+":""}${d.evidence.trade_delta_60s} / ${d.evidence.cvd_contract>=0?"+":""}${d.evidence.cvd_contract} BTC`;
    document.getElementById("obi-val").textContent = d.evidence.orderbook_imbalance.toFixed(3);
    document.getElementById("mom-rsi").textContent = `${d.evidence.momentum_score} / RSI ${d.evidence.rsi_14}`;
    document.getElementById("vol-val").textContent = `${(d.evidence.volatility_annualized*100).toFixed(1)}%`;
    document.getElementById("sb-mode").textContent = d.scoreboard.trading_mode.toUpperCase();
    const pnlSign = d.scoreboard.realized_pnl_usd >= 0 ? "+" : "";
    document.getElementById("sb-pnl").textContent = `${fmtUSD(d.scoreboard.bankroll_usd)} (${pnlSign}${fmtUSD(d.scoreboard.realized_pnl_usd)})`;
    document.getElementById("sb-total").textContent = `${d.scoreboard.total_predictions} / ${d.scoreboard.pending_predictions}`;
    document.getElementById("sb-final").textContent = `${d.scoreboard.final_wins} / ${d.scoreboard.final_losses} / ${d.scoreboard.final_accuracy_pct}% (${d.scoreboard.final_scored} scored)`;
    document.getElementById("sb-main").textContent = `${d.scoreboard.main_wins} / ${d.scoreboard.main_losses} / ${d.scoreboard.main_accuracy_pct}% (${d.scoreboard.main_scored} scored)`;
    document.getElementById("sb-early").textContent = `${d.scoreboard.early_wins} / ${d.scoreboard.early_losses} / ${d.scoreboard.early_accuracy_pct}% (${d.scoreboard.early_scored} scored)`;
    document.getElementById("sb-brier").textContent = d.scoreboard.brier_score != null ? d.scoreboard.brier_score.toFixed(4) : "Awaiting settlements";
    document.getElementById("sb-waits").textContent = d.scoreboard.waits_issued;
    document.getElementById("feeds-list").innerHTML = d.feeds.map(f => `<div class="kv"><span><i class="feed-dot st-${f.status}"></i>${f.name}</span><span>${f.status} ${f.age_seconds!=null?"("+f.age_seconds+"s)":""} — ${f.detail}</span></div>`).join("");
    document.getElementById("trades-body").innerHTML = (d.recent_trades||[]).map(t => `<tr><td>${t.timestamp_iso.split(" ")[1]}</td><td>${t.mode.toUpperCase()}</td><td>${t.contract_ticker}</td><td>${t.side}</td><td>${t.contracts}</td><td>$${t.entry_price.toFixed(2)}</td><td>${t.quote_source}</td><td>$${t.fee_paid.toFixed(2)}</td><td>+${(t.net_edge*100).toFixed(1)}c</td><td>${t.status}</td><td style="color:${t.pnl_usd>=0?'var(--up)':'var(--down)'};font-weight:700;">${t.pnl_usd>=0?'+':''}$${t.pnl_usd.toFixed(2)}</td></tr>`).join("");
    document.getElementById("history-body").innerHTML = (d.recent_predictions||[]).map(p => {
      let resText = "PENDING";
      if (p.settled) resText = p.final_won===true ? `<span style="color:var(--up);font-weight:700;">WIN</span>` : (p.final_won===false ? `<span style="color:var(--down);font-weight:700;">LOSS</span>` : `<span style="color:var(--wait);">WAIT (not scored)</span>`);
      return `<tr><td>${p.timestamp_iso.split(" ")[1]}</td><td>${p.contract_ticker}</td><td>${fmtUSD(p.strike_price)}</td><td>${fmtUSD(p.btc_price_at_pred)}</td><td>${Math.round(p.seconds_remaining)}s</td><td>${sigBadge(p.main_signal)}</td><td>${sigBadge(p.early_signal)}</td><td>${sigBadge(p.final_signal)}</td><td>${p.confidence.toFixed(1)}%</td><td>${p.actual_outcome||"--"}</td><td>${p.settlement_source||"--"}</td><td>${resText}</td></tr>`;
    }).join("");
  }
  async function fetchMultiExchange() {
    try {
      const res = await fetch("/api/multi-exchange");
      if (!res.ok) return;
      const m = await res.json();
      const statusEl = document.getElementById("mx-status");
      statusEl.textContent = m.status + " / SHADOW";
      statusEl.style.color = m.status === "LIVE" ? "var(--up)" : "var(--wait)";
      document.getElementById("mx-price").textContent = m.reference_price != null ? fmtUSD(m.reference_price) : "Waiting for 2 USD venues";
      document.getElementById("mx-venues").textContent = `${(m.included_venues||[]).join(", ")||"--"} (${m.usd_venue_count_live||0}/2 USD feeds)`;
      document.getElementById("mx-bid-depth").textContent = fmtUSD(m.consolidated_bid_depth_usd_within_25bps||0);
      document.getElementById("mx-ask-depth").textContent = fmtUSD(m.consolidated_ask_depth_usd_within_25bps||0);
      document.getElementById("mx-imbalance").textContent = m.consolidated_imbalance == null ? "--" : (m.consolidated_imbalance*100).toFixed(2)+"%";
      const flow15 = m.trade_flow?.windows?.["15"];
      const flow60 = m.trade_flow?.windows?.["60"];
      const deltaFmt = (f) => !f ? "--" : (f.status!=="ACTIVE" ? f.status : ((f.delta_btc>0?"+":"")+Number(f.delta_btc).toFixed(4)+" BTC"));
      document.getElementById("mx-flow-delta").textContent = deltaFmt(flow15)+" / "+deltaFmt(flow60)+(flow60?.status==="ACTIVE"&&flow60.delta_ratio!=null?" ("+(flow60.delta_ratio*100).toFixed(1)+"%)":"");
      document.getElementById("mx-flow-volume").textContent = flow60?.status==="ACTIVE" ? Number(flow60.buy_volume_btc||0).toFixed(4)+" / "+Number(flow60.sell_volume_btc||0).toFixed(4) : (flow60?.status||"--");
      document.getElementById("mx-flow-sources").textContent = flow60?.source_venues?.join(", ") || "--";
      document.getElementById("mx-primary-diff").textContent = m.reference_vs_primary_bps == null ? "--" : (m.reference_vs_primary_bps>0?"+":"")+m.reference_vs_primary_bps.toFixed(2)+" bps";
      document.getElementById("mx-brti-diff").textContent = m.reference_vs_brti_bps == null ? "--" : (m.reference_vs_brti_bps>0?"+":"")+m.reference_vs_brti_bps.toFixed(2)+" bps";
      document.getElementById("mx-strike-diff").textContent = m.reference_vs_strike_bps == null ? "--" : (m.reference_vs_strike_bps>0?"+":"")+m.reference_vs_strike_bps.toFixed(2)+" bps";
      document.getElementById("mx-method").textContent = m.method;
    } catch(e) {}
  }
  async function fetchInitial() { try { const res = await fetch("/api/state"); if (res.ok) updateUI(await res.json()); } catch(e){} }
  function connectWS() {
    const wsEl = document.getElementById("ws-status");
    const ws = new WebSocket(`${location.protocol==="https:"?"wss:":"ws:"}//${location.host}/ws`);
    ws.onopen = () => { wsEl.textContent = "LIVE STREAM"; wsEl.style.color = "var(--up)"; };
    ws.onmessage = (e) => updateUI(JSON.parse(e.data));
    ws.onclose = () => { wsEl.textContent = "RECONNECTING"; wsEl.style.color = "var(--wait)"; setTimeout(connectWS, 2000); };
  }
  fetchInitial(); fetchMultiExchange(); connectWS(); setInterval(tickTimers, 250); setInterval(fetchMultiExchange, 2000);
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return DASHBOARD_HTML

if __name__ == "__main__":
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, reload=False)
