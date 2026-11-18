# SATSIGNAL BTC 15M

A FastAPI monitoring and prediction service for BTC 15-minute Kalshi markets. This is a separately named project; it does not modify the legacy SATSIGNAL platform or SATBOT.

## Safety status

- This release is **paper-only**. The execution manager cannot submit demo or production orders.
- Paper entries require an active official `KXBTC15M` contract, an official strike, and a real Kalshi ask quote.
- Synthetic fallback windows are dashboard-only and cannot generate paper trades or enter the official prediction scoreboard.
- The scoreboard counts one official prediction lock per contract. Live engine snapshots are stored separately as observations and never inflate prediction totals. A directional Final Decision must agree on two consecutive one-second evaluations for the same official contract before it locks; WAIT, reversal, or contract rollover resets confirmation. If no confirmed direction exists at the 65-second final-entry cutoff, WAIT is locked. Locks are immutable and settlement grades only the lock.
- `/api/prediction-observations` returns the separate rolling observation history for analysis (limit 1,000; optional contract ticker filter).
- Prediction accuracy is based on settled official-contract locks. Pending locks are not wins or losses; WAIT signals are excluded from directional accuracy.
- Paper-trading P&L is reported separately from prediction accuracy.
- No prediction strategy is proven profitable. The probabilities are heuristic estimates, not calibrated guarantees.

## Five engines (preserved)

A. Evidence: trade flow, delta/CVD, order-book imbalance, momentum, RSI, volatility, strike distance and structure.
B. Weakness: stale feeds, absorption, low-volume drift, book/tape conflict and exhaustion.
C. Regime: stable, shaky or high-risk reversal regime.
D. Early: opening-window probability estimate.
E. Final: combines evidence, weakness, regime and early-engine agreement into UP/DOWN/WAIT.

The five-engine architecture is preserved. Reliability work hardens evidence inputs and gates: stale/missing order books degrade the forecast, short startup history is neutral for volume ratio, and a synthetic contract cannot produce an actionable final verdict.

## Local run (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:TRADING_MODE = "paper"
$env:PREDICTOR_DB_PATH = "$PWD\satsignal_btc15m.db"
.\.venv\Scripts\python.exe -m app.main
```

Open `http://localhost:8000`. API docs: `/docs`; health: `/api/health`; scoreboard: `/api/scoreboard`.

## Deployment and persistence

Build with the included Dockerfile. Set `TRADING_MODE=paper` and set `PREDICTOR_DB_PATH` to a mounted persistent volume path if the host provides one. A container's ephemeral filesystem may be wiped on restart; do not assume the scoreboard/history will survive without persistent storage. Do not activate paid storage without explicit approval.

## Feeds

- Binance aggregate trades and depth stream are primary.
- Coinbase WebSocket is fallback; its fallback order book is top-of-book only, not 20 levels.
- Kalshi polling accepts only currently active KXBTC15M markets with a valid official `floor_strike`.
- Official CF Benchmarks BRTI is accepted only from a valid, source-timestamped, fresh Kalshi pass-through response. The documented CF payload array is parsed using its source event time; stale, future-dated, malformed, and repeat-of-previous values are excluded. When fresh, BRTI contributes an independent 35% component to the Evidence Engine's strike-distance Z-score while primary spot retains 65%; a material spot/BRTI disagreement across the Kalshi strike adds a weakness penalty. BRTI is never replaced by Binance or another exchange. Missing entitlement leaves the BRTI component inactive rather than fabricating a value.

## Multi-exchange reference (shadow mode)

A read-only shadow market-data layer runs independently of the five prediction engines:

- Coinbase BTC/USD Level 2 book and matches stream.
- Kraken BTC/USD book and trades WebSocket.
- Bitstamp BTC/USD public order-book and ticker polling.
- Binance BTC/USDT is displayed as a comparison venue only and is not mixed into the USD reference.

The composite uses USD-quoted venue mid-prices, rejects venues more than 0.35% from the median, then calculates a capped depth-weighted mid-price from accepted venues. Near-market liquidity and imbalance are measured within 0.25% of mid. A composite is marked LIVE only when at least two accepted USD venues are fresh; otherwise it is PARTIAL or UNAVAILABLE. This is an independent estimate, **not official CF Benchmarks BRTI**.
- Coinbase and Kraken trade prints are normalized to aggressor-side BUY/SELL flow (Coinbase maker-side is inverted). Rolling 15s, 60s, and 180s windows expose buy volume, sell volume, net BTC delta, delta ratio, trade count, and unclassified volume. Quiet windows are distinguished from unclassified flow. These remain shadow measurements, not prediction inputs.

The shadow feed does not affect predictions or execution. It is visible on the dashboard and through:

- `/api/multi-exchange` — current venue/reference snapshot and comparison versus primary spot, official BRTI (when fresh), and strike.
- `/api/multi-exchange/history` — persisted observations.
- Shadow observations are recorded to SQLite every five seconds while the orchestrator is evaluating and a primary spot price is available. Source ages are stored; stale spot/BRTI values are excluded from comparisons.

## Reliability safeguards

- Primary trades/books reject malformed, non-finite, crossed, future-dated and out-of-order data; source switching can recover across small exchange-clock offsets.
- Primary feeds use exchange event timestamps when available. Closed one-second bars use the actual final trade price from that second.
- Stale order-book depth is excluded from evidence and forces a degraded WAIT state.
- Order-book depth must match the active spot-feed source; after a Binance/Coinbase source switch, mismatched depth is excluded and the forecast waits for a matching book.
- Official contract selection requires a fresh, non-future Kalshi poll. Synthetic contracts remain non-actionable.
- Paper execution independently rechecks contract time, spot-feed freshness, Kalshi quote freshness, ticker identity, probability validity and one-entry-per-contract.
- Synthetic contracts are never graded using a later contract's fallback price.
- Official settlement reconciliation runs separately from the prediction loop, processes a bounded oldest-first batch with limited concurrency, and is cancelled cleanly at shutdown.
- Shadow trade flow rejects malformed/non-finite trades and does not duplicate primary Binance data into its independent comparison venue.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

### Contract countdown
The dashboard countdown is calculated from the active contract's absolute `close_timestamp` and a server-clock offset, then refreshed locally four times per second. The synthetic fallback window is aligned to exact UTC 900-second (15-minute) boundaries. The checkpoint timer is derived from the contract open timestamp and the configured 180-second interval.
