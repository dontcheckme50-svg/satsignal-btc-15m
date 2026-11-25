# SATSIGNAL BTC 15M

A FastAPI monitoring and prediction service for BTC 15-minute Kalshi markets. This is a separately named project; it does not modify the legacy SATSIGNAL platform or SATBOT.

## Safety status

- This release is **paper-only**. The execution manager cannot submit demo or production orders.
- Paper entries require an active official `KXBTC15M` contract, an official strike, and a real Kalshi ask quote.
- Synthetic fallback windows are dashboard-only and cannot generate paper trades or enter the official prediction scoreboard.
- The scoreboard counts one official prediction lock per contract. Live engine snapshots are stored separately as observations and never inflate prediction totals. A directional Final Decision must agree on two consecutive one-second evaluations for the same official contract before it locks; WAIT, reversal, or contract rollover resets confirmation. If no confirmed direction exists at the 65-second final-entry cutoff, WAIT is locked. Locks are immutable and settlement grades only the lock.
- `/api/prediction-observations` returns the separate rolling observation history for analysis (limit 1,000; optional contract ticker filter).
- `/api/forecast` exposes Engine F and its independent lock state; `POST /api/forecast/lock` manually locks a current, fresh, directional forecast for an official Kalshi contract.
- Prediction accuracy is based on settled official-contract locks. Pending locks are not wins or losses; WAIT signals are excluded from directional accuracy.
- Paper-trading P&L is reported separately from prediction accuracy.
- No prediction strategy is proven profitable. The probabilities are heuristic estimates, not calibrated guarantees.

## Six engines (preserved)

A. Evidence: trade flow, delta/CVD, order-book imbalance, momentum, RSI, volatility, strike distance and structure.
B. Weakness: stale feeds, absorption, low-volume drift, book/tape conflict and exhaustion.
C. Regime: stable, shaky or high-risk reversal regime.
D. Early: opening-window probability estimate.
E. Final: combines evidence, weakness, regime and early-engine agreement into UP/DOWN/WAIT.
F. Forecast: independent strike-distance/tape probability with its own immutable manual lock per official contract. F does not modify Engine E's automatic official lock or place trades.

The six-engine architecture is preserved. Engine F is an independent forecast and its manual per-contract lock is stored separately in Supabase; it does not affect Engine E scoring or execution. Reliability work hardens evidence inputs and gates: stale/missing order books degrade the forecast, short startup history is neutral for volume ratio, and a synthetic contract cannot produce an actionable final verdict.

## Local run (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:TRADING_MODE = "paper"
$env:DATABASE_URL = "postgresql://USER:PASSWORD@HOST:PORT/postgres?sslmode=require"
.\.venv\Scripts\python.exe -m app.main
```

Open `http://localhost:8000`. API docs: `/docs`; process health (including storage status): `/api/health`; detailed PostgreSQL connection health and record counts: `/api/storage/health`; scoreboard: `/api/scoreboard`.

## Deployment and persistence

Supabase PostgreSQL is the only supported persistence backend. Set `DATABASE_URL` as a private hosting secret using the Supabase PostgreSQL connection string with TLS enabled (`sslmode=require`). Startup fails clearly when it is missing or cannot connect; the app never creates a local file database or in-memory substitute. No Hostless disk volume is required for ledger persistence.

## Feeds

- Binance aggregate trades and depth stream are primary.
- Coinbase WebSocket is fallback; its fallback order book is top-of-book only, not 20 levels.
- Kalshi polling accepts only currently active KXBTC15M markets with a valid official `floor_strike`.
- Official CF Benchmarks BRTI is accepted only from a valid, source-timestamped, fresh Kalshi pass-through response. The documented CF payload array is parsed using its source event time; stale, future-dated, malformed, and repeat-of-previous values are excluded. When fresh, BRTI contributes an independent 35% component to the Evidence Engine's strike-distance Z-score while primary spot retains 65%; a material spot/BRTI disagreement across the Kalshi strike adds a weakness penalty. BRTI is never replaced by Binance or another exchange. Missing entitlement leaves the BRTI component inactive rather than fabricating a value.

## SATSIGNAL Synthetic Benchmark (guarded supplemental mode)

The multi-exchange layer builds a clearly labeled synthetic BTC reference from independent public venues. It is **not official CF Benchmarks BRTI** and cannot replace or impersonate that feed.

- Coinbase BTC/USD Level 2 book and matches stream.
- Kraken BTC/USD book and trades WebSocket.
- Bitstamp BTC/USD public order-book and ticker polling.
- Binance BTC/USDT is displayed as a comparison venue only and is not mixed into the USD reference.

The composite uses USD-quoted venue mid-prices, rejects venues more than 0.35% from the median, then calculates a capped depth-weighted mid-price from accepted venues. Near-market liquidity and imbalance are measured within 0.25% of mid. A composite is marked LIVE only when at least two accepted USD venues are fresh; otherwise it is PARTIAL or UNAVAILABLE. This is an independent estimate, **not official CF Benchmarks BRTI**.
- Coinbase and Kraken trade prints are normalized to aggressor-side BUY/SELL flow (Coinbase maker-side is inverted). Rolling 15s, 60s, and 180s windows expose buy volume, sell volume, net BTC delta, delta ratio, trade count, and unclassified volume. Quiet windows are distinguished from unclassified flow. These remain shadow measurements, not prediction inputs.

The shadow feed does not affect predictions or execution. It is visible on the dashboard and through:

- `/api/multi-exchange` — current venue/reference snapshot and comparison versus primary spot, official BRTI (when fresh), and strike.
- `/api/multi-exchange/history` — persisted observations.
- Shadow observations are recorded to Supabase PostgreSQL every five seconds while the orchestrator is evaluating and a primary spot price is available. Source ages are stored; stale spot/BRTI values are excluded from comparisons.

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

Tests require `TEST_DATABASE_URL` to point to a dedicated disposable PostgreSQL test database. The test bootstrap routes the suite to this URL and clears only the four SATSIGNAL test tables before each test. Never point it at the production Supabase database.


```powershell
$env:TEST_DATABASE_URL = "postgresql://USER:PASSWORD@TEST-HOST:PORT/postgres?sslmode=require"
.\.venv\Scripts\python.exe -m pytest -q
```

### Contract countdown
The dashboard countdown uses the active contract's absolute `close_timestamp`, a millisecond server-time sync, and a monotonic browser clock. Clock sync is independent of prediction snapshots and the timer refreshes locally four times per second. The synthetic fallback window is aligned to exact UTC 900-second (15-minute) boundaries. The checkpoint timer is derived from the contract open timestamp and the configured 180-second interval.
