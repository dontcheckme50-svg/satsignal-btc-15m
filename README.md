# SATSIGNAL BTC 15M

A FastAPI monitoring and prediction service for BTC 15-minute Kalshi markets. This is a separately named project; it does not modify the legacy SATSIGNAL platform or SATBOT.

## Safety status

- This release is **paper-only**. The execution manager cannot submit demo or production orders.
- Paper entries require an active official `KXBTC15M` contract, an official strike, and a real Kalshi ask quote.
- Synthetic fallback windows are dashboard-only and cannot generate paper trades or enter the official prediction scoreboard.
- Prediction accuracy is based on settled official-contract predictions. Pending predictions are not wins or losses; WAIT signals are excluded from directional accuracy.
- Paper-trading P&L is reported separately from prediction accuracy.
- No prediction strategy is proven profitable. The probabilities are heuristic estimates, not calibrated guarantees.

## Five engines (preserved)

A. Evidence: trade flow, delta/CVD, order-book imbalance, momentum, RSI, volatility, strike distance and structure.
B. Weakness: stale feeds, absorption, low-volume drift, book/tape conflict and exhaustion.
C. Regime: stable, shaky or high-risk reversal regime.
D. Early: opening-window probability estimate.
E. Final: combines evidence, weakness, regime and early-engine agreement into UP/DOWN/WAIT.

The dashboard redesign changes presentation and reporting; it does not change the five engines' scoring formulas.

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
- BRTI polling is an optional authenticated integration attempt. It is displayed as a feed status but is not currently incorporated into five-engine scoring. Validate endpoint, entitlement and payload before relying on it.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

### Contract countdown
The dashboard countdown is calculated from the active contract's absolute `close_timestamp` and a server-clock offset, then refreshed locally four times per second. The synthetic fallback window is aligned to exact UTC 900-second (15-minute) boundaries. The checkpoint timer is derived from the contract open timestamp and the configured 180-second interval.
