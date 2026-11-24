# SATSIGNAL BTC 15M — Hostless deployment checklist

This project is a Dockerized FastAPI app. Hostless supports Git-connected apps built from a Dockerfile. The app must bind to `0.0.0.0`, listen on `PORT`, and expose a successful health endpoint.

## Free-first deployment

1. Push this project to a **new private Git repository**. Do not upload `.env`, private keys, or database files.
2. In Hostless, create an App from that repository and select Docker as the build system.
3. Use the repository root as the working directory and `Dockerfile` as the Dockerfile path.
4. Set the health-check path to `/api/health`.
5. Keep the app at one replica and paper-only mode. This release hardcodes `trading_mode = "paper"` and has no live/demo order-submission path.
6. Do **not** create or attach a paid volume or resize beyond the account's free entitlement. Review the current resource estimate before confirming creation or deployment.
7. Configure the secret `DATABASE_URL` with the project's Supabase PostgreSQL connection string and SSL enabled. The app uses Supabase for prediction, trade, and scoreboard persistence; it does not use SQLite or a local database file. Do not commit the connection string.
8. Deploy and verify:
   - `/api/health` returns `status: ok` and `trading_mode: paper`
   - `/` displays the dashboard
   - `/api/feeds` shows feed health
   - `/api/scoreboard` returns totals, pending counts, and per-engine results
   - `/api/history` and `/api/trades` return records
9. Do not treat HTTP 200 or a dashboard snapshot as proof that the official market contract is valid. Confirm the ticker begins `KXBTC15M`, strike source is `KALSHI_OFFICIAL`, and contract close time is in the future.
10. Let it collect verified settlements and paper observations before evaluating predictive performance. No profitability claim is made.

## Data integrity notes

- Prediction records are scored only after the official Kalshi settlement fetch verifies the contract outcome.
- WAIT signals are excluded from directional win-rate denominators and shown separately.
- Paper-trade P&L is separate from prediction accuracy.
- CF Benchmarks BRTI remains unavailable unless the required entitlement is configured; it is not silently replaced by Binance spot.
