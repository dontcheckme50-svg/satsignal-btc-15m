# SATSIGNAL BTC 15M — Hostless deployment checklist

This project is a Dockerized FastAPI app. Hostless supports Git-connected apps built from a Dockerfile. The app must bind to `0.0.0.0`, listen on `PORT`, and expose a successful health endpoint.

## Free-first deployment

1. Push this project to a **new private Git repository**. Do not upload `.env`, private keys, or database files.
2. In Hostless, create an App from that repository and select Docker as the build system.
3. Use the repository root as the working directory and `Dockerfile` as the Dockerfile path.
4. Set the health-check path to `/api/health`.
5. Keep the app at one replica and paper-only mode. This release hardcodes `trading_mode = "paper"` and has no live/demo order-submission path.
6. Do **not** create or attach a paid volume or resize beyond the account's free entitlement. Review the current resource estimate before confirming creation or deployment.
7. The current release uses SQLite. Without persistent storage, prediction history, trade history, and scoreboard records may be lost when the container is replaced. Do not assume the container filesystem is durable. For durable cloud history, use an included/free managed database allowance only after confirming the exact cost in the Hostless UI and migrating the app database deliberately. A new volume can incur charges.
8. Deploy and verify:
   - `/api/health` returns `status: ok` and `trading_mode: paper`
   - `/` displays the dashboard
   - `/api/feeds` shows feed health
   - `/api/scoreboard` returns totals, pending counts, and per-engine results
   - `/api/history` and `/api/trades` return records
9. Do not treat HTTP 200 or a dashboard snapshot as proof that the official market contract is valid. Confirm the ticker begins `KXBTC15M`, strike source is `KALSHI_OFFICIAL`, and contract close time is in the future.
10. Let it collect verified settlements and paper observations before evaluating predictive performance. No profitability claim is made.

## Kalshi credentials and official BRTI

- Configure `KALSHI_KEY_ID` as a Hostless secret/environment variable.
- Make the matching private key available as a Hostless-mounted secret file and set `KALSHI_PRIVATE_KEY_PATH` to that in-container file path. Do not paste PEM contents into deployment logs, source control, or ordinary configuration text.
- Confirm the key has the required Kalshi API permissions and the account has the official CF Benchmarks BRTI entitlement. Authentication alone does not guarantee feed entitlement.
- Verify the feed endpoint reports CF Benchmarks BRTI as `LIVE` with a fresh source timestamp. If access is denied or data is stale, the predictor must continue to report BRTI unavailable and must not substitute Binance spot for BRTI.
- Keep credentials out of the Docker build context. The repository's `.dockerignore` excludes environment files, private keys, local databases, and test artifacts.

## SQLite persistence verification (required before production)

SQLite can survive Hostless restarts only when its database file is inside a Hostless persistent volume. Docker's `RUN mkdir -p /data` creates a directory; it does not make that directory persistent.

1. Attach a persistent Hostless volume at `/data` only if its price is explicitly approved. Do not create a paid volume without approval.
2. Confirm the Hostless volume is mounted and writable at `/data`.
3. Set `PREDICTOR_DB_PATH=/data/satsignal_btc15m.db`.
4. Set `PREDICTOR_STORAGE_MOUNT=/data`.
5. Set `PREDICTOR_REQUIRE_PERSISTENT_STORAGE=1`. With this enabled, startup fails closed if the mount is missing, read-only, or the SQLite path is outside it. This prevents the app from quietly accepting writes to disposable container storage.
6. Verify `/api/storage/health` reports `persistent_mount_confirmed: true` and `status: PERSISTENT_MOUNT_CONFIRMED` under `persistent_storage`.
7. Write a test prediction/trade in a non-production test environment, restart/redeploy the Hostless app, then verify the same record IDs and scoreboard totals remain. Repeat at least three times.
8. If the mount cannot be confirmed, do not treat the app as production-ready. The app may run in development mode with persistence marked unverified, but records are not guaranteed across container replacement.

The automated regression suite also starts separate Python processes against one shared directory and verifies SQLite rows remain readable after three process restarts. This tests SQLite/process restart behavior; only a real Hostless restart with an attached persistent volume can verify Hostless volume retention.

## Data integrity notes

- Prediction records are scored only after the official Kalshi settlement fetch verifies the contract outcome.
- WAIT signals are excluded from directional win-rate denominators and shown separately.
- Paper-trade P&L is separate from prediction accuracy.
- CF Benchmarks BRTI remains unavailable unless the required entitlement is configured; it is not silently replaced by Binance spot.
