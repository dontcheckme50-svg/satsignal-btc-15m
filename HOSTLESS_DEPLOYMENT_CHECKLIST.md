# SATSIGNAL BTC 15M — Hostless deployment checklist

This project is a Dockerized FastAPI app. Hostless supports Git-connected apps built from a Dockerfile. The app must bind to `0.0.0.0`, listen on `PORT`, and expose a successful health endpoint.

## Free-first deployment

1. Push this project to a **new private Git repository**. Do not upload `.env`, private keys, or database files.
2. In Hostless, create an App from that repository and select Docker as the build system.
3. Use the repository root as the working directory and `Dockerfile` as the Dockerfile path.
4. Set the health-check path to `/api/health`.
5. Keep the app at one replica and paper-only mode. This release hardcodes `trading_mode = "paper"` and has no live/demo order-submission path.
6. Attach the Hostless persistent volume at `/data` using the same Hostless app deployment. If the Hostless UI shows an added charge, pause and confirm the price before applying it; do not silently approve paid resources.
7. The Docker image now defaults SQLite to `/data/satsignal_btc15m.db` and requires a verified writable persistent mount. The app will fail startup rather than quietly write the scoreboard and history to disposable container storage.
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

## Existing SQLite database migration (do not skip)

Before changing the live app's database path or replacing its container:

1. Identify the exact database file used by the current deployment from its existing `PREDICTOR_DB_PATH` setting. If unset, the application default is `satsignal_btc15m.db` relative to the app's working directory.
2. Export a consistent SQLite backup from the current deployment while preserving the original database. Do not rely on a raw copy of an active WAL database; use SQLite's online backup API or a verified Hostless database export.
3. Keep the original export untouched and stage a separate copy where a one-off migration process can read it and write to the mounted `/data` volume.
4. Run the migration utility before starting the new app process, with the destination path absent:
   ```sh
   python -m scripts.migrate_sqlite --source /path/to/exported-existing.db --destination /data/satsignal_btc15m.db
   ```
5. Save the JSON result. It must report `status: verified`, `integrity_check: ok`, and row counts for every ledger table present in the source. Compare those counts with the current deployment's history, scoreboard, and trades before allowing normal startup.
6. The migration utility refuses to overwrite any existing destination. If the destination already exists, stop and inspect it; do not delete it or rerun with a different source until its contents are understood.
7. Keep the old deployment and original export available until the new deployment passes the restart and record-retention checks. Do not delete the old copy as part of this migration.

Hostless must provide a way to export the current database and run the one-off migration before the new app starts. If it does not, pause deployment and choose a verified transfer method first. Never replace the old container on the assumption its local filesystem or database will remain available.

## SQLite persistence verification (required before production)

SQLite survives a Hostless container restart when the database file is stored on a Hostless persistent volume. The Docker image creates the `/data` directory, but the Hostless volume attachment is what makes the data durable. The image now sets `PREDICTOR_DB_PATH=/data/satsignal_btc15m.db`, `PREDICTOR_STORAGE_MOUNT=/data`, and `PREDICTOR_REQUIRE_PERSISTENT_STORAGE=1` by default.

1. Attach the Hostless persistent volume to the existing app at `/data`. If the UI displays an added charge, pause and confirm the exact price before applying it.
2. Confirm the volume is mounted and writable at `/data`.
3. Verify `/api/storage/health` reports `persistent_mount_confirmed: true` and `status: PERSISTENT_MOUNT_CONFIRMED` under `persistent_storage`.
4. Write a test prediction/trade in a non-production test environment, restart/redeploy the Hostless app, and verify the same record IDs and scoreboard totals remain. Repeat at least three times.
5. If the mount is missing or invalid, startup is blocked. Do not disable the guard to make the deployment appear healthy; correct the volume attachment instead.

CI now builds the production image, starts it with a persistent Docker volume, writes a SQLite marker, replaces the container, and verifies the marker survives. This proves the image's SQLite path and guard work with a persistent volume. Only a real Hostless restart with its attached volume can verify Hostless volume retention.
## Data integrity notes

- Prediction records are scored only after the official Kalshi settlement fetch verifies the contract outcome.
- WAIT signals are excluded from directional win-rate denominators and shown separately.
- Paper-trade P&L is separate from prediction accuracy.
- CF Benchmarks BRTI remains unavailable unless the required entitlement is configured; it is not silently replaced by Binance spot.
