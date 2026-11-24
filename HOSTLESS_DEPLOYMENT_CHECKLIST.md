# SATSIGNAL BTC 15M — Hostless deployment checklist

This project is a Dockerized FastAPI app. Hostless should build the repository with its Dockerfile, bind to `0.0.0.0`, listen on `PORT`, and use `/api/health` as the health-check path.

## Supabase is the only persistence backend

1. Confirm the correct Supabase project for the new `dontcheckme50-svg/satsignal-btc-15m` Hostless service.
2. In Supabase, open **Connect** and copy the PostgreSQL connection string. Use a connection type supported by Hostless (prefer the session pooler if direct connectivity is restricted) and require TLS with `sslmode=require`.
3. In Hostless, add `DATABASE_URL` as a private secret environment variable. Do not place the connection string in GitHub, source files, build logs, or chat.
4. Keep `TRADING_MODE=paper`. This release does not enable live order submission.
5. No Hostless disk volume is needed for prediction history, scoreboard, trade records, observations, or multi-exchange history. Those records are stored in Supabase PostgreSQL.
6. Deploy the new branch only after its pull request checks pass. Do not change the old Rudy1128 Hostless app.

## Deployment verification

After deployment, verify:

- `/api/health` returns HTTP 200, `trading_mode: paper`, and storage status `HEALTHY`.
- `/api/storage/health` reports `storage_type: POSTGRESQL`, `provider: SUPABASE`, and `connection_check: ok`.
- `/` loads the full dashboard.
- `/api/feeds` reports feed status.
- `/api/scoreboard`, `/api/history`, `/api/prediction-observations`, `/api/trades`, and `/api/multi-exchange/history` return valid responses.
- The active contract ticker begins with `KXBTC15M`, its strike source is `KALSHI_OFFICIAL`, and its close time is in the future.

A successful process health response by itself is not proof that a valid official market contract or fresh feeds are active. Verify those separately.

## Test database and CI

- GitHub Actions uses a temporary PostgreSQL service for regression and storage integration tests.
- Local test runs must set `TEST_DATABASE_URL` to a disposable PostgreSQL/Supabase test database. Test setup overrides `DATABASE_URL` with that value and clears only the SATSIGNAL test tables before each test.
- Never set `TEST_DATABASE_URL` to the production project.
- The production Hostless app uses `DATABASE_URL` and is not affected by test database cleanup.

## Data integrity notes

- Prediction records are scored only after the official Kalshi settlement fetch verifies the contract outcome.
- WAIT signals are excluded from directional win-rate denominators and shown separately.
- Paper-trade P&L is separate from prediction accuracy.
- Official CF Benchmarks BRTI is never substituted with Binance spot. Missing entitlement leaves the BRTI component inactive.
- The five prediction engines and the separate multi-exchange shadow feed remain independent as documented in the README.
