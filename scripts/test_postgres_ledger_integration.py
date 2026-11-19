"""Destructive only inside a uniquely named, temporary PostgreSQL schema.

Creates a schema, exercises the SATSIGNAL ledger API, and drops that schema in
finally. It never touches public tables or imports production data.
"""
from __future__ import annotations

import os
import re
import sys
import traceback
import uuid
from pathlib import Path

# Allow direct execution as `python scripts/test_postgres_ledger_integration.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg

from app.postgres_database import PostgresPredictionLedger
from app.models import MarketRegime, PredictionRecord, Signal, TradeRecord


def main() -> int:
    database_url = os.getenv("SUPABASE_DATABASE_URL", "")
    if not database_url:
        print("SKIP: SUPABASE_DATABASE_URL is not configured")
        return 0

    schema = "satsignal_test_" + uuid.uuid4().hex[:12]
    if not re.fullmatch(r"[a-z0-9_]+", schema):
        raise RuntimeError("Unsafe generated schema name")

    admin = psycopg.connect(database_url, connect_timeout=10, sslmode="require")
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
        admin.commit()
        # Build a libpq connection string with a correctly spaced -c option.
        # The old URI form omitted the space after -c, so libpq ignored it and
        # silently fell back to public. make_conninfo safely encodes the value.
        test_url = psycopg.conninfo.make_conninfo(
            database_url,
            options=f"-c search_path={schema}",
        )

        # Prove every new pooled connection resolves this unique schema before
        # allowing the ledger constructor to create or write any tables.
        with psycopg.connect(test_url, connect_timeout=10, sslmode="require") as guard:
            resolved_schema = guard.execute("SELECT current_schema()").fetchone()[0]
            if resolved_schema != schema:
                raise RuntimeError(f"SAFE_SCHEMA_GUARD: expected={schema}; resolved={resolved_schema}; refusing all ledger writes")

        ledger = PostgresPredictionLedger(test_url)

        prediction = PredictionRecord(
            contract_ticker="KXBTC15M-TEST-INTEGRATION",
            timestamp_iso="2026-11-19 12:00:00 UTC",
            epoch_ts=1795118400.0,
            strike_price=90000.0,
            btc_price_at_pred=90010.0,
            seconds_remaining=600.0,
            main_signal=Signal.UP,
            early_signal=Signal.UP,
            final_signal=Signal.UP,
            confidence=75.0,
            up_prob=0.75,
            down_prob=0.25,
            regime=MarketRegime.STABLE,
            brti_cohort="AGREE",
            brti_side_at_lock="UP",
            spot_side_at_lock="UP",
        )
        prediction_id = ledger.record_prediction(prediction)
        assert prediction_id > 0
        assert ledger.record_prediction(prediction) == prediction_id
        assert ledger.has_official_prediction_lock(prediction.contract_ticker)

        observation_id = ledger.record_prediction_observation(
            prediction.contract_ticker, 1795118401.0, 599.0, {"signal": "UP", "test": True}
        )
        assert observation_id > 0
        observations = ledger.get_recent_prediction_observations(
            limit=10, contract_ticker=prediction.contract_ticker
        )
        if not (
            len(observations) == 1
            and isinstance(observations[0].get("payload"), dict)
            and observations[0]["payload"].get("test") is True
        ):
            payload = observations[0].get("payload") if observations else None
            safe_keys = sorted(payload.keys()) if isinstance(payload, dict) else []
            raise AssertionError(
                f"observation_count={len(observations)}; "
                f"payload_type={type(payload).__name__}; payload_keys={safe_keys}"
            )

        trade_id = ledger.record_trade(TradeRecord(
            mode="paper",
            contract_ticker=prediction.contract_ticker,
            timestamp_iso="2026-11-19 12:00:01 UTC",
            side="YES",
            contracts=2,
            entry_price=0.40,
            quote_source="TEST",
            fee_paid=0.02,
            fair_prob_at_entry=0.75,
            net_edge=0.30,
            status="OPEN",
            pnl_usd=0.0,
            notes="temporary integration test",
        ))
        assert trade_id > 0
        assert prediction.contract_ticker in ledger.get_unsettled_contract_tickers()
        assert ledger.settle_contract(
            prediction.contract_ticker, 90020.0, official_result="UP",
            settlement_source="INTEGRATION_TEST"
        ) == 1

        recent = ledger.get_recent_predictions(limit=10)
        assert len(recent) == 1 and recent[0].settled is True and recent[0].final_won is True
        trades = ledger.get_recent_trades(limit=10)
        assert len(trades) == 1 and trades[0].status == "SETTLED"
        scoreboard = ledger.get_scoreboard()
        assert scoreboard.total_predictions == 1
        assert scoreboard.final_wins == 1
        assert scoreboard.final_losses == 0
        assert scoreboard.total_trades == 1

        health = ledger.get_storage_health()
        assert health["status"] == "HEALTHY"
        print("PASS: isolated PostgreSQL ledger write/read/lock/settlement/scoreboard checks")
        return 0
    finally:
        try:
            admin.rollback()
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            admin.commit()
        finally:
            admin.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Never expose DSNs or credential-bearing exception strings in CI logs.
        print(f"FAIL: PostgreSQL ledger integration test ({type(exc).__name__})")
        if isinstance(exc, AssertionError) and str(exc):
            # Only our explicitly sanitized assertion diagnostics are emitted.
            print(f"SAFE_ASSERTION_DETAIL: {exc}")
        elif isinstance(exc, RuntimeError) and str(exc).startswith("SAFE_SCHEMA_GUARD:"):
            print(str(exc))
        # Print source locations only; never print exception text or connection details.
        for frame in traceback.extract_tb(exc.__traceback__):
            print(f"  at {Path(frame.filename).name}:{frame.lineno} in {frame.name}")
        raise SystemExit(1)
