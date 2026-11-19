import sqlite3

from app.database import PredictionLedger


def _seed_settled_directional_rows(ledger: PredictionLedger, count: int) -> None:
    with ledger._connection() as conn:
        for index in range(count):
            # Deliberately overconfident historical forecasts with a mixed
            # outcome distribution, giving the calibrator something to learn.
            outcome = "UP" if index % 3 == 0 else "DOWN"
            conn.execute(
                """INSERT INTO predictions (
                    contract_ticker, timestamp_iso, epoch_ts, strike_price,
                    btc_price_at_pred, seconds_remaining, main_signal,
                    early_signal, final_signal, confidence, up_prob, down_prob,
                    regime, settled, actual_outcome, final_won, main_won,
                    early_won, is_official_lock
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 1)""",
                (
                    f"TEST-{index}", "2026-01-01T00:00:00Z", float(index),
                    100000.0, 100010.0, 100.0, "UP", "UP", "UP",
                    80.0, 0.8, 0.2, "STABLE", outcome,
                    int(outcome == "UP"), int(outcome == "UP"),
                    int(outcome == "UP"),
                ),
            )


def test_probability_calibration_stays_identity_with_insufficient_samples(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "small.db"))
    _seed_settled_directional_rows(ledger, 29)

    weights = ledger.get_engine_weights()

    assert weights["calibration_samples"] == 0
    assert weights["calibration_slope"] == 1.0
    assert weights["calibration_intercept"] == 0.0


def test_probability_calibration_activates_only_after_minimum_settled_sample(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "calibration.db"))
    _seed_settled_directional_rows(ledger, 45)

    weights = ledger.get_engine_weights()

    assert weights["calibration_samples"] == 45
    assert 0.25 <= weights["calibration_slope"] <= 2.5
    assert -1.0 <= weights["calibration_intercept"] <= 1.0
