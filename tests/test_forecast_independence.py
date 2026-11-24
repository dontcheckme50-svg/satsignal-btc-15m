from app.engines import EarlyPredictionEngine, ForecastEngine
from app.database import PredictionLedger
from app.models import (
    ContractInfo, EarlyPredictionReport, EvidenceMetrics, Signal, WeaknessReport,
)


def contract() -> ContractInfo:
    return ContractInfo(
        ticker="TEST-BTC-15M",
        strike_price=100000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=0.0,
        close_timestamp=900.0,
        seconds_remaining=600.0,
        checkpoint_seconds_remaining=60.0,
        is_official_kalshi=True,
    )


def evidence(**overrides) -> EvidenceMetrics:
    values = dict(
        live_price=100010.0,
        distance_from_strike=10.0,
        distance_pct=0.01,
        buying_pressure_pct=50.0,
        selling_pressure_pct=50.0,
        trade_delta_60s=0.0,
        cvd_contract=0.0,
        volume_ratio=1.0,
        momentum_score=0.0,
        rsi_14=50.0,
        volatility_annualized=0.5,
        orderbook_imbalance=0.0,
        large_order_bias=0.0,
        structure_bias="COMPRESSION",
        raw_score=0.0,
        signal=Signal.WAIT,
        strike_z_score=0.0,
    )
    values.update(overrides)
    return EvidenceMetrics(**values)


def weakness(**overrides) -> WeaknessReport:
    values = dict(
        weakness_score=0.0,
        flags=[],
        absorption_detected=False,
        volume_divergence=False,
        momentum_fading=False,
        feed_degraded=False,
    )
    values.update(overrides)
    return WeaknessReport(**values)


def test_early_probability_does_not_double_count_strike_distance():
    engine = EarlyPredictionEngine()
    first = engine.analyze(evidence(strike_z_score=2.5), weakness(), contract())
    second = engine.analyze(evidence(strike_z_score=-2.5), weakness(), contract())

    assert first.up_prob == second.up_prob
    assert first.down_prob == second.down_prob


def test_neutral_early_evidence_is_symmetric_and_waits():
    report = EarlyPredictionEngine().analyze(evidence(), weakness(), contract())

    assert report.up_prob == 0.5
    assert report.down_prob == 0.5
    assert report.signal == Signal.WAIT


def test_early_engine_responds_to_directional_tape_and_structure():
    engine = EarlyPredictionEngine()
    up = engine.analyze(
        evidence(
            buying_pressure_pct=75.0,
            selling_pressure_pct=25.0,
            trade_delta_60s=3.0,
            momentum_score=45.0,
            orderbook_imbalance=0.55,
            structure_bias="BULLISH_HH_HL",
        ),
        weakness(),
        contract(),
    )
    down = engine.analyze(
        evidence(
            buying_pressure_pct=25.0,
            selling_pressure_pct=75.0,
            trade_delta_60s=-3.0,
            momentum_score=-45.0,
            orderbook_imbalance=-0.55,
            structure_bias="BEARISH_LH_LL",
        ),
        weakness(),
        contract(),
    )

    assert up.up_prob > 0.5
    assert down.up_prob < 0.5
    assert up.signal == Signal.UP
    assert down.signal == Signal.DOWN


def test_degraded_feed_forces_early_wait():
    report = EarlyPredictionEngine().analyze(
        evidence(
            buying_pressure_pct=80.0,
            selling_pressure_pct=20.0,
            trade_delta_60s=5.0,
            momentum_score=70.0,
            orderbook_imbalance=0.8,
        ),
        weakness(feed_degraded=True, weakness_score=100.0),
        contract(),
    )

    assert report.signal == Signal.WAIT


def test_engine_f_forecast_is_independent_of_engine_e_final_output():
    engine = ForecastEngine()
    up = engine.forecast(
        evidence(strike_z_score=1.8, raw_score=35.0),
        weakness(),
        contract(),
    )
    down = engine.forecast(
        evidence(strike_z_score=-1.8, raw_score=-35.0),
        weakness(),
        contract(),
    )

    assert up.signal == Signal.UP
    assert down.signal == Signal.DOWN
    assert up.up_probability > down.up_probability
    assert "Engine F" not in up.rationale  # rationale describes its own forecast inputs


def test_engine_f_degraded_inputs_force_wait():
    report = ForecastEngine().forecast(
        evidence(strike_z_score=2.5, raw_score=80.0),
        weakness(feed_degraded=True, weakness_score=100.0),
        contract(),
    )

    assert report.signal == Signal.WAIT


def test_manual_forecast_lock_is_immutable_per_contract(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "forecast-lock.db"))
    original = {
        "contract_ticker": "KXBTC15M-FORECAST",
        "timestamp_iso": "2027-01-01 00:05:00 UTC",
        "epoch_ts": 300.0,
        "signal": "UP",
        "confidence": 72.0,
        "up_prob": 0.72,
        "down_prob": 0.28,
        "strike_price": 100000.0,
        "btc_price_at_lock": 100010.0,
    }
    first = ledger.record_forecast_lock(original)
    second = ledger.record_forecast_lock({**original, "signal": "DOWN", "confidence": 81.0})

    assert first["signal"] == "UP"
    assert second["signal"] == "UP"
    assert ledger.get_forecast_lock("KXBTC15M-FORECAST")["confidence"] == 72.0
