from app.engines import EarlyPredictionEngine, ForecastEngine
from app.orchestrator import PredictorOrchestrator
import uuid

import pytest

from app.database import PredictionLedger
from app.models import (
    ContractInfo, EarlyPredictionReport, EvidenceMetrics, ForecastReport, Signal, WeaknessReport,
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



def test_engine_f_forecast_is_independent_directional_and_symmetric():
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
    assert up.up_probability + up.down_probability == pytest.approx(1.0)


def test_engine_f_waits_when_forecast_inputs_are_degraded():
    report = ForecastEngine().forecast(
        evidence(strike_z_score=2.5, raw_score=80.0),
        weakness(feed_degraded=True, weakness_score=100.0),
        contract(),
    )

    assert report.signal == Signal.WAIT


def test_engine_f_lock_is_persisted_and_immutable_in_supabase():
    ledger = PredictionLedger()
    ticker = "KXBTC15M-ENGINEF-" + uuid.uuid4().hex[:12]
    original = {
        "contract_ticker": ticker,
        "timestamp_iso": "2026-11-24 12:05:00 UTC",
        "epoch_ts": 1795541100.0,
        "signal": "UP",
        "confidence": 72.0,
        "up_prob": 0.72,
        "down_prob": 0.28,
        "strike_price": 100000.0,
        "btc_price_at_lock": 100010.0,
    }

    first = ledger.record_forecast_lock(original)
    second = ledger.record_forecast_lock({
        **original,
        "signal": "DOWN",
        "confidence": 81.0,
        "up_prob": 0.19,
        "down_prob": 0.81,
    })

    assert first["signal"] == "UP"
    assert second["signal"] == "UP"
    assert second["confidence"] == 72.0
    assert ledger.get_forecast_lock(ticker)["signal"] == "UP"


def test_engine_f_rejects_wait_locks():
    ledger = PredictionLedger()
    with pytest.raises(ValueError, match="Only directional"):
        ledger.record_forecast_lock({
            "contract_ticker": "KXBTC15M-ENGINEF-WAIT-" + uuid.uuid4().hex[:8],
            "timestamp_iso": "2026-11-24 12:05:00 UTC",
            "epoch_ts": 1795541100.0,
            "signal": "WAIT",
            "confidence": 50.0,
            "up_prob": 0.5,
            "down_prob": 0.5,
            "strike_price": 100000.0,
            "btc_price_at_lock": 100000.0,
        })

class _ForecastLockLedger:
    def __init__(self):
        self.locks = {}
        self.write_count = 0

    def get_forecast_lock(self, ticker):
        return self.locks.get(ticker)

    def record_forecast_lock(self, row):
        self.write_count += 1
        return self.locks.setdefault(row["contract_ticker"], dict(row))


def _auto_lock_orchestrator(ledger):
    orchestrator = object.__new__(PredictorOrchestrator)
    orchestrator.ledger = ledger
    orchestrator._engine_f_lock_candidate_ticker = None
    orchestrator._engine_f_lock_candidate = Signal.WAIT
    orchestrator._engine_f_lock_candidate_count = 0
    return orchestrator


def test_engine_f_auto_locks_after_three_qualified_evaluations_and_persists_immutably():
    ledger = _ForecastLockLedger()
    orchestrator = _auto_lock_orchestrator(ledger)
    ev = evidence(signal=Signal.UP)
    report = ForecastReport(
        signal=Signal.UP, confidence=64.0,
        up_probability=0.64, down_probability=0.36,
    )

    assert orchestrator._maybe_auto_lock_engine_f(
        contract(), report, ev, weakness(), True, 1_800_000_000.0, True
    ) is None
    assert orchestrator._maybe_auto_lock_engine_f(
        contract(), report, ev, weakness(), True, 1_800_000_001.0, True
    ) is None
    locked = orchestrator._maybe_auto_lock_engine_f(
        contract(), report, ev, weakness(), True, 1_800_000_002.0, True
    )

    assert locked["signal"] == "UP"
    assert ledger.write_count == 1
    later = orchestrator._maybe_auto_lock_engine_f(
        contract(), ForecastReport(
            signal=Signal.DOWN, confidence=80.0,
            up_probability=0.2, down_probability=0.8,
        ), evidence(signal=Signal.DOWN), weakness(), True, 1_800_000_003.0, True
    )
    assert later["signal"] == "UP"
    assert ledger.write_count == 1


@pytest.mark.parametrize("forecast,ev,weak,remaining,fresh", [
    (ForecastReport(signal=Signal.UP, confidence=59.9, up_probability=0.599, down_probability=0.401), Signal.UP, 0.0, 600.0, True),
    (ForecastReport(signal=Signal.UP, confidence=70.0, up_probability=0.7, down_probability=0.3), Signal.DOWN, 0.0, 600.0, True),
    (ForecastReport(signal=Signal.UP, confidence=70.0, up_probability=0.7, down_probability=0.3), Signal.UP, 35.1, 600.0, True),
    (ForecastReport(signal=Signal.UP, confidence=70.0, up_probability=0.7, down_probability=0.3), Signal.UP, 0.0, 120.0, True),
    (ForecastReport(signal=Signal.UP, confidence=70.0, up_probability=0.7, down_probability=0.3), Signal.UP, 0.0, 600.0, False),
])
def test_engine_f_auto_lock_rejects_unqualified_or_late_forecast(forecast, ev, weak, remaining, fresh):
    ledger = _ForecastLockLedger()
    orchestrator = _auto_lock_orchestrator(ledger)
    c = contract().model_copy(update={"seconds_remaining": remaining})

    for n in range(4):
        assert orchestrator._maybe_auto_lock_engine_f(
            c, forecast, evidence(signal=ev), weakness(weakness_score=weak),
            fresh, 1_800_000_000.0 + n, True
        ) is None
    assert ledger.write_count == 0


def test_engine_f_auto_lock_does_not_persist_during_observation_only_evaluation():
    ledger = _ForecastLockLedger()
    orchestrator = _auto_lock_orchestrator(ledger)
    report = ForecastReport(
        signal=Signal.UP, confidence=70.0,
        up_probability=0.7, down_probability=0.3,
    )

    for n in range(5):
        assert orchestrator._maybe_auto_lock_engine_f(
            contract(), report, evidence(signal=Signal.UP), weakness(),
            True, 1_800_000_000.0 + n, False
        ) is None
    assert ledger.write_count == 0

def test_engine_f_lock_is_in_settlement_queue_and_graded_by_verified_outcome():
    ledger = PredictionLedger()
    ticker = "KXBTC15M-ENGINEF-SETTLE-" + uuid.uuid4().hex[:10]
    ledger.record_forecast_lock({
        "contract_ticker": ticker,
        "timestamp_iso": "2026-11-24 12:05:00 UTC",
        "epoch_ts": 1795541100.0,
        "signal": "UP",
        "confidence": 72.0,
        "up_prob": 0.72,
        "down_prob": 0.28,
        "strike_price": 100000.0,
        "btc_price_at_lock": 100010.0,
    })

    assert ticker in ledger.get_unsettled_contract_tickers()
    # Verified Kalshi outcome is authoritative; no settlement price is required.
    ledger.settle_contract(
        ticker,
        settlement_price=None,
        official_result="UP",
        settlement_source="KALSHI_VERIFIED",
    )

    lock = ledger.get_forecast_lock(ticker)
    assert lock["settled"] == 1
    assert lock["actual_outcome"] == "UP"
    assert lock["won"] == 1
    assert lock["settlement_source"] == "KALSHI_VERIFIED"

