import time

from app.engines import EvidenceEngine, FinalDecisionEngine, WeaknessEngine, RegimeEngine, EarlyPredictionEngine
from app.models import ContractInfo, EarlyPredictionReport, MarketRegime, RegimeReport, Signal


def _contract():
    now = time.time()
    return ContractInfo(
        ticker="KXBTC15M-TEST",
        strike_price=64000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 780.0,
        close_timestamp=now + 120.0,
        seconds_remaining=120.0,
        checkpoint_seconds_remaining=120.0,
        is_official_kalshi=True,
    )


def test_15m_radar_flags_primary_vs_usd_reference_price_gap():
    now = time.time()
    contract = _contract()
    ev = EvidenceEngine().analyze(
        now, 64040.0, contract, [], [], [], [],
        reference_price=64000.0, reference_age_sec=0.5,
    )

    assert ev.reference_price == 64000.0
    assert ev.reference_spread_bps is not None
    assert ev.reference_conflict is True

    weakness = WeaknessEngine().analyze(ev, 0.2, 0.2, True)
    assert any("Independent USD exchange reference disagrees" in flag for flag in weakness.flags)


def test_15m_radar_does_not_flag_small_fresh_reference_difference():
    now = time.time()
    contract = _contract()
    ev = EvidenceEngine().analyze(
        now, 64040.0, contract, [], [], [], [],
        reference_price=64039.0, reference_age_sec=0.5,
    )

    assert ev.reference_conflict is False


def test_final_decision_waits_when_only_one_engine_has_direction():
    now = time.time()
    contract = _contract()
    ev = EvidenceEngine().analyze(now, 64040.0, contract, [], [], [], [])
    # Explicitly reproduce the screenshot's partial-alignment case.
    early = EarlyPredictionReport(
        active_window=True, signal=Signal.UP, confidence=60.0,
        up_prob=0.60, down_prob=0.40, rationale="test",
    )
    weakness = WeaknessEngine().analyze(ev, 0.2, 0.2, True)
    regime = RegimeReport(
        regime=MarketRegime.SHAKY, volatility_percentile=30.0,
        chop_index=0.6, min_confidence_required=59.0, explanation="test",
    )

    final = FinalDecisionEngine().decide(ev, weakness, regime, early, contract, {})

    assert final.agreement_status == "PARTIAL_ALIGNMENT"
    assert final.signal == Signal.WAIT
    assert "not independently confirmed" in final.rationale


def test_final_decision_waits_when_independent_reference_conflicts():
    now = time.time()
    contract = _contract()
    ev = EvidenceEngine().analyze(
        now, 64040.0, contract, [], [], [], [],
        reference_price=64000.0, reference_age_sec=0.5,
    )
    early = EarlyPredictionReport(
        active_window=True, signal=Signal.UP, confidence=70.0,
        up_prob=0.70, down_prob=0.30, rationale="test",
    )
    weakness = WeaknessEngine().analyze(ev, 0.2, 0.2, True)
    regime = RegimeReport(
        regime=MarketRegime.STABLE, volatility_percentile=20.0,
        chop_index=0.2, min_confidence_required=54.0, explanation="test",
    )

    final = FinalDecisionEngine().decide(ev, weakness, regime, early, contract, {})

    assert ev.reference_conflict is True
    assert final.signal == Signal.WAIT
    assert "independent USD reference" in final.rationale


def test_final_decision_waits_without_official_brti_even_if_engines_agree():
    now = time.time()
    contract = _contract()
    ev = EvidenceEngine().analyze(now, 64040.0, contract, [], [], [], [])
    ev.signal = Signal.UP
    early = EarlyPredictionReport(
        active_window=True, signal=Signal.UP, confidence=80.0,
        up_prob=0.80, down_prob=0.20, rationale="test",
    )
    weakness = WeaknessEngine().analyze(ev, 0.2, 0.2, True)
    regime = RegimeReport(
        regime=MarketRegime.STABLE, volatility_percentile=20.0,
        chop_index=0.2, min_confidence_required=54.0, explanation="test",
    )

    final = FinalDecisionEngine().decide(ev, weakness, regime, early, contract, {})

    assert final.agreement_status == "STRONG_CONSENSUS"
    assert final.signal == Signal.WAIT
    assert "Official CF Benchmarks BRTI is unavailable" in final.rationale


def test_resampled_radar_history_covers_full_15_minute_window():
    now = time.time()
    bars = [(int(now - 900 + i), 64000.0 + i, 1.0) for i in range(901)]

    prices = EvidenceEngine._resample_prices_to_seconds(bars, now, 64900.0)

    assert len(prices) == 901
    assert prices[0] == 64000.0
    assert prices[-1] == 64900.0
