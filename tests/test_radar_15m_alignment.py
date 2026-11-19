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



def test_early_engine_ignores_strike_distance_and_waits_without_flow():
    now = time.time()
    contract = _contract()
    base = EvidenceEngine().analyze(now, 64040.0, contract, [], [], [], [])
    weakness = WeaknessEngine().analyze(base, 0.2, 0.2, True)

    early = EarlyPredictionEngine().analyze(base, weakness, contract)

    assert early.signal == Signal.WAIT
    assert abs(early.up_prob - 0.5) < 0.01


def test_early_engine_probability_is_independent_of_strike_distance():
    now = time.time()
    contract = _contract()
    base = EvidenceEngine().analyze(now, 64040.0, contract, [], [], [], [])
    weakness = WeaknessEngine().analyze(base, 0.2, 0.2, True)
    far_above = base.model_copy(update={"strike_z_score": 8.0})
    far_below = base.model_copy(update={"strike_z_score": -8.0})

    up_case = EarlyPredictionEngine().analyze(far_above, weakness, contract)
    down_case = EarlyPredictionEngine().analyze(far_below, weakness, contract)

    assert up_case.up_prob == down_case.up_prob
    assert up_case.signal == down_case.signal


def test_final_probability_does_not_count_strike_z_score_twice():
    now = time.time()
    contract = _contract()
    base = EvidenceEngine().analyze(now, 64040.0, contract, [], [], [], [])
    weakness = WeaknessEngine().analyze(base, 0.2, 0.2, True)
    early = EarlyPredictionReport(
        active_window=True, signal=Signal.UP, confidence=65.0,
        up_prob=0.65, down_prob=0.35, rationale="flow-confirmed test",
    )
    regime = RegimeReport(
        regime=MarketRegime.STABLE, volatility_percentile=20.0,
        chop_index=0.2, min_confidence_required=54.0, explanation="test",
    )
    extreme_positive_z = base.model_copy(update={"raw_score": 12.0, "strike_z_score": 9.0})
    extreme_negative_z = base.model_copy(update={"raw_score": 12.0, "strike_z_score": -9.0})

    first = FinalDecisionEngine().decide(extreme_positive_z, weakness, regime, early, contract, {})
    second = FinalDecisionEngine().decide(extreme_negative_z, weakness, regime, early, contract, {})

    assert first.up_probability == second.up_probability
    assert first.down_probability == second.down_probability


def test_final_probability_is_directionally_symmetric():
    now = time.time()
    contract = _contract()
    base = EvidenceEngine().analyze(now, 64000.0, contract, [], [], [], [])
    weakness = WeaknessEngine().analyze(base, 0.2, 0.2, True)
    regime = RegimeReport(
        regime=MarketRegime.STABLE, volatility_percentile=20.0,
        chop_index=0.2, min_confidence_required=54.0, explanation="test",
    )
    up_ev = base.model_copy(update={"raw_score": 35.0})
    down_ev = base.model_copy(update={"raw_score": -35.0})
    up_early = EarlyPredictionReport(
        active_window=True, signal=Signal.UP, confidence=70.0,
        up_prob=0.70, down_prob=0.30, rationale="test",
    )
    down_early = EarlyPredictionReport(
        active_window=True, signal=Signal.DOWN, confidence=70.0,
        up_prob=0.30, down_prob=0.70, rationale="test",
    )

    up_result = FinalDecisionEngine().decide(up_ev, weakness, regime, up_early, contract, {})
    down_result = FinalDecisionEngine().decide(down_ev, weakness, regime, down_early, contract, {})

    assert abs(up_result.up_probability - down_result.down_probability) < 0.001
    assert abs(up_result.down_probability - down_result.up_probability) < 0.001
