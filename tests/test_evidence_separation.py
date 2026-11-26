from app.engines import EvidenceEngine
from app.models import ContractInfo


def _contract(open_timestamp: float, seconds_remaining: float) -> ContractInfo:
    return ContractInfo(
        ticker="TEST-BTC-15M", strike_price=90000.0,
        strike_source="KALSHI_OFFICIAL", open_timestamp=open_timestamp,
        close_timestamp=open_timestamp + 900.0,
        seconds_remaining=seconds_remaining,
        checkpoint_seconds_remaining=60.0,
        is_official_kalshi=True,
    )


def test_valid_brti_changes_strike_probability_not_tape_evidence_score():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    engine = EvidenceEngine()

    no_brti = engine.analyze(now, 90010.0, contract, [], [], [], [])
    bullish_brti = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        official_brti_price=90040.0, official_brti_age_sec=0.5,
    )
    bearish_brti = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        official_brti_price=89980.0, official_brti_age_sec=0.5,
    )

    assert no_brti.raw_score == bullish_brti.raw_score == bearish_brti.raw_score
    assert bullish_brti.strike_z_score > bearish_brti.strike_z_score


def test_eligible_synthetic_benchmark_is_supplemental_and_explicit():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    engine = EvidenceEngine()

    baseline = engine.analyze(now, 90010.0, contract, [], [], [], [])
    synthetic_up = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        synthetic_benchmark_price=90040.0,
        synthetic_benchmark_age_sec=1.0,
        synthetic_benchmark_eligible=True,
    )
    synthetic_down = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        synthetic_benchmark_price=89980.0,
        synthetic_benchmark_age_sec=1.0,
        synthetic_benchmark_eligible=True,
    )

    assert synthetic_up.synthetic_benchmark_used is True
    assert synthetic_up.synthetic_benchmark_price == 90040.0
    assert synthetic_up.strike_z_score > baseline.strike_z_score > synthetic_down.strike_z_score
    assert synthetic_up.raw_score == baseline.raw_score
    expected_without_brti = 0.90 * EvidenceEngine._strike_z_score(90010.0, contract.strike_price, contract.seconds_remaining, baseline.volatility_annualized) + 0.10 * EvidenceEngine._strike_z_score(90040.0, contract.strike_price, contract.seconds_remaining, baseline.volatility_annualized)
    assert synthetic_up.strike_z_score == round(expected_without_brti, 6)


def test_ineligible_or_stale_synthetic_benchmark_is_ignored():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    engine = EvidenceEngine()

    baseline = engine.analyze(now, 90010.0, contract, [], [], [], [])
    ineligible = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        synthetic_benchmark_price=89900.0,
        synthetic_benchmark_age_sec=1.0,
        synthetic_benchmark_eligible=False,
    )
    stale = engine.analyze(
        now, 90010.0, contract, [], [], [], [],
        synthetic_benchmark_price=89900.0,
        synthetic_benchmark_age_sec=5.01,
        synthetic_benchmark_eligible=True,
    )

    assert ineligible.synthetic_benchmark_used is False
    assert stale.synthetic_benchmark_used is False
    assert ineligible.strike_z_score == baseline.strike_z_score
    assert stale.strike_z_score == baseline.strike_z_score


def test_synthetic_benchmark_cannot_outweigh_fresh_official_brti():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    engine = EvidenceEngine()

    # Keep the spot/reference gap large enough to survive the engine's
    # six-decimal strike_z_score output rounding.
    baseline = engine.analyze(now, 89000.0, contract, [], [], [], [])
    brti_only = engine.analyze(
        now, 89000.0, contract, [], [], [], [],
        official_brti_price=91000.0, official_brti_age_sec=0.5,
    )
    both = engine.analyze(
        now, 89000.0, contract, [], [], [], [],
        official_brti_price=91000.0, official_brti_age_sec=0.5,
        synthetic_benchmark_price=87000.0,
        synthetic_benchmark_age_sec=1.0,
        synthetic_benchmark_eligible=True,
    )

    # Give the synthetic reference a deliberately opposing value so its
    # 10% weight is measurable; official BRTI retains 25% weight.
    synthetic_increment = abs(both.strike_z_score - brti_only.strike_z_score)
    brti_increment = abs(brti_only.strike_z_score - baseline.strike_z_score)
    assert both.synthetic_benchmark_used is True
    assert 0.0 < synthetic_increment < brti_increment
