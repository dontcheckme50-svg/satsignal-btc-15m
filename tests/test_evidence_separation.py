from app.engines import EvidenceEngine
from tests.test_system import _contract


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
