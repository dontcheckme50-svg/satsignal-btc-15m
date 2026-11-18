from app.database import PredictionLedger
from app.feeds import MarketDataHub
from app.models import ContractInfo, FinalDecisionReport, MarketRegime, PredictionRecord, Signal
from app.orchestrator import PredictorOrchestrator


def _official_contract(ticker: str, now: float, remaining: float = 800.0, strike: float = 90000.0):
    return ContractInfo(
        ticker=ticker,
        strike_price=strike,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 10.0,
        close_timestamp=now + remaining,
        seconds_remaining=remaining,
        checkpoint_seconds_remaining=remaining % 180 or 180.0,
        is_official_kalshi=True,
        yes_bid_dollars=0.20,
        yes_ask_dollars=0.21,
        no_bid_dollars=0.79,
        no_ask_dollars=0.80,
    )


def _ready_hub(contract: ContractInfo, now: float, price: float = 90010.0):
    hub = MarketDataHub()
    hub.live_price = price
    hub.last_spot_ts = now
    hub.active_spot_source = "TEST_LIVE"
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    return hub


def test_contract_rollover_resets_observation_throttle(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "rollover-observations.db"))
    now = 1_800_000_000.0
    first = _official_contract("KXBTC15M-FIRST", now)
    hub = _ready_hub(first, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    first_snapshot = orchestrator.evaluate_once(now=now)
    assert first_snapshot.contract.ticker == first.ticker
    first_obs = ledger.get_recent_prediction_observations(limit=10)
    assert len(first_obs) == 1
    assert first_obs[0]["contract_ticker"] == first.ticker

    next_now = now + 1.0
    second = _official_contract("KXBTC15M-SECOND", next_now, strike=90100.0)
    hub.kalshi_contract = second
    hub.last_kalshi_ts = next_now
    hub.live_price = 90110.0
    hub.last_spot_ts = next_now

    second_snapshot = orchestrator.evaluate_once(now=next_now)
    observations = ledger.get_recent_prediction_observations(limit=10)
    assert second_snapshot.contract.ticker == second.ticker
    assert {item["contract_ticker"] for item in observations} == {first.ticker, second.ticker}
    assert len(observations) == 2
    # Observations are not scored predictions.
    assert ledger.get_recent_predictions(limit=10) == []
    assert ledger.get_scoreboard().total_predictions == 0


def test_final_lockout_creates_single_wait_lock_when_no_direction(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "final-lockout.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-LOCKOUT", now, remaining=64.0)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    snapshot = orchestrator.evaluate_once(now=now)

    records = ledger.get_recent_predictions()
    assert snapshot.contract.seconds_remaining == 64.0
    assert len(records) == 1
    assert records[0].contract_ticker == contract.ticker
    assert records[0].final_signal == Signal.WAIT
    assert ledger.has_official_prediction_lock(contract.ticker)
    assert ledger.get_scoreboard().total_predictions == 1


def test_synthetic_contract_never_creates_official_lock_or_observation(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "synthetic-recording.db"))
    now = 1_800_000_000.0
    hub = MarketDataHub()
    hub.live_price = 90010.0
    hub.last_spot_ts = now
    hub.active_spot_source = "TEST_LIVE"
    orchestrator = PredictorOrchestrator(hub, ledger)

    snapshot = orchestrator.evaluate_once(now=now)

    assert snapshot.contract.is_official_kalshi is False
    assert snapshot.contract.strike_source == "SYNTHETIC_OPEN_PROXY"
    assert ledger.get_recent_predictions() == []
    assert ledger.get_recent_prediction_observations() == []


def _prediction(ticker: str, ts: float, signal: Signal) -> PredictionRecord:
    return PredictionRecord(
        contract_ticker=ticker,
        timestamp_iso=f"2027-01-01 {int(ts)}",
        epoch_ts=ts,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=500.0,
        main_signal=signal,
        early_signal=signal,
        final_signal=signal,
        confidence=70.0,
        up_prob=0.70 if signal == Signal.UP else 0.30,
        down_prob=0.30 if signal == Signal.UP else 0.70,
        regime=MarketRegime.STABLE,
    )


def test_official_lock_is_idempotent_and_immutable_per_contract(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "unique-lock.db"))
    ticker = "KXBTC15M-UNIQUE"
    first_id = ledger.record_prediction(_prediction(ticker, 100.0, Signal.UP))
    second_id = ledger.record_prediction(_prediction(ticker, 130.0, Signal.DOWN))

    records = ledger.get_recent_predictions()
    assert first_id == second_id
    assert len(records) == 1
    assert records[0].final_signal == Signal.UP
    assert records[0].epoch_ts == 100.0
    assert ledger.get_scoreboard().total_predictions == 1


def test_observations_do_not_inflate_scoreboard_and_lock_settles_once(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "observation-scoreboard.db"))
    ticker = "KXBTC15M-OBS"
    for ts, sig in [(100.0, Signal.UP), (130.0, Signal.DOWN), (160.0, Signal.UP)]:
        ledger.record_prediction_observation(ticker, ts, 500.0, {"signal": sig.value, "prob": 0.7})
    ledger.record_prediction(_prediction(ticker, 100.0, Signal.UP))

    assert len(ledger.get_recent_prediction_observations(limit=10)) == 3
    assert ledger.get_scoreboard().total_predictions == 1
    assert ledger.settle_contract(ticker, 90020.0, official_result="UP") == 1
    assert ledger.settle_contract(ticker, 90020.0, official_result="UP") == 0
    sb = ledger.get_scoreboard()
    assert sb.final_scored == 1
    assert sb.final_wins == 1
    assert sb.final_losses == 0


def test_existing_snapshot_history_migrates_to_one_legacy_score_record(tmp_path):
    import sqlite3

    db_path = str(tmp_path / "legacy-history.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_ticker TEXT NOT NULL,
            timestamp_iso TEXT NOT NULL,
            epoch_ts REAL NOT NULL,
            strike_price REAL NOT NULL,
            btc_price_at_pred REAL NOT NULL,
            seconds_remaining REAL NOT NULL,
            main_signal TEXT NOT NULL,
            early_signal TEXT NOT NULL,
            final_signal TEXT NOT NULL,
            confidence REAL NOT NULL,
            up_prob REAL NOT NULL,
            down_prob REAL NOT NULL,
            regime TEXT NOT NULL,
            settled INTEGER NOT NULL DEFAULT 0,
            settlement_source TEXT,
            settlement_price REAL,
            actual_outcome TEXT,
            final_won INTEGER,
            main_won INTEGER,
            early_won INTEGER
        )
    """)
    for epoch, sig in [(100.0, "UP"), (130.0, "DOWN"), (160.0, "UP")]:
        conn.execute(
            """INSERT INTO predictions (contract_ticker, timestamp_iso, epoch_ts, strike_price,
               btc_price_at_pred, seconds_remaining, main_signal, early_signal, final_signal,
               confidence, up_prob, down_prob, regime, settled, actual_outcome, final_won)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("KXBTC15M-LEGACY", "2027-01-01 00:00:00", epoch, 90000.0, 90010.0,
             500.0, sig, sig, sig, 70.0, 0.7, 0.3, "STABLE MARKET", 1, "UP", 1),
        )
    conn.commit()
    conn.close()

    ledger = PredictionLedger(db_path)
    migrated = ledger.get_recent_predictions(limit=10)
    assert len(migrated) == 1
    assert migrated[0].epoch_ts == 160.0
    assert migrated[0].is_official_lock is True
    assert ledger.get_scoreboard().total_predictions == 1
    with ledger._connection() as conn:
        rows = conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE is_official_lock = 0").fetchone()
        assert rows["n"] == 2



def _final_report(signal: Signal) -> FinalDecisionReport:
    return FinalDecisionReport(
        signal=signal,
        confidence=72.0 if signal != Signal.WAIT else 0.0,
        up_probability=0.72 if signal == Signal.UP else 0.28 if signal == Signal.DOWN else 0.5,
        down_probability=0.28 if signal == Signal.UP else 0.72 if signal == Signal.DOWN else 0.5,
        agreement_status="TEST",
        rationale="Deterministic lifecycle test",
    )


def _evaluate_sequence(orchestrator, hub, ticker, start, signals, remaining=500.0):
    snapshots = []
    close_ts = start + remaining
    for offset, signal in enumerate(signals):
        ts = start + offset
        hub.kalshi_contract = ContractInfo(
            ticker=ticker,
            strike_price=90000.0,
            strike_source="KALSHI_OFFICIAL",
            open_timestamp=start - 10.0,
            close_timestamp=close_ts,
            seconds_remaining=close_ts - ts,
            checkpoint_seconds_remaining=(close_ts - ts) % 180 or 180.0,
            is_official_kalshi=True,
            yes_bid_dollars=0.20,
            yes_ask_dollars=0.21,
            no_bid_dollars=0.79,
            no_ask_dollars=0.80,
        )
        hub.last_kalshi_ts = ts
        hub.last_spot_ts = ts
        hub.live_price = 90010.0
        orchestrator.final_engine.decide = lambda *args, _signal=signal: _final_report(_signal)
        snapshots.append(orchestrator.evaluate_once(now=ts))
    return snapshots


def test_transient_directional_signal_is_not_officially_locked(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "transient-direction.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-TRANSIENT", now)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    _evaluate_sequence(orchestrator, hub, contract.ticker, now, [Signal.UP, Signal.WAIT])

    assert ledger.get_recent_predictions() == []
    assert not ledger.has_official_prediction_lock(contract.ticker)
    assert orchestrator._directional_lock_candidate_count == 0


def test_direction_reversal_resets_confirmation_and_requires_two_new_matches(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "reversal-confirmation.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-REVERSAL", now)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    _evaluate_sequence(
        orchestrator, hub, contract.ticker, now,
        [Signal.UP, Signal.DOWN, Signal.DOWN],
    )
    assert ledger.get_recent_predictions() == []
    assert orchestrator._directional_lock_candidate == Signal.DOWN
    assert orchestrator._directional_lock_candidate_count == 2

    _evaluate_sequence(orchestrator, hub, contract.ticker, now + 3.0, [Signal.DOWN])
    records = ledger.get_recent_predictions()
    assert len(records) == 1
    assert records[0].final_signal == Signal.DOWN


def test_two_consecutive_matching_directions_create_one_immutable_lock(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "confirmed-direction.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-CONFIRMED", now)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    _evaluate_sequence(orchestrator, hub, contract.ticker, now, [Signal.UP, Signal.UP, Signal.DOWN])

    records = ledger.get_recent_predictions()
    assert len(records) == 1
    assert records[0].final_signal == Signal.UP
    assert records[0].epoch_ts == now + 1.0
    assert ledger.get_scoreboard().total_predictions == 1


def test_unconfirmed_direction_at_final_lockout_becomes_wait(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "cutoff-confirmation.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-CUTOFF", now, remaining=66.0)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    _evaluate_sequence(
        orchestrator, hub, contract.ticker, now,
        [Signal.UP, Signal.UP], remaining=65.0,
    )

    records = ledger.get_recent_predictions()
    assert len(records) == 1
    assert records[0].final_signal == Signal.WAIT


def test_contract_rollover_resets_directional_confirmation(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "rollover-confirmation.db"))
    now = 1_800_000_000.0
    first = _official_contract("KXBTC15M-OLD", now)
    hub = _ready_hub(first, now)
    orchestrator = PredictorOrchestrator(hub, ledger)
    orchestrator.final_engine.decide = lambda *args: _final_report(Signal.UP)
    orchestrator.evaluate_once(now=now)
    assert orchestrator._directional_lock_candidate_count == 1

    next_now = now + 1.0
    second = _official_contract("KXBTC15M-NEW", next_now)
    hub.kalshi_contract = second
    hub.last_kalshi_ts = next_now
    hub.last_spot_ts = next_now
    hub.live_price = 90100.0
    orchestrator.evaluate_once(now=next_now)

    assert not ledger.has_official_prediction_lock(second.ticker)
    assert orchestrator._directional_lock_candidate_ticker == second.ticker
    assert orchestrator._directional_lock_candidate_count == 1
