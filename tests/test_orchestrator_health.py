import time
from types import SimpleNamespace

from app.models import ContractInfo
from app.orchestrator import PredictorOrchestrator


def _health_only_orchestrator():
    orchestrator = object.__new__(PredictorOrchestrator)
    orchestrator._running = True
    orchestrator.latest_snapshot = None
    orchestrator.last_cycle_started_ts = None
    orchestrator.last_cycle_success_ts = None
    orchestrator.last_cycle_duration_sec = None
    orchestrator.last_cycle_error = None
    orchestrator.consecutive_cycle_errors = 0
    orchestrator.total_cycle_errors = 0
    orchestrator.last_execution_error = None
    orchestrator.last_settlement_error = None
    orchestrator.active_contract = None
    orchestrator.hub = SimpleNamespace(
        last_spot_ts=0.0,
        last_book_ts=0.0,
        last_kalshi_ts=0.0,
        brti_status="UNAVAILABLE",
    )
    return orchestrator


def test_health_reports_warming_up_before_first_snapshot():
    orchestrator = _health_only_orchestrator()

    health = orchestrator.get_runtime_health()

    assert health["health"] == "WARMING_UP"
    assert health["has_snapshot"] is False
    assert health["last_cycle_age_seconds"] is None


def test_health_reports_degraded_when_cycles_are_failing_before_snapshot():
    orchestrator = _health_only_orchestrator()
    orchestrator.consecutive_cycle_errors = 1
    orchestrator.total_cycle_errors = 1
    orchestrator.last_cycle_error = "RuntimeError: feed failure"

    health = orchestrator.get_runtime_health()

    assert health["health"] == "DEGRADED"
    assert health["last_cycle_error"] == "RuntimeError: feed failure"


def test_health_reports_healthy_with_recent_successful_snapshot():
    orchestrator = _health_only_orchestrator()
    orchestrator.last_cycle_success_ts = time.time()
    orchestrator.latest_snapshot = object()

    health = orchestrator.get_runtime_health()

    assert health["health"] == "HEALTHY"
    assert health["has_snapshot"] is True


def test_health_reports_degraded_when_last_success_is_stale():
    orchestrator = _health_only_orchestrator()
    orchestrator.last_cycle_success_ts = time.time() - 6.0
    orchestrator.latest_snapshot = object()

    health = orchestrator.get_runtime_health()

    assert health["health"] == "DEGRADED"
    assert health["has_snapshot"] is True



def test_health_reports_stopped_when_worker_is_not_running():
    orchestrator = _health_only_orchestrator()
    orchestrator._running = False
    orchestrator.last_cycle_success_ts = time.time()
    orchestrator.latest_snapshot = object()

    health = orchestrator.get_runtime_health()

    assert health["health"] == "STOPPED"
    assert health["running"] is False



def test_health_reports_degraded_for_execution_failure_even_with_fresh_snapshot():
    orchestrator = _health_only_orchestrator()
    orchestrator.last_cycle_success_ts = time.time()
    orchestrator.latest_snapshot = object()
    orchestrator.last_execution_error = "RuntimeError: paper executor failed"

    health = orchestrator.get_runtime_health()

    assert health["health"] == "DEGRADED"
    assert health["last_execution_error"] == "RuntimeError: paper executor failed"

def test_market_data_readiness_is_separate_from_worker_health():
    orchestrator = _health_only_orchestrator()
    now = time.time()
    orchestrator.last_cycle_success_ts = now
    orchestrator.latest_snapshot = object()
    orchestrator.hub.last_spot_ts = now
    orchestrator.hub.last_book_ts = now
    orchestrator.hub.last_kalshi_ts = now
    orchestrator.active_contract = ContractInfo(
        ticker="KXBTC15M-HEALTH",
        strike_price=90000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 60.0,
        close_timestamp=now + 840.0,
        seconds_remaining=840.0,
        checkpoint_seconds_remaining=120.0,
        is_official_kalshi=True,
    )

    health = orchestrator.get_runtime_health()

    assert health["health"] == "HEALTHY"
    assert health["market_data_health"] == "READY"
    assert health["market_data_ready"] is True
    # BRTI entitlement is optional and must not make otherwise valid spot,
    # book, and official-contract data appear unavailable.
    assert health["official_brti_status"] == "UNAVAILABLE"


def test_market_data_readiness_degrades_when_order_book_is_stale():
    orchestrator = _health_only_orchestrator()
    now = time.time()
    orchestrator.last_cycle_success_ts = now
    orchestrator.latest_snapshot = object()
    orchestrator.hub.last_spot_ts = now
    orchestrator.hub.last_book_ts = now - 60.0
    orchestrator.hub.last_kalshi_ts = now
    orchestrator.active_contract = ContractInfo(
        ticker="KXBTC15M-STALE-BOOK",
        strike_price=90000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 60.0,
        close_timestamp=now + 840.0,
        seconds_remaining=840.0,
        checkpoint_seconds_remaining=120.0,
        is_official_kalshi=True,
    )

    health = orchestrator.get_runtime_health()

    assert health["health"] == "HEALTHY"
    assert health["market_data_health"] == "DEGRADED"
    assert health["market_data_ready"] is False
    assert health["order_book_age_seconds"] >= 60.0
