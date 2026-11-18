import time

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
