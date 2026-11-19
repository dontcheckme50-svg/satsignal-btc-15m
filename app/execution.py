import math, time
from app.config import settings
from app.database import PredictionLedger
from app.feeds import MarketDataHub, iso_now, safe_age_seconds
from app.models import ContractInfo, FinalDecisionReport, Signal, TradeRecord


def kalshi_taker_fee(price: float) -> float:
    # Kalshi fee estimate in USD per contract; verify current fee schedule before live use.
    return math.ceil(0.07 * price * (1.0 - price) * 100.0) / 100.0


class ExecutionManager:
    """Paper-only simulator. It never submits live/demo orders."""

    def __init__(self, hub: MarketDataHub, ledger: PredictionLedger):
        self.hub = hub
        self.ledger = ledger
        self.max_contracts_per_window = 20

    async def evaluate_and_execute(
        self,
        now: float,
        contract: ContractInfo,
        decision: FinalDecisionReport,
        baseline_z_prob: float | None = None,
    ) -> TradeRecord | None:
        # Hard safety gates: no synthetic contracts, no proxy quotes, no live mode.
        if settings.trading_mode != "paper":
            return None
        if not contract.is_official_kalshi:
            return None
        if not contract.ticker.startswith("KXBTC15M"):
            return None
        if contract.strike_source != "KALSHI_OFFICIAL":
            return None
        if decision.signal == Signal.WAIT:
            return None
        # A prediction snapshot can be delayed across a 15-minute rollover.
        # Require the live feed's current contract to match the complete identity
        # of the snapshot before considering a paper entry.
        active_market = self.hub.kalshi_contract
        if (
            active_market is None
            or active_market.ticker != contract.ticker
            or active_market.open_timestamp != contract.open_timestamp
            or active_market.close_timestamp != contract.close_timestamp
            or active_market.strike_price != contract.strike_price
            or not active_market.is_official_kalshi
            or active_market.strike_source != "KALSHI_OFFICIAL"
        ):
            return None

        # Recompute against the actual wall clock; snapshot countdowns can be
        # stale if the prediction/execution cycle was delayed.
        execution_now = time.time()
        if not (active_market.open_timestamp <= execution_now < active_market.close_timestamp):
            return None
        if (active_market.close_timestamp - execution_now) < settings.final_lockout_seconds:
            return None
        spot_age = safe_age_seconds(self.hub.last_spot_ts, execution_now)
        kalshi_age = safe_age_seconds(self.hub.last_kalshi_ts, execution_now)
        if spot_age is None or spot_age > settings.stale_feed_threshold_sec:
            return None
        book_age = safe_age_seconds(self.hub.last_book_ts, execution_now)
        book_source_matches = bool(
            self.hub.active_spot_source
            and self.hub.last_book_source == self.hub.active_spot_source
        )
        if (
            book_age is None
            or book_age > settings.stale_feed_threshold_sec
            or not book_source_matches
            or not self.hub.bids
            or not self.hub.asks
        ):
            return None
        if kalshi_age is None or kalshi_age > 12.0:
            return None
        sb = self.ledger.get_scoreboard()
        if sb.daily_loss_locked:
            return None

        # Only one paper entry per contract prevents repeated per-second orders.
        if self.ledger.count_trades_for_ticker(contract.ticker) > 0:
            return None

        side = "YES" if decision.signal == Signal.UP else "NO"
        fair_prob = decision.up_probability if side == "YES" else decision.down_probability
        # Price from the freshest live quote, not the possibly older snapshot.
        ask = active_market.yes_ask_dollars if side == "YES" else active_market.no_ask_dollars

        # Validate model probability and market quote before fee/Kelly arithmetic.
        if not math.isfinite(float(fair_prob)) or not (0.0 <= float(fair_prob) <= 1.0):
            return None
        if ask is None or not math.isfinite(float(ask)) or not (0.01 <= float(ask) <= 0.99):
            return None

        ask_price = round(float(ask), 2)
        fee_per_contract = kalshi_taker_fee(ask_price)
        total_cost = ask_price + fee_per_contract
        net_edge = round(fair_prob - total_cost, 4)
        if net_edge < settings.min_net_edge_cents or total_cost >= 0.99:
            return None

        full_kelly = max(0.0, (fair_prob - total_cost) / (1.0 - total_cost))
        wager = min(
            sb.bankroll_usd * full_kelly * settings.kelly_fraction,
            settings.max_risk_per_trade_usd,
        )
        qty = min(
            max(0, int(wager // total_cost)),
            self.max_contracts_per_window,
        )
        if qty <= 0:
            return None

        rec = TradeRecord(
            mode="paper",
            contract_ticker=contract.ticker,
            timestamp_iso=iso_now(execution_now),
            side=side,
            contracts=qty,
            entry_price=ask_price,
            quote_source="KALSHI_LIVE_BOOK",
            fee_paid=round(fee_per_contract * qty, 4),
            fair_prob_at_entry=fair_prob,
            net_edge=net_edge,
            status="OPEN",
            notes="Paper-only simulated fill using observed Kalshi ask; no order submitted.",
        )
        rec.id = self.ledger.record_trade(rec)
        return rec
