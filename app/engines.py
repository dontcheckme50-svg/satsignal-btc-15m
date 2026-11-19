import math
from typing import List, Optional, Tuple
import numpy as np
from scipy.stats import norm
from app.config import settings
from app.models import ContractInfo, EarlyPredictionReport, EvidenceMetrics, FinalDecisionReport, MarketRegime, RegimeReport, Signal, WeaknessReport

class EvidenceEngine:
    def analyze(self, now: float, live_price: float, contract: ContractInfo, trades: List[Tuple[float, float, float, bool]], bars_1s: List[Tuple[int, float, float]], bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], official_brti_price: Optional[float] = None, official_brti_age_sec: Optional[float] = None) -> EvidenceMetrics:
        dist = live_price - contract.strike_price
        dist_pct = (dist / contract.strike_price * 100.0) if contract.strike_price > 0 else 0.0
        recent_trades = [t for t in trades if contract.open_timestamp <= t[0] and now - 60.0 <= t[0] <= now]
        buy_vol_60s = sum(t[2] for t in recent_trades if not t[3])
        sell_vol_60s = sum(t[2] for t in recent_trades if t[3])
        tot_vol_60s = buy_vol_60s + sell_vol_60s
        buy_pct, sell_pct = (round((buy_vol_60s / tot_vol_60s) * 100.0, 1), round((sell_vol_60s / tot_vol_60s) * 100.0, 1)) if tot_vol_60s > 0 else (50.0, 50.0)
        delta_60s = round(buy_vol_60s - sell_vol_60s, 4)
        # Only use observed trades within this contract and at/before the snapshot time.
        cvd = sum((t[2] if not t[3] else -t[2]) for t in trades if contract.open_timestamp <= t[0] <= now)
        trades_10m = [t for t in trades if contract.open_timestamp <= t[0] and now - 600.0 <= t[0] <= now]
        # Normalize against the amount of observed history. A short startup
        # sample must not be divided by a full ten minutes and create a false
        # volume-spike signal.
        if trades_10m:
            observed_seconds = min(600.0, now - min(t[0] for t in trades_10m))
            avg_1m_vol = (sum(t[2] for t in trades_10m) / (observed_seconds / 60.0)) if observed_seconds >= 60.0 else 0.0
        else:
            avg_1m_vol = 0.0
        vol_ratio = round(tot_vol_60s / avg_1m_vol, 2) if avg_1m_vol > 1e-6 else 1.0
        bid_depth, ask_depth = sum(q for _, q in bids[:10]), sum(q for _, q in asks[:10])
        obi = (bid_depth - ask_depth) / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0.0
        all_sizes = [q for _, q in bids[:10]] + [q for _, q in asks[:10]]
        med_sz = float(np.median(all_sizes)) if all_sizes else 0.0
        large_bids = sum(q for _, q in bids[:10] if med_sz > 0 and q >= 2.5 * med_sz)
        large_asks = sum(q for _, q in asks[:10] if med_sz > 0 and q >= 2.5 * med_sz)
        large_bias = (large_bids - large_asks) / (large_bids + large_asks) if (large_bids + large_asks) > 0 else 0.0
        # Only recent, source-observed bars may influence short-horizon signals.
        # Rebuild a one-second time series so sparse trade bars do not make
        # "last 30 observations" silently mean an arbitrary number of seconds.
        valid_bars = [b for b in bars_1s if contract.open_timestamp <= b[0] and now - 300.0 <= b[0] <= now]
        prices = self._resample_prices_to_seconds(valid_bars, now, live_price)
        rsi_14 = self._compute_rsi(prices[-60:], period=14)
        ann_vol = self._compute_annualized_vol(prices)
        p_30s = prices[-30] if len(prices) >= 30 else prices[0]
        p_120s = prices[-120] if len(prices) >= 120 else prices[0]
        ret_30s = (live_price - p_30s) / p_30s if p_30s > 0 else 0.0
        ret_120s = (live_price - p_120s) / p_120s if p_120s > 0 else 0.0
        mom_score = float(np.clip((ret_30s * 18000.0) + (ret_120s * 9000.0), -100.0, 100.0))
        structure = self._detect_structure(prices[-90:])
        spot_z = self._strike_z_score(live_price, contract.strike_price, contract.seconds_remaining, ann_vol)
        brti_age = official_brti_age_sec
        brti_valid = (
            official_brti_price is not None
            and math.isfinite(float(official_brti_price))
            and float(official_brti_price) > 0
            and brti_age is not None
            and math.isfinite(float(brti_age))
            and 0.0 <= float(brti_age) <= settings.stale_feed_threshold_sec
        )
        brti_distance = (float(official_brti_price) - contract.strike_price) if brti_valid else None
        brti_z = self._strike_z_score(float(official_brti_price), contract.strike_price, contract.seconds_remaining, ann_vol) if brti_valid else None
        # Spot remains the primary price input; valid official BRTI contributes
        # an independent settlement-benchmark confirmation, never a fallback.
        strike_z = (0.65 * spot_z + 0.35 * brti_z) if brti_z is not None else spot_z
        brti_spread_bps = (
            ((float(official_brti_price) - live_price) / live_price) * 10000.0
            if brti_valid and live_price > 0 else None
        )
        brti_conflict = bool(
            brti_valid
            and dist * float(brti_distance) < 0
            and abs(dist / contract.strike_price * 10000.0) >= 0.5
            and abs(float(brti_distance) / contract.strike_price * 10000.0) >= 0.5
        )
        raw_score = float(np.clip(((buy_pct - 50.0) / 50.0) * 30.0 + (0.65 * obi + 0.35 * large_bias) * 25.0 + (mom_score / 100.0) * 25.0 + float(np.clip(strike_z * 12.0, -20.0, 20.0)), -100.0, 100.0))
        sig = Signal.UP if raw_score >= 18.0 else (Signal.DOWN if raw_score <= -18.0 else Signal.WAIT)
        return EvidenceMetrics(live_price=round(live_price, 2), distance_from_strike=round(dist, 2), distance_pct=round(dist_pct, 4), buying_pressure_pct=buy_pct, selling_pressure_pct=sell_pct, trade_delta_60s=delta_60s, cvd_contract=round(cvd, 4), volume_ratio=vol_ratio, momentum_score=round(mom_score, 1), rsi_14=round(rsi_14, 1), volatility_annualized=round(ann_vol, 4), orderbook_imbalance=round(obi, 3), large_order_bias=round(large_bias, 3), structure_bias=structure, raw_score=round(raw_score, 1), signal=sig, official_brti_price=round(float(official_brti_price), 2) if brti_valid else None, official_brti_age_seconds=round(float(brti_age), 3) if brti_valid else None, brti_distance_from_strike=round(float(brti_distance), 2) if brti_valid else None, brti_spot_spread_bps=round(brti_spread_bps, 3) if brti_spread_bps is not None else None, brti_conflict=brti_conflict, strike_z_score=round(strike_z, 6))

    @staticmethod
    def _resample_prices_to_seconds(
        bars: List[Tuple[int, float, float]], now: float, live_price: float
    ) -> List[float]:
        """Return at most 301 one-second prices, carrying the last observed trade forward."""
        end_sec = int(now)
        start_sec = max(end_sec - 300, min((int(bar[0]) for bar in bars), default=end_sec))
        observed = {}
        for ts, price, _volume in bars:
            sec, value = int(ts), float(price)
            if start_sec <= sec <= end_sec and math.isfinite(value) and value > 0:
                observed[sec] = value
        observed[end_sec] = float(live_price)
        result = []
        last_price = float(live_price)
        for sec in range(start_sec, end_sec + 1):
            if sec in observed:
                last_price = observed[sec]
            result.append(last_price)
        return result

    @staticmethod
    def _compute_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1: return 50.0
        deltas = np.diff(prices)
        avg_gain = float(np.mean(np.where(deltas > 0, deltas, 0.0)[-period:]))
        avg_loss = float(np.mean(np.where(deltas < 0, -deltas, 0.0)[-period:]))
        if avg_loss == 0: return 100.0 if avg_gain > 0 else 50.0
        return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

    @staticmethod
    def _compute_annualized_vol(prices: List[float]) -> float:
        if len(prices) < 10: return 0.45
        arr = np.array(prices, dtype=float)
        log_rets = np.diff(np.log(arr[arr > 0]))
        if len(log_rets) < 5: return 0.45
        return float(np.clip(float(np.std(log_rets)) * math.sqrt(365.25 * 86400.0), 0.15, 2.50))

    @staticmethod
    def _detect_structure(prices: List[float]) -> str:
        if len(prices) < 30: return "COMPRESSION"
        third = len(prices) // 3
        p1, p2, p3 = prices[:third], prices[third:2*third], prices[2*third:]
        if max(p3) > max(p2) > max(p1) and min(p3) >= min(p1): return "BULLISH_HH_HL"
        if min(p3) < min(p2) < min(p1) and max(p3) <= max(p1): return "BEARISH_LH_LL"
        return "COMPRESSION"

    @staticmethod
    def _strike_z_score(spot: float, strike: float, secs_rem: float, ann_vol: float) -> float:
        if strike <= 0 or spot <= 0: return 0.0
        denom = ann_vol * math.sqrt(max(secs_rem, 5.0) / (365.25 * 86400.0))
        return (math.log(spot / strike) / denom) if denom > 1e-8 else 0.0

class WeaknessEngine:
    def analyze(
        self,
        ev: EvidenceMetrics,
        spot_age_sec: Optional[float],
        book_age_sec: Optional[float] = None,
        book_source_matches: bool = True,
    ) -> WeaknessReport:
        flags, penalty, absorption, vol_div, mom_fade, feed_bad = [], 0.0, False, False, False, False
        if spot_age_sec is None or spot_age_sec > settings.stale_feed_threshold_sec:
            feed_bad = True; penalty += 50.0; flags.append(f"Delayed or missing spot feed ({spot_age_sec}s old)")
        if not book_source_matches:
            feed_bad = True; penalty += 25.0; flags.append("Order-book source does not match the active spot feed")
        elif book_age_sec is None or book_age_sec > settings.stale_feed_threshold_sec:
            feed_bad = True; penalty += 25.0; flags.append(f"Delayed or missing order-book feed ({book_age_sec}s old)")
        if ev.brti_conflict:
            penalty += 20.0; flags.append("Official BRTI and spot are on opposite sides of the Kalshi strike")
        if ev.buying_pressure_pct >= 62.0 and ev.momentum_score < -10.0:
            absorption = True; penalty += 25.0; flags.append("Sell-wall absorption: heavy taker buying failing to lift price")
        elif ev.selling_pressure_pct >= 62.0 and ev.momentum_score > 10.0:
            absorption = True; penalty += 25.0; flags.append("Bid absorption: heavy taker selling absorbed without drop")
        if abs(ev.momentum_score) > 30.0 and ev.volume_ratio < 0.65:
            vol_div = True; penalty += 20.0; flags.append(f"Low-volume drift ({ev.volume_ratio}x baseline volume)")
        if (ev.orderbook_imbalance > 0.35 and ev.trade_delta_60s < -0.5) or (ev.orderbook_imbalance < -0.35 and ev.trade_delta_60s > 0.5):
            penalty += 18.0; flags.append("Book-to-tape conflict (resting depth opposes aggressive trades)")
        if (ev.rsi_14 >= 78.0 and ev.momentum_score > 0) or (ev.rsi_14 <= 22.0 and ev.momentum_score < 0):
            mom_fade = True; penalty += 15.0; flags.append(f"Extreme RSI ({ev.rsi_14}) exhaustion risk")
        if 46.0 <= ev.buying_pressure_pct <= 54.0 and abs(ev.momentum_score) < 12.0:
            penalty += 15.0; flags.append("Balanced/indecisive order flow")
        return WeaknessReport(weakness_score=round(min(100.0, penalty), 1), flags=flags, absorption_detected=absorption, volume_divergence=vol_div, momentum_fading=mom_fade, feed_degraded=feed_bad)

class RegimeEngine:
    def analyze(self, ev: EvidenceMetrics, weakness: WeaknessReport, bars_1s: List[Tuple[int, float, float]]) -> RegimeReport:
        prices = [b[1] for b in bars_1s[-180:]]
        chop = self._choppiness_ratio(prices)
        vol_pct = min(100.0, round((ev.volatility_annualized / 1.20) * 100.0, 1))
        if weakness.absorption_detected or (ev.volatility_annualized > 0.95 and chop > 0.72) or weakness.weakness_score >= 55.0:
            return RegimeReport(regime=MarketRegime.HIGH_RISK_REVERSAL, volatility_percentile=vol_pct, chop_index=round(chop, 3), min_confidence_required=66.0, explanation="Elevated reversal/absorption hazard; strict probability hurdle enforced.")
        if chop > 0.58 or weakness.weakness_score >= 30.0 or ev.volume_ratio < 0.75:
            return RegimeReport(regime=MarketRegime.SHAKY, volatility_percentile=vol_pct, chop_index=round(chop, 3), min_confidence_required=59.0, explanation="Two-way chop or mixed flow; moderate probability hurdle required.")
        return RegimeReport(regime=MarketRegime.STABLE, volatility_percentile=vol_pct, chop_index=round(chop, 3), min_confidence_required=54.0, explanation="Clean directional structure with coherent volume and volatility.")

    @staticmethod
    def _choppiness_ratio(prices: List[float]) -> float:
        if len(prices) < 15: return 0.5
        path_len = float(np.sum(np.abs(np.diff(prices))))
        if path_len <= 1e-9: return 0.5
        return float(np.clip(1.0 - (abs(prices[-1] - prices[0]) / path_len), 0.0, 1.0))

class EarlyPredictionEngine:
    def analyze(self, ev: EvidenceMetrics, weakness: WeaknessReport, contract: ContractInfo) -> EarlyPredictionReport:
        in_early_window = max(0.0, 900.0 - contract.seconds_remaining) <= settings.early_window_seconds

        # This engine is intentionally independent of strike-distance probability.
        # FinalDecisionEngine already models distance-to-strike; reusing it here
        # would count the same price-location evidence twice and inflate confidence.
        # Use only short-horizon tape, momentum, order-book and structure evidence.
        delta_scale = max(0.75, abs(ev.trade_delta_60s) + 0.75)
        flow_component = math.tanh(ev.trade_delta_60s / delta_scale)
        pressure_component = (ev.buying_pressure_pct - ev.selling_pressure_pct) / 100.0
        momentum_component = float(np.clip(ev.momentum_score / 100.0, -1.0, 1.0))
        structure_component = (
            1.0 if ev.structure_bias == "BULLISH_HH_HL"
            else -1.0 if ev.structure_bias == "BEARISH_LH_LL"
            else 0.0
        )
        # Keep the model bounded and symmetric so absent/neutral evidence cannot
        # create a default UP bias.
        early_logit = (
            0.55 * flow_component
            + 0.35 * pressure_component
            + 0.40 * momentum_component
            + 0.20 * structure_component
            + 0.20 * float(np.clip(ev.orderbook_imbalance, -1.0, 1.0))
        )
        if weakness.absorption_detected:
            early_logit *= 0.45
        if weakness.momentum_fading:
            early_logit *= 0.70
        penalty_factor = 1.0 - min(0.65, max(0.0, weakness.weakness_score) / 140.0)
        early_logit *= penalty_factor

        up_prob = round(float(np.clip(norm.cdf(early_logit), 0.05, 0.95)), 4)
        down_prob = round(1.0 - up_prob, 4)
        confidence = round(max(up_prob, down_prob) * 100.0, 1)
        if weakness.feed_degraded or confidence < 55.0:
            sig, rationale = Signal.WAIT, "Early flow evidence is weak or market feeds are degraded."
        elif up_prob > 0.5:
            sig, rationale = Signal.UP, (
                f"Independent early tape evidence favors upside "
                f"(60s delta {ev.trade_delta_60s:+.3f}, momentum {ev.momentum_score:+.1f})."
            )
        else:
            sig, rationale = Signal.DOWN, (
                f"Independent early tape evidence favors downside "
                f"(60s delta {ev.trade_delta_60s:+.3f}, momentum {ev.momentum_score:+.1f})."
            )
        return EarlyPredictionReport(
            active_window=in_early_window, signal=sig, confidence=confidence,
            up_prob=up_prob, down_prob=down_prob, rationale=rationale
        )

class FinalDecisionEngine:
    def decide(self, ev: EvidenceMetrics, weakness: WeaknessReport, regime: RegimeReport, early: EarlyPredictionReport, contract: ContractInfo, weights: dict) -> FinalDecisionReport:
        # Use the exact strike-distance evidence produced upstream. This carries
        # fresh official BRTI confirmation through to the final probability while
        # retaining spot-only behavior for legacy EvidenceMetrics payloads.
        z_terminal = (
            float(ev.strike_z_score)
            if ev.strike_z_score is not None and math.isfinite(float(ev.strike_z_score))
            else EvidenceEngine._strike_z_score(
                ev.live_price, contract.strike_price,
                contract.seconds_remaining, ev.volatility_annualized,
            )
        )
        micro_drift_z = ((ev.raw_score / 100.0) * 0.40 * weights.get("evidence", 1.0)) + ((early.up_prob - 0.5) * 0.45 * weights.get("early", 1.0))
        raw_up_prob = float(np.clip(norm.cdf(z_terminal + micro_drift_z), 0.02, 0.98))
        shrink = min(0.70, weakness.weakness_score / 130.0)
        up_prob = round(0.5 + (raw_up_prob - 0.5) * (1.0 - shrink), 4)
        down_prob = round(1.0 - up_prob, 4)
        directional_votes = [s for s in (ev.signal, early.signal) if s != Signal.WAIT]
        details: List[str] = []
        if len(directional_votes) == 2 and directional_votes[0] == directional_votes[1]:
            agreement = "STRONG_CONSENSUS"; details.append(f"Evidence & Early engines both confirm {directional_votes[0].value}.")
        elif len(directional_votes) == 2:
            agreement = "CONFLICTED"; details.append(f"Engine disagreement: Evidence={ev.signal.value} vs Early={early.signal.value}.")
        elif len(directional_votes) == 1:
            agreement = "PARTIAL_ALIGNMENT"; details.append(f"One engine directional ({directional_votes[0].value}), other neutral.")
        else:
            agreement = "CONFLICTED"; details.append("Both Evidence and Early engines report insufficient directional edge.")
        if weakness.flags: details.extend(weakness.flags[:2])
        confidence = round(max(up_prob, down_prob) * 100.0, 1)
        if weakness.feed_degraded:
            final_sig, rationale = Signal.WAIT, "WAIT: Live market feed is stale or degraded."
        elif not contract.is_official_kalshi or contract.strike_source != "KALSHI_OFFICIAL":
            final_sig, rationale = Signal.WAIT, "WAIT: Verified official Kalshi contract and strike unavailable; proxy contract is non-actionable."
        elif contract.seconds_remaining < settings.final_lockout_seconds:
            final_sig, rationale = Signal.WAIT, "WAIT: Final 65s settlement averaging window active; avoiding late entry."
        elif agreement == "CONFLICTED" and len(directional_votes) == 2:
            final_sig, rationale = Signal.WAIT, "WAIT: Independent engines disagree on direction."
        elif confidence < regime.min_confidence_required:
            final_sig, rationale = Signal.WAIT, f"WAIT: Calibrated probability {confidence}% below {regime.regime.value} hurdle ({regime.min_confidence_required}%)."
        elif up_prob > 0.5:
            final_sig, rationale = Signal.UP, f"UP ({confidence:.1f}% prob): Price ${ev.distance_from_strike:+.2f} vs strike under {regime.regime.value}."
        else:
            final_sig, rationale = Signal.DOWN, f"DOWN ({confidence:.1f}% prob): Price ${ev.distance_from_strike:+.2f} vs strike under {regime.regime.value}."
        return FinalDecisionReport(signal=final_sig, confidence=confidence, up_probability=up_prob, down_probability=down_prob, agreement_status=agreement, agreement_details=details, rationale=rationale)
