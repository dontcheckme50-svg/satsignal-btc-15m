from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

class Signal(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    WAIT = "WAIT"

class MarketRegime(str, Enum):
    STABLE = "STABLE MARKET"
    SHAKY = "SHAKY MARKET"
    HIGH_RISK_REVERSAL = "HIGH-RISK REVERSAL"

class FeedStatus(BaseModel):
    name: str
    status: str
    last_updated_iso: Optional[str] = None
    age_seconds: Optional[float] = None
    detail: str = ""

class ContractInfo(BaseModel):
    ticker: str
    strike_price: float
    strike_source: str
    open_timestamp: float
    close_timestamp: float
    seconds_remaining: float
    checkpoint_seconds_remaining: float
    is_official_kalshi: bool
    yes_bid_dollars: Optional[float] = None
    yes_ask_dollars: Optional[float] = None
    no_bid_dollars: Optional[float] = None
    no_ask_dollars: Optional[float] = None

class EvidenceMetrics(BaseModel):
    live_price: float
    distance_from_strike: float
    distance_pct: float
    buying_pressure_pct: float
    selling_pressure_pct: float
    trade_delta_60s: float
    cvd_contract: float
    volume_ratio: float
    momentum_score: float
    rsi_14: float
    volatility_annualized: float
    orderbook_imbalance: float
    large_order_bias: float
    structure_bias: str
    raw_score: float
    signal: Signal

class WeaknessReport(BaseModel):
    weakness_score: float
    flags: List[str] = Field(default_factory=list)
    absorption_detected: bool = False
    volume_divergence: bool = False
    momentum_fading: bool = False
    feed_degraded: bool = False

class RegimeReport(BaseModel):
    regime: MarketRegime
    volatility_percentile: float
    chop_index: float
    min_confidence_required: float
    explanation: str

class EarlyPredictionReport(BaseModel):
    active_window: bool
    signal: Signal
    confidence: float
    up_prob: float
    down_prob: float
    rationale: str

class FinalDecisionReport(BaseModel):
    signal: Signal
    confidence: float
    up_probability: float
    down_probability: float
    agreement_status: str
    agreement_details: List[str] = Field(default_factory=list)
    rationale: str

class EngineStatusSummary(BaseModel):
    engine_a_evidence: dict
    engine_b_weakness: dict
    engine_c_regime: dict
    engine_d_early: dict
    engine_e_final: dict

class PredictionRecord(BaseModel):
    id: Optional[int] = None
    is_official_lock: bool = True
    contract_ticker: str
    timestamp_iso: str
    epoch_ts: float
    strike_price: float
    btc_price_at_pred: float
    seconds_remaining: float
    main_signal: Signal
    early_signal: Signal
    final_signal: Signal
    confidence: float
    up_prob: float
    down_prob: float
    regime: MarketRegime
    settled: bool = False
    settlement_source: Optional[str] = None
    settlement_price: Optional[float] = None
    actual_outcome: Optional[str] = None
    final_won: Optional[bool] = None
    main_won: Optional[bool] = None
    early_won: Optional[bool] = None

class TradeRecord(BaseModel):
    id: Optional[int] = None
    mode: str
    contract_ticker: str
    timestamp_iso: str
    side: str
    contracts: int
    entry_price: float
    quote_source: str
    fee_paid: float
    fair_prob_at_entry: float
    net_edge: float
    status: str
    pnl_usd: float = 0.0
    notes: str = ""

class Scoreboard(BaseModel):
    total_predictions: int = 0
    pending_predictions: int = 0
    total_settled: int = 0
    final_scored: int = 0
    final_wins: int = 0
    final_losses: int = 0
    final_accuracy_pct: float = 0.0
    main_scored: int = 0
    main_wins: int = 0
    main_losses: int = 0
    main_accuracy_pct: float = 0.0
    early_scored: int = 0
    early_wins: int = 0
    early_losses: int = 0
    early_accuracy_pct: float = 0.0
    brier_score: Optional[float] = None
    waits_issued: int = 0
    trading_mode: str = "paper"
    bankroll_usd: float = 1000.0
    realized_pnl_usd: float = 0.0
    total_trades: int = 0
    daily_loss_locked: bool = False

class DashboardSnapshot(BaseModel):
    server_time_iso: str
    contract: ContractInfo
    feeds: List[FeedStatus]
    evidence: EvidenceMetrics
    weakness: WeaknessReport
    regime: RegimeReport
    early: EarlyPredictionReport
    final: FinalDecisionReport
    engines: EngineStatusSummary
    scoreboard: Scoreboard
    recent_predictions: List[PredictionRecord]
    recent_trades: List[TradeRecord]
