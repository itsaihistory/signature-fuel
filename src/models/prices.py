"""
Data models for price data, index values, and pricing decisions.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class IndexPrice:
    """A single index price observation."""
    index_key: str          # Key into PRICING_INDICES
    date: date
    price: float            # $/gallon
    source: str = "eia"     # "eia", "platts", "opis", "argus"


@dataclass
class SpotPrice:
    """EIA spot price observation."""
    series_id: str
    date: date
    price: float            # $/gallon
    region: str = ""


@dataclass
class InventoryData:
    """Weekly jet fuel inventory by PADD."""
    padd: int
    date: date
    inventory_kb: float     # Thousand barrels
    week_over_week_change: Optional[float] = None
    year_over_year_change: Optional[float] = None


@dataclass
class RefineryUtil:
    """Weekly refinery utilization by PADD."""
    padd: int
    date: date
    utilization_pct: float  # Percent


@dataclass
class PricingRecommendation:
    """Output of the daily contract comparison for one market."""
    market_name: str
    date: date
    contract_a_price: float
    contract_b_price: float
    recommended_contract: str   # "A" or "B"
    savings_per_gal: float
    trend_signal: str           # "rising", "falling", "flat"
    confidence: str = "normal"  # "high", "normal", "low"
    notes: str = ""
    generated_at: Optional[datetime] = None

    @property
    def savings_pct(self) -> float:
        higher = max(self.contract_a_price, self.contract_b_price)
        if higher == 0:
            return 0.0
        return self.savings_per_gal / higher * 100


@dataclass
class WeeklyBrief:
    """Weekly market intelligence summary."""
    week_ending: date
    generated_at: datetime
    spot_prices: dict = field(default_factory=dict)
    inventory_summary: dict = field(default_factory=dict)
    utilization_summary: dict = field(default_factory=dict)
    crude_benchmarks: dict = field(default_factory=dict)
    trend_signals: list = field(default_factory=list)
    disruption_alerts: list = field(default_factory=list)
    recommendations: list = field(default_factory=list)
