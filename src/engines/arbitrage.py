"""
Contract comparison and arbitrage engine.

Core logic: Compare dual supply contracts per market each day and recommend
the cheaper option. When spot prices are rising, a weekly average lags (cheaper).
When falling, a prior-day price catches the drop faster (cheaper).

All contract parameters are configurable — never hardcoded.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from src.models.prices import PricingRecommendation

logger = logging.getLogger(__name__)


class ArbitrageEngine:
    """Computes daily contract recommendations across markets."""

    def __init__(self, eia_client=None):
        self.eia = eia_client

    def compute_trend(
        self, prices: list[dict], lookback_days: int = 5
    ) -> tuple[str, float]:
        """
        Determine price trend from recent spot data.
        Returns (signal, magnitude) where signal is "rising"/"falling"/"flat"
        and magnitude is the average daily change in $/gal.
        """
        if len(prices) < 2:
            return "flat", 0.0

        recent = prices[-lookback_days:] if len(prices) >= lookback_days else prices
        changes = []
        for i in range(1, len(recent)):
            changes.append(recent[i]["value"] - recent[i - 1]["value"])

        avg_change = sum(changes) / len(changes)

        # Threshold: ~0.5 cent/gal daily move is meaningful
        if avg_change > 0.005:
            return "rising", avg_change
        elif avg_change < -0.005:
            return "falling", avg_change
        return "flat", avg_change

    def compute_weekly_average(
        self, prices: list[dict], as_of: Optional[date] = None
    ) -> Optional[float]:
        """
        Compute prior-week average price (Mon-Fri of the week before as_of).
        This simulates the Platts Prior Week Average index.
        """
        if not prices:
            return None
        ref_date = as_of or date.today()
        # Prior week = Monday through Friday of last week
        days_since_monday = ref_date.weekday()
        last_monday = ref_date - timedelta(days=days_since_monday + 7)
        last_friday = last_monday + timedelta(days=4)

        week_prices = [
            p["value"] for p in prices
            if last_monday.isoformat() <= p["date"] <= last_friday.isoformat()
        ]

        if not week_prices:
            # Fall back to last 5 available prices
            recent = [p["value"] for p in prices[-5:]]
            return sum(recent) / len(recent) if recent else None

        return sum(week_prices) / len(week_prices)

    def get_prior_day_price(
        self, prices: list[dict], as_of: Optional[date] = None
    ) -> Optional[float]:
        """Get the most recent prior-day price before as_of."""
        if not prices:
            return None
        ref_date = as_of or date.today()
        # Find the last price on or before ref_date - 1
        target = (ref_date - timedelta(days=1)).isoformat()
        candidates = [p for p in prices if p["date"] <= target]
        return candidates[-1]["value"] if candidates else prices[-1]["value"]

    def compare_contracts(
        self,
        market_name: str,
        contract_a_price: float,
        contract_b_price: float,
        contract_a_transport: float = 0.0,
        contract_b_transport: float = 0.0,
        contract_a_diff: float = 0.0,
        contract_b_diff: float = 0.0,
        trend_signal: str = "flat",
        as_of: Optional[date] = None,
    ) -> PricingRecommendation:
        """
        Compare two contract prices and produce a recommendation.
        Prices should be the raw index values; differentials and transport
        costs are added here.
        """
        total_a = contract_a_price + contract_a_diff + contract_a_transport
        total_b = contract_b_price + contract_b_diff + contract_b_transport

        if total_a <= total_b:
            recommended = "A"
            savings = total_b - total_a
        else:
            recommended = "B"
            savings = total_a - total_b

        # Confidence based on magnitude of difference
        if savings > 0.03:
            confidence = "high"
        elif savings > 0.01:
            confidence = "normal"
        else:
            confidence = "low"

        notes = self._generate_notes(
            recommended, total_a, total_b, savings, trend_signal, confidence
        )

        return PricingRecommendation(
            market_name=market_name,
            date=as_of or date.today(),
            contract_a_price=round(total_a, 4),
            contract_b_price=round(total_b, 4),
            recommended_contract=recommended,
            savings_per_gal=round(savings, 4),
            trend_signal=trend_signal,
            confidence=confidence,
            notes=notes,
            generated_at=datetime.now(),
        )

    def run_market(
        self,
        market_name: str,
        spot_prices: list[dict],
        contract_a_transport: float = 0.0,
        contract_b_transport: float = 0.0,
        contract_a_diff: float = 0.0,
        contract_b_diff: float = 0.0,
        as_of: Optional[date] = None,
    ) -> Optional[PricingRecommendation]:
        """
        Run full arbitrage analysis for a single market using spot price data.
        Contract A = prior week average, Contract B = prior day.
        """
        pwa = self.compute_weekly_average(spot_prices, as_of)
        pda = self.get_prior_day_price(spot_prices, as_of)

        if pwa is None or pda is None:
            logger.warning(f"Insufficient price data for {market_name}")
            return None

        trend, _ = self.compute_trend(spot_prices)

        return self.compare_contracts(
            market_name=market_name,
            contract_a_price=pwa,
            contract_b_price=pda,
            contract_a_transport=contract_a_transport,
            contract_b_transport=contract_b_transport,
            contract_a_diff=contract_a_diff,
            contract_b_diff=contract_b_diff,
            trend_signal=trend,
            as_of=as_of,
        )

    def _generate_notes(
        self,
        recommended: str,
        total_a: float,
        total_b: float,
        savings: float,
        trend: str,
        confidence: str,
    ) -> str:
        parts = []

        if trend == "rising":
            parts.append("Prices rising — weekly avg (Contract A) likely lags spot, favoring A.")
        elif trend == "falling":
            parts.append("Prices falling — prior day (Contract B) captures drop faster, favoring B.")
        else:
            parts.append("Prices flat — minimal spread between contracts.")

        if confidence == "low":
            parts.append(f"Spread is narrow (${savings:.4f}/gal) — monitor closely.")
        elif confidence == "high":
            parts.append(f"Clear advantage: ${savings:.4f}/gal savings.")

        return " ".join(parts)
