"""
Daily pricing recommendation pipeline.

Orchestrates: data fetch -> contract comparison -> recommendation table
for all configured markets. Designed to run after Platts MOC (~4:30 PM ET).

NOTE: EIA only publishes daily jet fuel spot for Gulf Coast. All markets
use GC spot as the base index — regional price differences are captured
via differentials. Once Platts/OPIS subscriptions are available (Tier 3),
per-region indices will replace this approximation.
"""

import logging
from datetime import date, datetime
from typing import Optional

from src.api.eia_client import EIAClient
from src.engines.arbitrage import ArbitrageEngine
from src.models.prices import PricingRecommendation

logger = logging.getLogger(__name__)


# Key markets with per-market index source configuration.
# Each market specifies which publisher its contracts price off of.
# "index_source": "platts" | "argus" | "eia" — determines which API to query.
# Differentials and transport costs are PROVISIONAL — update with actual terms.
DEFAULT_MARKETS = {
    "South Florida": {
        "fbo_codes": ["MIA", "FLL", "PBI", "OPF", "BCT"],
        "index_source": "platts",   # Contract prices off Platts GC
        "contract_a_diff": 0.0,
        "contract_b_diff": 0.0,
        "contract_a_transport": 0.015,
        "contract_b_transport": 0.015,
    },
    "New York / New Jersey": {
        "fbo_codes": ["TEB", "HPN", "EWR", "LGA", "JFK"],
        "index_source": "platts",   # Platts NY Harbor
        "contract_a_diff": 0.08,
        "contract_b_diff": 0.08,
        "contract_a_transport": 0.012,
        "contract_b_transport": 0.012,
    },
    "Los Angeles": {
        "fbo_codes": ["VNY", "LAX", "SNA", "BUR"],
        "index_source": "platts",   # Platts LA
        "contract_a_diff": 0.10,
        "contract_b_diff": 0.10,
        "contract_a_transport": 0.010,
        "contract_b_transport": 0.010,
    },
    "Dallas / Fort Worth": {
        "fbo_codes": ["DAL", "DFW", "ADS"],
        "index_source": "argus",    # Some DFW contracts price off Argus
        "contract_a_diff": 0.0,
        "contract_b_diff": 0.0,
        "contract_a_transport": 0.018,
        "contract_b_transport": 0.018,
    },
    "Houston": {
        "fbo_codes": ["IAH", "HOU"],
        "index_source": "platts",
        "contract_a_diff": 0.0,
        "contract_b_diff": 0.0,
        "contract_a_transport": 0.008,
        "contract_b_transport": 0.008,
    },
    "Denver": {
        "fbo_codes": ["APA", "DEN"],
        "index_source": "argus",    # Rockies — Argus Group 3
        "contract_a_diff": 0.06,
        "contract_b_diff": 0.06,
        "contract_a_transport": 0.025,
        "contract_b_transport": 0.025,
    },
    "Chicago": {
        "fbo_codes": ["MDW", "PWK", "ORD"],
        "index_source": "argus",    # Chicago — Argus
        "contract_a_diff": 0.03,
        "contract_b_diff": 0.03,
        "contract_a_transport": 0.020,
        "contract_b_transport": 0.020,
    },
    "Atlanta": {
        "fbo_codes": ["PDK", "ATL", "FTY"],
        "index_source": "platts",
        "contract_a_diff": 0.0,
        "contract_b_diff": 0.0,
        "contract_a_transport": 0.012,
        "contract_b_transport": 0.012,
    },
    "Washington D.C.": {
        "fbo_codes": ["IAD", "DCA"],
        "index_source": "platts",
        "contract_a_diff": 0.07,
        "contract_b_diff": 0.07,
        "contract_a_transport": 0.015,
        "contract_b_transport": 0.015,
    },
    "Seattle": {
        "fbo_codes": ["BFI", "SEA"],
        "index_source": "platts",
        "contract_a_diff": 0.12,
        "contract_b_diff": 0.12,
        "contract_a_transport": 0.022,
        "contract_b_transport": 0.022,
    },
    "San Francisco": {
        "fbo_codes": ["SFO", "OAK", "SJC"],
        "index_source": "platts",
        "contract_a_diff": 0.10,
        "contract_b_diff": 0.10,
        "contract_a_transport": 0.014,
        "contract_b_transport": 0.014,
    },
    "Minneapolis": {
        "fbo_codes": ["MSP", "STP"],
        "index_source": "argus",    # Upper Midwest — Argus
        "contract_a_diff": 0.04,
        "contract_b_diff": 0.04,
        "contract_a_transport": 0.022,
        "contract_b_transport": 0.022,
    },
}


class DailyPipeline:
    """Runs daily contract optimization across all markets.

    Each market can specify its own index source (platts, argus, or eia).
    The pipeline routes each market to the right API. If the configured
    source isn't available, it falls back to EIA Gulf Coast spot.
    """

    def __init__(self, eia_client: Optional[EIAClient] = None, markets: Optional[dict] = None):
        self.eia = eia_client or EIAClient()
        self.engine = ArbitrageEngine(self.eia)
        self.markets = markets or DEFAULT_MARKETS

        # Try to load premium data sources
        self.platts = None
        self.argus = None
        self.opis = None
        try:
            from src.api.platts_client import PlattsClient
            self.platts = PlattsClient()
            if not self.platts.available:
                self.platts = None
        except Exception:
            pass
        try:
            from src.api.argus_client import ArgusClient
            self.argus = ArgusClient()
            if not self.argus.available:
                self.argus = None
        except Exception:
            pass
        try:
            from src.api.opis_client import OPISClient
            self.opis = OPISClient()
            if not self.opis.available:
                self.opis = None
        except Exception:
            pass

    def run(self, as_of: Optional[date] = None) -> list[PricingRecommendation]:
        """Execute daily pricing pipeline for all markets."""
        run_date = as_of or date.today()
        logger.info(f"=== Daily Pricing Pipeline - {run_date} ===")

        # Report available data sources
        sources = []
        if self.platts:
            sources.append("Platts")
        if self.argus:
            sources.append("Argus")
        if self.opis:
            sources.append("OPIS")
        sources.append("EIA")  # Always available
        logger.info(f"  Available sources: {', '.join(sources)}")

        # Fetch EIA GC spot (always needed as baseline and for trend analysis)
        gc_spot = self.eia.get_jet_spot_prices(days=90, use_cache=True)
        if not gc_spot:
            logger.error("No Gulf Coast spot data available")
            return []

        logger.info(f"  GC spot: {len(gc_spot)} records, latest={gc_spot[-1]['date']} @ ${gc_spot[-1]['value']}/gal")

        recommendations = []
        for market_name, config in self.markets.items():
            source = config.get("index_source", "eia")
            rec = self._run_market(market_name, config, source, gc_spot, run_date)
            if rec:
                recommendations.append(rec)

        logger.info(f"Generated {len(recommendations)} recommendations")
        return recommendations

    def _run_market(self, market_name, config, source, gc_spot, run_date):
        """Route a market to its configured index source."""
        # Try the configured premium source first
        if source == "platts" and self.platts:
            rec = self._run_market_platts(market_name, config, gc_spot, run_date)
            if rec:
                return rec
        elif source == "argus" and self.argus:
            # Argus client has the same get_market_prices interface as Platts
            prices = self.argus.get_market_prices(market_name, days=30)
            pwa_data = prices.get("pwa", [])
            pda_data = prices.get("pda", [])
            if pwa_data and pda_data:
                trend, _ = self.engine.compute_trend(
                    [{"date": p["date"], "value": p["value"]} for p in pda_data]
                )
                return self.engine.compare_contracts(
                    market_name=market_name,
                    contract_a_price=pwa_data[-1]["value"],
                    contract_b_price=pda_data[-1]["value"],
                    contract_a_transport=config.get("contract_a_transport", 0),
                    contract_b_transport=config.get("contract_b_transport", 0),
                    trend_signal=trend,
                    as_of=run_date,
                )

        # Fallback to EIA with differentials
        return self.engine.run_market(
            market_name=market_name,
            spot_prices=gc_spot,
            contract_a_transport=config.get("contract_a_transport", 0),
            contract_b_transport=config.get("contract_b_transport", 0),
            contract_a_diff=config.get("contract_a_diff", 0),
            contract_b_diff=config.get("contract_b_diff", 0),
            as_of=run_date,
        )

    def _run_market_platts(self, market_name, config, gc_spot, run_date):
        """Run market using Platts PWA/PDA data directly (no simulation needed)."""
        prices = self.platts.get_market_prices(market_name, days=30)
        pwa_data = prices.get("pwa", [])
        pda_data = prices.get("pda", [])

        if not pwa_data or not pda_data:
            logger.warning(f"No Platts data for {market_name}, falling back to EIA")
            return self.engine.run_market(
                market_name=market_name, spot_prices=gc_spot,
                contract_a_transport=config.get("contract_a_transport", 0),
                contract_b_transport=config.get("contract_b_transport", 0),
                contract_a_diff=config.get("contract_a_diff", 0),
                contract_b_diff=config.get("contract_b_diff", 0),
                as_of=run_date,
            )

        # With Platts, PWA and PDA are published directly — no need to compute
        pwa_price = pwa_data[-1]["value"]
        pda_price = pda_data[-1]["value"]
        trend, _ = self.engine.compute_trend(
            [{"date": p["date"], "value": p["value"]} for p in pda_data]
        )

        return self.engine.compare_contracts(
            market_name=market_name,
            contract_a_price=pwa_price,
            contract_b_price=pda_price,
            contract_a_transport=config.get("contract_a_transport", 0),
            contract_b_transport=config.get("contract_b_transport", 0),
            trend_signal=trend,
            as_of=run_date,
        )

    def format_table(self, recommendations: list[PricingRecommendation]) -> str:
        """Format recommendations as a text table for review."""
        if not recommendations:
            return "No recommendations generated."

        source = "Platts" if self.platts else "EIA Gulf Coast Spot (regional differentials applied)"
        lines = [
            f"{'='*92}",
            f"  DAILY PRICING RECOMMENDATIONS - {recommendations[0].date}",
            f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}",
            f"  Data Source: {source}",
            f"{'='*92}",
            "",
            f"  {'Market':<25} {'A (PWA)':<12} {'B (PDA)':<12} {'Pick':<6} "
            f"{'Save $/gal':<12} {'Trend':<10} {'Conf':<8}",
            f"  {'-'*87}",
        ]

        total_savings = 0.0
        for r in sorted(recommendations, key=lambda x: x.savings_per_gal, reverse=True):
            lines.append(
                f"  {r.market_name:<25} ${r.contract_a_price:<11.4f} "
                f"${r.contract_b_price:<11.4f} {r.recommended_contract:<6} "
                f"${r.savings_per_gal:<11.4f} {r.trend_signal:<10} {r.confidence:<8}"
            )
            total_savings += r.savings_per_gal

        lines.extend([
            f"  {'-'*87}",
            f"  Average savings: ${total_savings / len(recommendations):.4f}/gal "
            f"across {len(recommendations)} markets",
            "",
        ])

        # Notes for high-savings markets
        high_impact = [r for r in recommendations if r.confidence == "high"]
        if high_impact:
            lines.append("  KEY OPPORTUNITIES:")
            for r in high_impact:
                lines.append(f"    * {r.market_name}: {r.notes}")
            lines.append("")

        lines.append(f"{'='*92}")
        return "\n".join(lines)
