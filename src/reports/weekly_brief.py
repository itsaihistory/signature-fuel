"""
Weekly Market Intelligence Brief Generator.

Compiles: spot price trends, inventory levels, refinery utilization,
crude benchmarks, production, imports/exports, demand, and forward-looking analysis.
Target delivery: Monday 7 AM ET covering prior week.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from src.api.eia_client import EIAClient
from config.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)


class WeeklyBriefGenerator:
    """Generates the weekly market intelligence report."""

    def __init__(self, eia_client: Optional[EIAClient] = None):
        self.eia = eia_client or EIAClient()

    def generate(self, week_ending: Optional[date] = None) -> str:
        """Generate full weekly brief as formatted text."""
        end_date = week_ending or date.today()
        start_date = end_date - timedelta(days=7)

        logger.info(f"Generating weekly brief: {start_date} to {end_date}")

        # Fetch all data up front
        all_data = self.eia.fetch_all(use_cache=True)

        sections = [
            self._header(start_date, end_date),
            self._spot_price_section(all_data),
            self._crude_benchmark_section(all_data),
            self._inventory_section(all_data),
            self._production_section(all_data),
            self._demand_section(all_data),
            self._imports_exports_section(all_data),
            self._trend_analysis_section(all_data),
            self._supply_demand_balance(all_data),
            self._forward_look_section(),
            self._footer(),
        ]

        report = "\n\n".join(s for s in sections if s)
        self._save(report, end_date)
        return report

    def _header(self, start: date, end: date) -> str:
        return "\n".join([
            "=" * 80,
            "  SIGNATURE ENERGY - WEEKLY MARKET INTELLIGENCE BRIEF",
            f"  Week of {start.strftime('%B %d')} - {end.strftime('%B %d, %Y')}",
            f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}",
            "=" * 80,
        ])

    def _spot_price_section(self, data: dict) -> str:
        lines = ["SECTION 1: JET FUEL SPOT PRICES (Gulf Coast)", "-" * 50]

        gc = data.get("jet_spot_gc", [])
        if not gc:
            lines.append("  No spot price data available.")
            return "\n".join(lines)

        latest = gc[-1]
        current = latest["value"]
        lines.append(f"  Current:     ${current:.4f}/gal ({latest['date']})")

        week_ago = self._find_n_days_ago(gc, 7)
        if week_ago:
            wow = current - week_ago
            pct = (wow / week_ago) * 100
            d = "+" if wow > 0 else ""
            lines.append(f"  Week/Week:   {d}${wow:.4f} ({d}{pct:.1f}%)")

        month_ago = self._find_n_days_ago(gc, 30)
        if month_ago:
            mom = current - month_ago
            pct = (mom / month_ago) * 100
            d = "+" if mom > 0 else ""
            lines.append(f"  Month/Month: {d}${mom:.4f} ({d}{pct:.1f}%)")

        # Recent daily prices
        lines.append("")
        lines.append("  Last 10 trading days:")
        for p in gc[-10:]:
            lines.append(f"    {p['date']}  ${p['value']:.4f}/gal")

        return "\n".join(lines)

    def _crude_benchmark_section(self, data: dict) -> str:
        lines = ["SECTION 2: CRUDE OIL BENCHMARKS", "-" * 50]

        for name, key in [("WTI Cushing", "wti"), ("Brent", "brent")]:
            prices = data.get(key, [])
            if not prices:
                lines.append(f"  {name}: No data")
                continue
            latest = prices[-1]
            current = latest["value"]
            lines.append(f"  {name}:")
            lines.append(f"    Current: ${current:.2f}/bbl ({latest['date']})")
            week_ago = self._find_n_days_ago(prices, 7)
            if week_ago:
                chg = current - week_ago
                d = "+" if chg > 0 else ""
                lines.append(f"    Week/Week: {d}${chg:.2f}/bbl")
            lines.append("")

        # Crack spread: the difference between refined jet fuel price and crude oil
        # input cost. This is the refining margin — what refiners earn converting
        # crude into jet fuel. A wide crack spread means jet fuel is expensive
        # relative to crude (strong refining economics, tight jet fuel supply, or
        # elevated demand). A narrow spread means jet fuel is cheap relative to
        # crude. For pricing, a widening crack spread signals upward pressure on
        # jet fuel even if crude is flat.
        gc = data.get("jet_spot_gc", [])
        wti = data.get("wti", [])
        if gc and wti:
            jet_gal = gc[-1]["value"]
            wti_bbl = wti[-1]["value"]
            wti_gal = wti_bbl / 42  # 42 gallons per barrel
            crack = jet_gal - wti_gal
            crack_bbl = crack * 42
            lines.append(f"  Jet Fuel Crack Spread (GC Jet vs WTI Crude):")
            lines.append(f"    Jet fuel: ${jet_gal:.4f}/gal | Crude: ${wti_gal:.4f}/gal (${wti_bbl:.2f}/bbl)")
            lines.append(f"    Crack spread: ${crack:.4f}/gal (${crack_bbl:.2f}/bbl)")
            if crack_bbl > 35:
                lines.append(f"    *** WIDE SPREAD — jet fuel pricing well above crude input cost.")
                lines.append(f"    This indicates tight jet supply, strong demand, or refining constraints.")
                lines.append(f"    Expect continued upward pressure on contract prices. ***")
            elif crack_bbl < 15:
                lines.append(f"    Narrow spread — jet fuel relatively cheap vs crude. Potential downside limited.")

        return "\n".join(lines)

    def _inventory_section(self, data: dict) -> str:
        lines = ["SECTION 3: JET FUEL INVENTORIES (Thousand Barrels)", "-" * 50]

        inv = data.get("jet_inventories", [])
        if not inv:
            lines.append("  No inventory data available.")
            return "\n".join(lines)

        # Group by area, get latest two weeks
        by_area = {}
        for r in inv:
            area = r.get("area-name", "Unknown")
            by_area.setdefault(area, []).append(r)

        for area in ["U.S.", "PADD 1", "PADD 2", "PADD 3", "PADD 4", "PADD 5"]:
            padd_names = {"PADD 1": "East Coast", "PADD 2": "Midwest", "PADD 3": "Gulf Coast",
                          "PADD 4": "Rocky Mountain", "PADD 5": "West Coast", "U.S.": "Total U.S."}
            records = by_area.get(area, [])
            if not records:
                continue
            latest = records[-1]
            label = padd_names.get(area, area)
            lines.append(f"  {area} ({label}):")
            lines.append(f"    Latest: {float(latest['value']):,.0f} KB ({latest['date']})")
            if len(records) >= 2:
                prev = records[-2]
                chg = float(latest["value"]) - float(prev["value"])
                d = "+" if chg > 0 else ""
                lines.append(f"    Week/Week: {d}{chg:,.0f} KB")
                if chg < -500:
                    lines.append(f"    *** SIGNIFICANT DRAW - monitor supply tightness ***")
                elif chg > 500:
                    lines.append(f"    Notable build - downward price pressure likely")
            lines.append("")

        return "\n".join(lines)

    def _production_section(self, data: dict) -> str:
        lines = ["SECTION 4: JET FUEL PRODUCTION (Thousand Barrels/Day)", "-" * 50]

        prod = data.get("jet_production", [])
        if not prod:
            lines.append("  No production data available.")
            return "\n".join(lines)

        by_area = {}
        for r in prod:
            area = r.get("area-name", "Unknown")
            by_area.setdefault(area, []).append(r)

        for area in ["U.S.", "PADD 1", "PADD 2", "PADD 3", "PADD 4", "PADD 5"]:
            records = by_area.get(area, [])
            if not records:
                continue
            latest = records[-1]
            lines.append(f"  {area}: {float(latest['value']):,.0f} MBBL/D ({latest['date']})")
            if len(records) >= 2:
                prev = records[-2]
                chg = float(latest["value"]) - float(prev["value"])
                d = "+" if chg > 0 else ""
                lines.append(f"    Week/Week: {d}{chg:,.0f} MBBL/D")

        return "\n".join(lines)

    def _demand_section(self, data: dict) -> str:
        lines = ["SECTION 5: JET FUEL DEMAND - PRODUCT SUPPLIED (Thousand Barrels/Day)", "-" * 50]

        demand = data.get("jet_demand", [])
        if not demand:
            lines.append("  No demand data available.")
            return "\n".join(lines)

        latest = demand[-1]
        lines.append(f"  U.S. Product Supplied: {float(latest['value']):,.0f} MBBL/D ({latest['date']})")

        if len(demand) >= 2:
            prev = demand[-2]
            chg = float(latest["value"]) - float(prev["value"])
            d = "+" if chg > 0 else ""
            lines.append(f"  Week/Week: {d}{chg:,.0f} MBBL/D")

        # 4-week average
        if len(demand) >= 4:
            avg4 = sum(float(r["value"]) for r in demand[-4:]) / 4
            lines.append(f"  4-Week Average: {avg4:,.0f} MBBL/D")

        return "\n".join(lines)

    def _imports_exports_section(self, data: dict) -> str:
        lines = ["SECTION 6: JET FUEL IMPORTS & EXPORTS (Thousand Barrels/Day)", "-" * 50]

        ie = data.get("jet_imports_exports", [])
        if not ie:
            lines.append("  No import/export data available.")
            return "\n".join(lines)

        # Get latest week's data
        latest_date = ie[-1]["date"]
        latest_week = [r for r in ie if r["date"] == latest_date]

        for r in latest_week:
            area = r.get("area-name", "")
            process = r.get("process-name", "")
            val = float(r["value"])
            lines.append(f"  {area:20s}  {process:15s}  {val:>6,.0f} MBBL/D")

        return "\n".join(lines)

    def _trend_analysis_section(self, data: dict) -> str:
        lines = ["SECTION 7: TREND ANALYSIS & ARBITRAGE SIGNALS", "-" * 50]

        gc = data.get("jet_spot_gc", [])
        if not gc or len(gc) < 10:
            lines.append("  Insufficient data for trend analysis.")
            return "\n".join(lines)

        prices = [p["value"] for p in gc]

        # Moving averages
        if len(prices) >= 20:
            ma5 = sum(prices[-5:]) / 5
            ma20 = sum(prices[-20:]) / 20

            lines.append(f"  Gulf Coast Jet Fuel:")
            lines.append(f"    5-day MA:  ${ma5:.4f}/gal")
            lines.append(f"    20-day MA: ${ma20:.4f}/gal")

            if ma5 > ma20 * 1.005:
                lines.append("    Signal: BULLISH - 5-day above 20-day, prices trending up")
                lines.append("    Arbitrage: Prior Week Avg contracts likely cheaper")
            elif ma5 < ma20 * 0.995:
                lines.append("    Signal: BEARISH - 5-day below 20-day, prices trending down")
                lines.append("    Arbitrage: Prior Day contracts likely cheaper")
            else:
                lines.append("    Signal: NEUTRAL - moving averages converging")
                lines.append("    Arbitrage: Minimal spread between contracts")
            lines.append("")

        # Volatility
        if len(prices) >= 21:
            changes = [prices[i] - prices[i-1] for i in range(-20, 0)]
            mean_chg = sum(changes) / len(changes)
            variance = sum((c - mean_chg)**2 for c in changes) / len(changes)
            vol = variance ** 0.5
            lines.append(f"    20-day volatility: ${vol:.4f}/gal daily")
            if vol > 0.02:
                lines.append("    *** HIGH VOLATILITY - larger arbitrage opportunities ***")

        return "\n".join(lines)

    def _supply_demand_balance(self, data: dict) -> str:
        lines = ["SECTION 8: SUPPLY/DEMAND BALANCE SNAPSHOT", "-" * 50]

        prod = data.get("jet_production", [])
        demand = data.get("jet_demand", [])
        ie = data.get("jet_imports_exports", [])

        # Latest U.S. production
        us_prod = [r for r in prod if r.get("area-name") == "U.S."]
        us_demand = demand  # already U.S. only

        if us_prod and us_demand:
            p = float(us_prod[-1]["value"])
            d = float(us_demand[-1]["value"])
            balance = p - d
            lines.append(f"  U.S. Production:      {p:>6,.0f} MBBL/D")
            lines.append(f"  U.S. Product Supplied: {d:>6,.0f} MBBL/D")
            lines.append(f"  Domestic Balance:      {balance:>+6,.0f} MBBL/D")

            # Net imports
            latest_ie_date = ie[-1]["date"] if ie else None
            if latest_ie_date:
                week_ie = [r for r in ie if r["date"] == latest_ie_date and r.get("area-name") == "U.S."]
                imports = sum(float(r["value"]) for r in week_ie if "Import" in r.get("process-name", ""))
                exports = sum(float(r["value"]) for r in week_ie if "Export" in r.get("process-name", ""))
                net_imp = imports - exports
                lines.append(f"  Net Imports:           {net_imp:>+6,.0f} MBBL/D")
                lines.append(f"  Total Supply:          {p + net_imp:>6,.0f} MBBL/D")
        else:
            lines.append("  Insufficient data for balance calculation.")

        return "\n".join(lines)

    def _forward_look_section(self) -> str:
        return "\n".join([
            "SECTION 9: FORWARD-LOOKING SIGNALS",
            "-" * 50,
            "  Factors to monitor this week:",
            "  - EIA Weekly Petroleum Status Report (Wednesday 10:30 AM ET)",
            "  - Gulf Coast refinery maintenance schedules",
            "  - Weather disruptions (hurricane season Jun-Nov, winter storms Dec-Feb)",
            "  - Geopolitical developments affecting crude benchmarks",
            "  - Seasonal demand patterns (holiday travel, major sporting events)",
            "",
            "  Tier 2 integrations planned: NOAA weather, FlightAware demand data",
        ])

    def _footer(self) -> str:
        return "\n".join([
            "=" * 80,
            "  CONFIDENTIAL - Signature Energy Internal Use Only",
            "  Prepared by: Fuel Pricing Intelligence System",
            "  Contact: Parker Gordon, Manager - Pricing",
            "=" * 80,
        ])

    def _find_n_days_ago(self, data: list[dict], n: int) -> Optional[float]:
        target = (date.today() - timedelta(days=n)).isoformat()
        candidates = [p for p in data if p["date"] <= target]
        return candidates[-1]["value"] if candidates else None

    def _save(self, report: str, week_ending: date):
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        path = OUTPUT_DIR / f"weekly_brief_{week_ending.strftime('%Y%m%d')}.txt"
        path.write_text(report)
        logger.info(f"Saved weekly brief to {path}")
