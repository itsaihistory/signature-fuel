"""
Signature Energy — Fuel Pricing Intelligence System
Main entry point and CLI.

Usage:
    python main.py daily          Run daily contract optimization
    python main.py weekly         Generate weekly intelligence brief
    python main.py backtest       Backtest arbitrage on Platts sample data
    python main.py load-data      Load & export reference data
    python main.py status         Show system status and data freshness
"""

import sys
import logging
from datetime import date, datetime

# Add project root to path
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))

from config.settings import OUTPUT_DIR
from src.api.eia_client import EIAClient
from src.engines.daily_pipeline import DailyPipeline
from src.engines.arbitrage import ArbitrageEngine
from src.reports.weekly_brief import WeeklyBriefGenerator
from src.data.loader import ReferenceDataLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("signature-fuel")


def cmd_daily():
    """Run daily contract optimization pipeline."""
    print("\n=== Signature Energy — Daily Pricing Pipeline ===\n")
    pipeline = DailyPipeline()
    recs = pipeline.run()
    output = pipeline.format_table(recs)
    print(output)

    # Save to file
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"daily_recs_{date.today().strftime('%Y%m%d')}.txt"
    path.write_text(output)
    print(f"\nSaved to {path}")


def cmd_weekly():
    """Generate weekly market intelligence brief."""
    print("\n=== Signature Energy — Weekly Intelligence Brief ===\n")
    gen = WeeklyBriefGenerator()
    report = gen.generate()
    print(report)


def cmd_backtest():
    """Backtest the arbitrage engine on historical Platts sample data."""
    print("\n=== Arbitrage Backtest — Platts GC Sample Data ===\n")

    loader = ReferenceDataLoader()
    platts_data = loader.load_platts_sample()

    if not platts_data:
        print("No Platts sample data found. Ensure GC_Platts_Weekday_Sample*.xlsx is available.")
        return

    engine = ArbitrageEngine()

    # Simulate daily decisions
    a_wins = 0
    b_wins = 0
    total_savings = 0.0
    results = []

    for i, record in enumerate(platts_data):
        pwa = record.get("prior_week_avg")
        pda = record.get("prior_day")
        if pwa is None or pda is None:
            continue

        # Determine trend from last 5 prior-day prices
        recent_prices = [
            {"date": platts_data[j]["date"], "value": platts_data[j]["prior_day"]}
            for j in range(max(0, i - 5), i + 1)
            if platts_data[j].get("prior_day") is not None
        ]
        trend, _ = engine.compute_trend(recent_prices)

        from datetime import date as _date
        try:
            rec_date = _date.fromisoformat(record["date"])
        except (ValueError, TypeError):
            rec_date = None

        rec = engine.compare_contracts(
            market_name="GC Backtest",
            contract_a_price=pwa,
            contract_b_price=pda,
            trend_signal=trend,
            as_of=rec_date,
        )

        if rec.recommended_contract == "A":
            a_wins += 1
        else:
            b_wins += 1
        total_savings += rec.savings_per_gal
        results.append(rec)

    total = a_wins + b_wins
    if total == 0:
        print("No valid records for backtest.")
        return

    print(f"  Total trading days:    {total}")
    print(f"  Contract A (PWA) wins: {a_wins} ({a_wins/total*100:.1f}%)")
    print(f"  Contract B (PDA) wins: {b_wins} ({b_wins/total*100:.1f}%)")
    print(f"  Avg savings/gal:       ${total_savings/total:.4f}")
    print(f"  Total savings/gal:     ${total_savings:.4f}")
    print()

    # Show sample of highest-savings days
    top = sorted(results, key=lambda r: r.savings_per_gal, reverse=True)[:10]
    print("  Top 10 savings days:")
    print(f"  {'Date':<12} {'A (PWA)':<10} {'B (PDA)':<10} {'Pick':<6} {'Save':<10} {'Trend':<8}")
    print(f"  {'-'*56}")
    for r in top:
        print(f"  {r.date!s:<12} ${r.contract_a_price:<9.4f} ${r.contract_b_price:<9.4f} "
              f"{r.recommended_contract:<6} ${r.savings_per_gal:<9.4f} {r.trend_signal:<8}")


def cmd_load_data():
    """Load reference data and export as JSON."""
    print("\n=== Loading Reference Data ===\n")
    loader = ReferenceDataLoader()

    fbos = loader.load_fbos()
    if fbos:
        loader.export_fbos_json(fbos)
        print(f"  Loaded {len(fbos)} FBOs")
        # Summary by PADD
        padd_counts = {}
        for f in fbos:
            p = f.padd or 0
            padd_counts[p] = padd_counts.get(p, 0) + 1
        for padd in sorted(padd_counts):
            name = {0: "Unknown", 1: "East Coast", 2: "Midwest", 3: "Gulf Coast",
                    4: "Rocky Mountain", 5: "West Coast"}.get(padd, f"PADD {padd}")
            print(f"    PADD {padd} ({name}): {padd_counts[padd]} FBOs")

    terminals = loader.load_terminals()
    print(f"  Loaded {len(terminals)} terminals")

    rates = loader.load_transport_rates()
    print(f"  Loaded {len(rates)} transport rates")

    platts = loader.load_platts_sample()
    print(f"  Loaded {len(platts)} Platts sample records")


def cmd_status():
    """Show system status."""
    from config.settings import EIA_API_KEY, CACHE_DIR

    print("\n=== System Status ===\n")
    print(f"  EIA API Key:  {'Configured' if EIA_API_KEY else 'NOT SET — set EIA_API_KEY env var'}")
    print(f"  Cache Dir:    {CACHE_DIR}")
    print(f"  Output Dir:   {OUTPUT_DIR}")

    # Check cache freshness
    if CACHE_DIR.exists():
        cached = list(CACHE_DIR.glob("eia_*.json"))
        print(f"  Cached files: {len(cached)}")
        for f in sorted(cached):
            age_hrs = (datetime.now().timestamp() - f.stat().st_mtime) / 3600
            print(f"    {f.name}: {age_hrs:.1f} hrs old")
    else:
        print("  Cache: empty")

    # Check reference data
    loader = ReferenceDataLoader()
    fbos = loader.load_fbos()
    print(f"  FBOs loaded:  {len(fbos)}")


COMMANDS = {
    "daily": cmd_daily,
    "weekly": cmd_weekly,
    "backtest": cmd_backtest,
    "load-data": cmd_load_data,
    "status": cmd_status,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        print("Available commands:", ", ".join(COMMANDS.keys()))
        sys.exit(1)

    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
