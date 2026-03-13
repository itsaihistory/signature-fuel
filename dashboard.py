"""
Signature Energy — Fuel Pricing Intelligence Dashboard

Run with:  streamlit run dashboard.py
"""

import sys
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd
import pydeck as pdk
import yfinance as yf

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import CACHE_DIR, OUTPUT_DIR
from src.api.eia_client import EIAClient
from src.api.news_client import NewsClient
from src.engines.arbitrage import ArbitrageEngine
from src.engines.daily_pipeline import DailyPipeline, DEFAULT_MARKETS


# ── Helpers ─────────────────────────────────────────────────────────────────

def fmt_volume(val_thousands: float, per_day: bool = True) -> str:
    """Format EIA volume data (reported in thousands of barrels) into
    readable units.  e.g. 1723 thousand bbl → '1.72 million bbl/day'."""
    suffix = "/day" if per_day else ""
    abs_val = abs(val_thousands)
    sign = "-" if val_thousands < 0 else "+" if per_day and val_thousands > 0 else ""
    if abs_val >= 1000:
        return f"{sign}{val_thousands / 1000:,.2f} million bbl{suffix}"
    return f"{sign}{val_thousands:,.0f} thousand bbl{suffix}"


def fmt_volume_short(val_thousands: float, per_day: bool = True) -> str:
    """Compact version for metric cards — e.g. '1.72M bbl/d'."""
    suffix = "/d" if per_day else ""
    abs_val = abs(val_thousands)
    sign = "-" if val_thousands < 0 else ""
    if abs_val >= 1000:
        return f"{sign}{abs_val / 1000:,.2f}M bbl{suffix}"
    return f"{sign}{abs_val:,.0f}K bbl{suffix}"


def fmt_volume_delta(val_thousands: float, per_day: bool = True) -> str:
    """Signed compact version for metric deltas."""
    suffix = "/d" if per_day else ""
    abs_val = abs(val_thousands)
    sign = "+" if val_thousands >= 0 else "-"
    if abs_val >= 1000:
        return f"{sign}{abs_val / 1000:,.2f}M bbl{suffix}"
    return f"{sign}{abs_val:,.0f}K bbl{suffix}"


# ── Page Config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Signature Energy — Fuel Pricing",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* ── Global ─────────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Tighten main content padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* ── Sidebar ────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
    }
    section[data-testid="stSidebar"] * {
        color: #e2e8f0 !important;
    }
    section[data-testid="stSidebar"] .stRadio label:hover {
        color: #38bdf8 !important;
    }

    /* ── Metric cards ───────────────────────────────────────── */
    [data-testid="stMetric"] {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    [data-testid="stMetric"]:hover {
        border-color: #cbd5e1;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: #64748b !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        color: #0f172a !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.85rem !important;
        font-weight: 500 !important;
    }

    /* ── DataFrames / tables ────────────────────────────────── */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }

    /* ── Page titles ────────────────────────────────────────── */
    h1 {
        font-weight: 700 !important;
        color: #0f172a !important;
        border-bottom: 3px solid #3b82f6;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem !important;
    }
    h2, h3 {
        font-weight: 600 !important;
        color: #1e293b !important;
    }

    /* ── Alert boxes ────────────────────────────────────────── */
    .stAlert {
        border-radius: 10px;
    }

    /* ── Dividers ───────────────────────────────────────────── */
    hr {
        border: none;
        border-top: 1px solid #e2e8f0;
        margin: 1.5rem 0;
    }

    /* ── News cards ─────────────────────────────────────────── */
    .news-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        margin-bottom: 0.75rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Cached Data Loading ────────────────────────────────────────────────────

@st.cache_data(ttl=1800)
def load_eia_data():
    """Fetch all EIA data (cached for 30 min)."""
    eia = EIAClient()
    return eia.fetch_all(use_cache=True)


@st.cache_data(ttl=1800)
def load_daily_recommendations():
    """Run the daily pipeline and return recommendations."""
    pipeline = DailyPipeline()
    return pipeline.run()


@st.cache_data(ttl=86400)
def load_fbo_data():
    """Load FBO reference data from JSON."""
    fbo_path = Path("data/reference/fbos.json")
    if fbo_path.exists():
        return json.loads(fbo_path.read_text())
    return []


@st.cache_data(ttl=3600)
def load_news():
    """Fetch energy market news headlines."""
    client = NewsClient()
    return client.get_energy_headlines(days_back=3, max_results=25)


@st.cache_data(ttl=300)  # 5-minute cache — keeps it fresh without hammering Yahoo
def load_live_prices():
    """Fetch near-real-time futures prices from Yahoo Finance (free, no key)."""
    symbols = {
        "CL=F": "WTI Crude",
        "BZ=F": "Brent Crude",
        "HO=F": "Heating Oil",    # closest jet fuel proxy
        "RB=F": "RBOB Gasoline",
        "NG=F": "Natural Gas",
    }
    results = {}
    for sym, label in symbols.items():
        try:
            ticker = yf.Ticker(sym)
            info = ticker.fast_info
            price = info.last_price
            prev = info.previous_close
            change = price - prev if prev else None
            pct = (change / prev * 100) if prev else None
            results[sym] = {
                "label": label,
                "price": price,
                "change": change,
                "pct": pct,
            }
        except Exception:
            pass
    return results


# ── Sidebar ─────────────────────────────────────────────────────────────────

st.sidebar.image("assets/Signature_Aviation_new_logo.jpg", width=180)
st.sidebar.markdown("### Signature Energy")
st.sidebar.caption("Fuel Pricing Intelligence")

page = st.sidebar.radio(
    "Navigate",
    ["Daily Recommendations", "Market Overview", "Market News",
     "Inventory & Supply", "FBO & Pipeline Map"],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")

if st.sidebar.button("Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Data freshness
jet_cache = CACHE_DIR / "eia_jet_spot_gc.json"
if jet_cache.exists():
    age_min = (datetime.now().timestamp() - jet_cache.stat().st_mtime) / 60
    if age_min < 60:
        st.sidebar.success(f"Data updated {age_min:.0f} min ago")
    else:
        st.sidebar.warning(f"Data is {age_min / 60:.1f} hrs old")
else:
    st.sidebar.info("No cached data — will fetch fresh")

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<small style='color:#94a3b8'>Data: EIA (free tier)<br>"
    "Platts / Argus / OPIS: not yet connected</small>",
    unsafe_allow_html=True,
)


# ── Page: Daily Recommendations ─────────────────────────────────────────────

def page_daily():
    st.title("Daily Contract Recommendations")

    recs = load_daily_recommendations()

    if not recs:
        st.error("No recommendations available. Check EIA API key and network.")
        return

    rec_date = recs[0].date
    st.caption(f"As of {rec_date}  |  {len(recs)} markets analyzed")

    # Summary metrics
    avg_savings = sum(r.savings_per_gal for r in recs) / len(recs)
    a_picks = sum(1 for r in recs if r.recommended_contract == "A")
    b_picks = len(recs) - a_picks
    high_conf = sum(1 for r in recs if r.confidence == "high")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Savings", f"${avg_savings:.4f}/gal")
    col2.metric("Contract A Picks", f"{a_picks} of {len(recs)}")
    col3.metric("Contract B Picks", f"{b_picks} of {len(recs)}")
    col4.metric("High Confidence", f"{high_conf} markets")

    st.markdown("---")

    # Recommendation table
    rows = []
    for r in sorted(recs, key=lambda x: x.savings_per_gal, reverse=True):
        rows.append({
            "Market": r.market_name,
            "A (Prior Week Avg)": f"${r.contract_a_price:.4f}",
            "B (Prior Day)": f"${r.contract_b_price:.4f}",
            "Pick": r.recommended_contract,
            "Savings $/gal": r.savings_per_gal,
            "Trend": r.trend_signal,
            "Confidence": r.confidence,
        })

    df = pd.DataFrame(rows)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Savings $/gal": st.column_config.NumberColumn(format="$%.4f"),
            "Pick": st.column_config.TextColumn(width="small"),
            "Confidence": st.column_config.TextColumn(width="small"),
        },
    )

    # Key opportunities
    high_impact = [r for r in recs if r.confidence == "high"]
    if high_impact:
        st.markdown("### Top Opportunities")
        for r in sorted(high_impact, key=lambda x: x.savings_per_gal, reverse=True)[:5]:
            st.info(f"**{r.market_name}**: {r.notes}")


# ── Page: Market Overview ───────────────────────────────────────────────────

def page_market():
    st.title("Market Overview")

    # ── Live Futures Ticker (Yahoo Finance, ~5 min delay) ───
    live = load_live_prices()
    if live:
        st.markdown("### Live Futures")
        st.caption("Near real-time via Yahoo Finance. Updated every 5 minutes.")

        cols = st.columns(len(live))
        for i, (sym, d) in enumerate(live.items()):
            unit = "/gal" if sym in ("HO=F", "RB=F") else "/bbl" if sym in ("CL=F", "BZ=F") else ""
            fmt = f"${d['price']:.4f}{unit}" if sym in ("HO=F", "RB=F") else f"${d['price']:.2f}{unit}"
            delta_str = None
            if d["change"] is not None:
                sign = "+" if d["change"] >= 0 else ""
                if sym in ("HO=F", "RB=F"):
                    delta_str = f"{sign}{d['change']:.4f} ({sign}{d['pct']:.2f}%)"
                else:
                    delta_str = f"{sign}{d['change']:.2f} ({sign}{d['pct']:.2f}%)"
            cols[i].metric(d["label"], fmt, delta=delta_str)

        st.markdown("---")

    # ── EIA Historical Data ─────────────────────────────────
    data = load_eia_data()
    gc = data.get("jet_spot_gc", [])

    if not gc:
        st.error("No spot price data available.")
        return

    latest = gc[-1]
    current = latest["value"]

    week_ago_val = None
    target = (date.today() - timedelta(days=7)).isoformat()
    candidates = [p for p in gc if p["date"] <= target]
    if candidates:
        week_ago_val = candidates[-1]["value"]

    st.markdown("### EIA Reported Prices")
    st.caption(f"EIA data as of {latest['date']} (typically 1-2 day lag).")

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Gulf Coast Jet Spot",
        f"${current:.4f}/gal",
        delta=f"${current - week_ago_val:.4f} w/w" if week_ago_val else None,
    )

    wti = data.get("wti", [])
    brent = data.get("brent", [])
    if wti:
        col2.metric("WTI Crude (EIA)", f"${wti[-1]['value']:.2f}/bbl")
    if brent:
        col3.metric("Brent Crude (EIA)", f"${brent[-1]['value']:.2f}/bbl")

    st.markdown("---")

    # Refining Margin (formerly "Crack Spread")
    live_wti = live.get("CL=F", {}).get("price") if live else None
    live_ho = live.get("HO=F", {}).get("price") if live else None

    if (live_wti and live_ho) or (wti and gc):
        st.markdown("### Crude-to-Jet Fuel Markup")
        st.caption(
            "How much more jet fuel costs than the crude oil it's made from. "
            "A higher markup means refiners are charging more — typically due to "
            "tight supply or strong demand. Normal range: $15–25/bbl."
        )

        if live_wti and live_ho:
            wti_gal_live = live_wti / 42
            crack_live = live_ho - wti_gal_live
            crack_bbl_live = crack_live * 42

            # Show the math step by step
            st.markdown(
                f"<div style='background:#f8fafc;border:1px solid #e2e8f0;"
                f"border-radius:10px;padding:1.25rem;margin:0.75rem 0'>"
                f"<span style='color:#64748b;font-size:0.85rem;font-weight:600;"
                f"text-transform:uppercase;letter-spacing:0.04em'>The Math</span><br>"
                f"<span style='font-size:1.3rem'>"
                f"<strong>${live_ho:.4f}</strong>/gal <span style='color:#64748b'>(jet fuel)</span>"
                f" &nbsp;−&nbsp; "
                f"<strong>${wti_gal_live:.4f}</strong>/gal <span style='color:#64748b'>(crude oil ÷ 42 gal/bbl)</span>"
                f" &nbsp;=&nbsp; "
                f"<strong style='color:#0f172a'>${crack_live:.4f}</strong>/gal markup"
                f"</span><br>"
                f"<span style='color:#64748b;font-size:0.9rem'>"
                f"${crack_live:.4f}/gal × 42 gal/bbl = <strong>${crack_bbl_live:.0f}/bbl</strong>"
                f" (industry-standard unit)</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Plain-English interpretation
            if crack_bbl_live > 35:
                level = "HIGH"
                color = "#dc2626"
                explanation = (
                    f"At **${crack_bbl_live:.0f}/bbl**, the refining markup is well above "
                    f"the normal $15–25 range. Jet fuel is expensive relative to crude right now. "
                    f"This usually means supply is tight (refinery outages, maintenance) or "
                    f"demand is unusually strong (peak travel season)."
                )
            elif crack_bbl_live > 25:
                level = "ELEVATED"
                color = "#ca8a04"
                explanation = (
                    f"At **${crack_bbl_live:.0f}/bbl**, the refining markup is slightly above normal. "
                    f"Jet fuel prices have a moderate premium over crude."
                )
            elif crack_bbl_live < 15:
                level = "LOW"
                color = "#16a34a"
                explanation = (
                    f"At **${crack_bbl_live:.0f}/bbl**, the refining markup is below normal. "
                    f"Jet fuel is relatively cheap compared to crude — oversupply or weak demand."
                )
            else:
                level = "NORMAL"
                color = "#3b82f6"
                explanation = (
                    f"At **${crack_bbl_live:.0f}/bbl**, the refining markup is within the "
                    f"normal $15–25 range. No unusual pricing pressure."
                )

            st.markdown(
                f"<div style='background:#f8fafc;border-left:4px solid {color};"
                f"padding:1rem 1.25rem;border-radius:8px;margin:0.75rem 0'>"
                f"<strong style='color:{color}'>{level}</strong> — "
                f"${crack_bbl_live:.0f}/bbl &nbsp;|&nbsp; Normal range: $15–25/bbl<br>"
                f"<span style='color:#475569'>{explanation}</span></div>",
                unsafe_allow_html=True,
            )

        elif wti and gc:
            wti_gal = wti[-1]["value"] / 42
            crack = current - wti_gal
            crack_bbl = crack * 42

            col_1, col_2, col_3 = st.columns(3)
            col_1.metric("Jet Fuel Spot", f"${current:.4f}/gal")
            col_2.metric("Crude Oil (WTI)", f"${wti_gal:.4f}/gal")
            col_3.metric("Refining Markup", f"${crack:.4f}/gal")

            if crack_bbl > 35:
                level, color = "HIGH", "#dc2626"
            elif crack_bbl > 25:
                level, color = "ELEVATED", "#ca8a04"
            elif crack_bbl < 15:
                level, color = "LOW", "#16a34a"
            else:
                level, color = "NORMAL", "#3b82f6"

            st.markdown(
                f"<div style='background:#f8fafc;border-left:4px solid {color};"
                f"padding:1rem 1.25rem;border-radius:8px;margin:0.75rem 0'>"
                f"<strong style='color:{color}'>{level}</strong> — "
                f"${crack_bbl:.0f}/bbl &nbsp;|&nbsp; Normal range: $15–25/bbl</div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Price chart
    st.markdown("### Gulf Coast Jet Fuel Spot (90 days)")
    df_gc = pd.DataFrame(gc)
    df_gc["date"] = pd.to_datetime(df_gc["date"])
    df_gc = df_gc.set_index("date")
    st.line_chart(df_gc["value"], y_label="$/gal", color="#3b82f6")

    # Crude chart
    if wti and brent:
        st.markdown("### Crude Oil Benchmarks (90 days)")
        df_wti = pd.DataFrame(wti)[["date", "value"]].rename(columns={"value": "WTI"})
        df_brent = pd.DataFrame(brent)[["date", "value"]].rename(columns={"value": "Brent"})
        df_crude = df_wti.merge(df_brent, on="date", how="outer")
        df_crude["date"] = pd.to_datetime(df_crude["date"])
        df_crude = df_crude.set_index("date").sort_index()
        st.line_chart(df_crude, y_label="$/bbl")

    # Trend analysis
    st.markdown("---")
    st.markdown("### Trend Signals")
    prices = [p["value"] for p in gc]
    if len(prices) >= 20:
        ma5 = sum(prices[-5:]) / 5
        ma20 = sum(prices[-20:]) / 20

        col_t1, col_t2, col_t3 = st.columns(3)
        col_t1.metric("5-Day MA", f"${ma5:.4f}")
        col_t2.metric("20-Day MA", f"${ma20:.4f}")

        spread_pct = ((ma5 - ma20) / ma20) * 100

        if ma5 > ma20 * 1.005:
            col_t3.metric("Signal", "BULLISH")
            st.markdown(
                f"<div style='background:#eff6ff;border-left:4px solid #3b82f6;"
                f"padding:1rem 1.25rem;border-radius:8px;margin:0.75rem 0'>"
                f"<strong>Why BULLISH?</strong><br>"
                f"The 5-day average (<strong>${ma5:.4f}</strong>) is "
                f"<strong>{spread_pct:.1f}%</strong> above the 20-day average "
                f"(<strong>${ma20:.4f}</strong>). This means recent prices are higher "
                f"than the longer-term trend — the market is moving up.<br><br>"
                f"<strong>What it means for contracts:</strong> In a rising market, "
                f"Contract A (Prior Week Average) is typically cheaper because the "
                f"5-day average includes older, lower prices. Contract B (Prior Day) "
                f"reflects yesterday's higher price.</div>",
                unsafe_allow_html=True,
            )
        elif ma5 < ma20 * 0.995:
            col_t3.metric("Signal", "BEARISH")
            st.markdown(
                f"<div style='background:#fef2f2;border-left:4px solid #dc2626;"
                f"padding:1rem 1.25rem;border-radius:8px;margin:0.75rem 0'>"
                f"<strong>Why BEARISH?</strong><br>"
                f"The 5-day average (<strong>${ma5:.4f}</strong>) is "
                f"<strong>{abs(spread_pct):.1f}%</strong> below the 20-day average "
                f"(<strong>${ma20:.4f}</strong>). This means recent prices are lower "
                f"than the longer-term trend — the market is moving down.<br><br>"
                f"<strong>What it means for contracts:</strong> In a falling market, "
                f"Contract B (Prior Day) is typically cheaper because yesterday's price "
                f"is already lower than the 5-day average, which still includes older, "
                f"higher prices.</div>",
                unsafe_allow_html=True,
            )
        else:
            col_t3.metric("Signal", "NEUTRAL")
            st.markdown(
                f"<div style='background:#f8fafc;border-left:4px solid #6b7280;"
                f"padding:1rem 1.25rem;border-radius:8px;margin:0.75rem 0'>"
                f"<strong>Why NEUTRAL?</strong><br>"
                f"The 5-day average (<strong>${ma5:.4f}</strong>) and 20-day average "
                f"(<strong>${ma20:.4f}</strong>) are within 0.5% of each other "
                f"(spread: {spread_pct:+.1f}%). No clear direction.<br><br>"
                f"<strong>What it means for contracts:</strong> The difference between "
                f"Contract A and Contract B is minimal. Either choice is reasonable.</div>",
                unsafe_allow_html=True,
            )

    # Volatility
    if len(prices) >= 21:
        changes = [prices[i] - prices[i - 1] for i in range(-20, 0)]
        mean_chg = sum(changes) / len(changes)
        variance = sum((c - mean_chg) ** 2 for c in changes) / len(changes)
        vol = variance ** 0.5
        st.metric("20-Day Volatility", f"${vol:.4f}/gal daily")
        if vol > 0.02:
            st.warning("HIGH VOLATILITY — larger arbitrage opportunities available")


# ── Page: Inventory & Supply ────────────────────────────────────────────────

def page_inventory():
    st.title("Inventory & Supply Balance")

    data = load_eia_data()

    # ── Inventories ─────────────────────────────────────────
    st.markdown("### Jet Fuel Inventories by Region")
    st.caption("Source: EIA Weekly Petroleum Status Report. Values in thousand barrels.")

    inv = data.get("jet_inventories", [])
    if inv:
        padd_names = {
            "U.S.": "Total U.S.", "PADD 1": "East Coast", "PADD 2": "Midwest",
            "PADD 3": "Gulf Coast", "PADD 4": "Rocky Mountain", "PADD 5": "West Coast",
        }

        by_area = {}
        for r in inv:
            area = r.get("area-name", "Unknown")
            by_area.setdefault(area, []).append(r)

        inv_rows = []
        for area in ["U.S.", "PADD 1", "PADD 2", "PADD 3", "PADD 4", "PADD 5"]:
            records = by_area.get(area, [])
            if not records:
                continue
            latest = records[-1]
            val = float(latest["value"])
            wow = None
            if len(records) >= 2:
                prev = records[-2]
                wow = val - float(prev["value"])
            inv_rows.append({
                "Region": padd_names.get(area, area),
                "PADD": area if area != "U.S." else "",
                "Inventory (thousand bbl)": val,
                "Week/Week Change": wow,
                "As Of": latest["date"],
            })

        df_inv = pd.DataFrame(inv_rows)
        st.dataframe(
            df_inv,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Inventory (thousand bbl)": st.column_config.NumberColumn(format="%,.0f"),
                "Week/Week Change": st.column_config.NumberColumn(
                    "W/W Change (thousand bbl)", format="%+,.0f"
                ),
                "PADD": st.column_config.TextColumn(width="small"),
            },
        )

        # Flag significant draws
        for row in inv_rows:
            if row["Week/Week Change"] is not None and row["Week/Week Change"] < -500:
                st.warning(
                    f"Significant inventory draw at **{row['Region']}** "
                    f"(week of {row['As Of']}): "
                    f"{row['Week/Week Change']:+,.0f} thousand barrels"
                )
    else:
        st.info("No inventory data available.")

    st.markdown("---")

    # ── Supply / Demand Balance ─────────────────────────────
    st.markdown("### Supply / Demand Balance")
    st.caption(
        "EIA weekly data. Production and demand in thousand barrels per day. "
        "Values over 1,000 are also shown in millions for readability."
    )

    prod = data.get("jet_production", [])
    demand = data.get("jet_demand", [])

    us_prod = [r for r in prod if r.get("area-name") == "U.S."]
    if us_prod and demand:
        p = float(us_prod[-1]["value"])
        d = float(demand[-1]["value"])
        balance = p - d

        col1, col2, col3 = st.columns(3)
        col1.metric(
            "U.S. Production",
            fmt_volume_short(p),
            help=f"{p:,.0f} thousand barrels per day",
        )
        col2.metric(
            "U.S. Product Supplied",
            fmt_volume_short(d),
            help=f"{d:,.0f} thousand barrels per day (demand proxy)",
        )
        col3.metric(
            "Domestic Balance",
            fmt_volume_delta(balance),
            help="Production minus demand. Positive = domestic surplus.",
        )

        # Net imports
        ie = data.get("jet_imports_exports", [])
        if ie:
            latest_date = ie[-1]["date"]
            week_ie = [
                r for r in ie
                if r["date"] == latest_date and r.get("area-name") == "U.S."
            ]
            imports = sum(
                float(r["value"]) for r in week_ie
                if "Import" in r.get("process-name", "")
            )
            exports = sum(
                float(r["value"]) for r in week_ie
                if "Export" in r.get("process-name", "")
            )
            net_imp = imports - exports

            col_a, col_b = st.columns(2)
            col_a.metric(
                "Net Imports",
                fmt_volume_delta(net_imp),
                help=f"Imports ({imports:,.0f}K) minus exports ({exports:,.0f}K) thousand bbl/day",
            )
            col_b.metric(
                "Total Supply",
                fmt_volume_short(p + net_imp),
                help="Production + net imports",
            )
    else:
        st.info("No production/demand data available.")

    st.markdown("---")

    # ── Refinery Utilization ────────────────────────────────
    st.markdown("### Refinery Utilization")
    st.caption("Gross inputs to atmospheric crude oil distillation units, thousand barrels per day.")

    util = data.get("refinery_util", [])
    if util:
        by_area = {}
        for r in util:
            area = r.get("area-name", "Unknown")
            by_area.setdefault(area, []).append(r)

        util_rows = []
        for area in sorted(by_area.keys()):
            records = by_area[area]
            latest = records[-1]
            val = float(latest["value"])
            util_rows.append({
                "Region": area,
                "Gross Inputs (thousand bbl/day)": val,
                "As Of": latest["date"],
            })

        df_util = pd.DataFrame(util_rows)
        st.dataframe(
            df_util,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Gross Inputs (thousand bbl/day)": st.column_config.NumberColumn(
                    format="%,.0f"
                ),
            },
        )
    else:
        st.info("No refinery utilization data available.")


# ── Pipeline Route Data ─────────────────────────────────────────────────────
# Approximate waypoints for major U.S. jet fuel pipelines.
# Coordinates trace the publicly documented corridor through major hubs.

PIPELINE_ROUTES = {
    "Colonial Pipeline": {
        "color": [220, 38, 38, 180],  # red
        "description": "Houston TX to Linden NJ — 5,500 mi, largest refined-products pipeline in the U.S.",
        "capacity": "~2.5 million bbl/day",
        "products": "Gasoline, diesel, jet fuel, heating oil",
        "path": [
            [-95.36, 29.76],   # Houston TX
            [-94.10, 30.08],   # Beaumont TX
            [-93.22, 30.23],   # Lake Charles LA
            [-91.19, 30.45],   # Baton Rouge LA
            [-89.97, 30.00],   # Hammond LA
            [-89.29, 32.30],   # Jackson MS
            [-88.90, 32.35],   # Meridian MS
            [-87.55, 33.21],   # Tuscaloosa AL
            [-86.80, 33.52],   # Birmingham / Pelham AL
            [-85.00, 33.40],   # Anniston AL
            [-84.56, 33.84],   # Austell GA (major terminal)
            [-84.39, 33.75],   # Atlanta GA
            [-83.37, 33.96],   # Athens GA
            [-82.40, 34.18],   # Anderson SC
            [-81.07, 34.00],   # Columbia SC
            [-80.84, 35.23],   # Charlotte NC
            [-79.79, 36.07],   # Greensboro NC (major terminal)
            [-79.43, 37.27],   # Roanoke VA
            [-78.47, 37.55],   # Charlottesville VA
            [-77.44, 37.54],   # Richmond VA
            [-77.04, 38.90],   # Washington D.C.
            [-76.61, 39.29],   # Baltimore MD
            [-75.16, 39.95],   # Philadelphia PA
            [-74.23, 40.64],   # Linden NJ (terminus)
        ],
    },
    "Plantation Pipeline": {
        "color": [22, 163, 74, 180],  # green
        "description": "Baton Rouge LA to Washington D.C. area — southeastern refined products.",
        "capacity": "~660,000 bbl/day",
        "products": "Gasoline, diesel, jet fuel",
        "path": [
            [-91.19, 30.45],   # Baton Rouge LA
            [-89.97, 30.00],   # Hammond LA
            [-89.29, 32.30],   # Jackson MS (area)
            [-88.55, 33.45],   # Tuscaloosa AL (area)
            [-86.80, 33.52],   # Birmingham AL
            [-85.68, 33.46],   # Anniston / Oxford AL
            [-84.56, 33.84],   # Austell GA (junction w/ Colonial)
            [-84.39, 33.75],   # Atlanta GA
            [-83.40, 34.77],   # Gainesville GA
            [-82.83, 34.68],   # Clemson SC area
            [-82.55, 35.60],   # Asheville NC
            [-81.68, 35.91],   # Morganton NC
            [-80.84, 35.23],   # Charlotte NC
            [-80.26, 36.10],   # Winston-Salem NC
            [-79.79, 36.07],   # Greensboro NC
            [-79.10, 36.50],   # Danville VA
            [-79.43, 37.27],   # Roanoke VA
            [-78.87, 37.83],   # Charlottesville VA area
            [-77.44, 37.54],   # Richmond VA
            [-77.04, 38.90],   # Washington D.C. area
        ],
    },
    "Explorer Pipeline": {
        "color": [37, 99, 235, 180],  # blue
        "description": "Houston TX to Hammond IN (Chicago area) — Gulf Coast to Midwest.",
        "capacity": "~660,000 bbl/day",
        "products": "Gasoline, diesel, jet fuel",
        "path": [
            [-95.36, 29.76],   # Houston TX
            [-94.10, 30.08],   # Beaumont TX
            [-93.22, 30.23],   # Lake Charles LA
            [-92.02, 30.22],   # Lafayette LA
            [-91.19, 30.45],   # Baton Rouge LA
            [-91.15, 31.31],   # Natchez MS
            [-90.18, 32.30],   # Jackson MS area
            [-89.53, 34.37],   # Holly Springs MS
            [-89.97, 35.15],   # Memphis TN
            [-88.75, 35.61],   # Jackson TN
            [-87.99, 36.17],   # Nashville area
            [-87.50, 36.97],   # Bowling Green KY
            [-86.76, 37.77],   # Elizabethtown KY
            [-85.76, 38.25],   # Louisville KY
            [-86.16, 39.77],   # Indianapolis IN
            [-87.33, 41.48],   # Hammond IN (Chicago area)
        ],
    },
    "CALNEV Pipeline (Kinder Morgan)": {
        "color": [168, 85, 247, 180],  # purple
        "description": "Colton CA to Las Vegas NV — 566 mi, transports gasoline, diesel, and jet fuel via parallel 14\" and 8\" lines.",
        "capacity": "~120,000 bbl/day",
        "products": "Gasoline, diesel, jet fuel",
        "path": [
            [-117.31, 34.05],  # Colton CA
            [-117.02, 34.54],  # Victorville CA area
            [-116.97, 34.84],  # Barstow CA
            [-116.17, 35.26],  # Baker CA
            [-115.51, 35.60],  # Primm NV
            [-115.17, 36.08],  # Jean NV
            [-115.14, 36.17],  # Las Vegas NV
        ],
    },
    "SFPP — San Diego Line (Kinder Morgan)": {
        "color": [245, 158, 11, 180],  # amber/orange
        "description": "LA Basin (Carson) to San Diego — 135-mi southern segment of the SFPP system, the largest products pipeline in the Western U.S.",
        "capacity": "~400,000 bbl/day (combined SFPP system)",
        "products": "Gasoline, diesel, jet fuel",
        "path": [
            [-118.26, 33.83],  # Carson / LA Harbor area
            [-117.88, 33.77],  # Long Beach area
            [-117.56, 33.63],  # Orange County
            [-117.35, 33.19],  # Oceanside / Camp Pendleton
            [-117.16, 32.72],  # San Diego (Mission Valley)
        ],
    },
    "Everglades Pipeline (Buckeye)": {
        "color": [6, 182, 212, 180],  # teal/cyan
        "description": "Port Everglades to MIA and FLL — 36-mi subterranean jet fuel (Jet A) pipeline serving Miami and Fort Lauderdale airports.",
        "capacity": "Dedicated jet fuel line",
        "products": "Jet A fuel",
        "path": [
            [-80.12, 26.09],  # Port Everglades (Broward County)
            [-80.15, 26.07],  # Fort Lauderdale-Hollywood Intl (FLL)
            [-80.29, 25.80],  # Miami International Airport (MIA)
        ],
    },
    "SFPP — East Line (Kinder Morgan)": {
        "color": [245, 158, 11, 180],  # amber/orange (same color — same system)
        "description": "LA Basin to Phoenix and Tucson AZ — ~400-mi eastern segment originating in El Paso TX corridor.",
        "capacity": "~400,000 bbl/day (combined SFPP system)",
        "products": "Gasoline, diesel, jet fuel",
        "path": [
            [-118.26, 33.83],  # Carson / LA Harbor area
            [-117.88, 33.77],  # Long Beach area
            [-117.31, 34.05],  # Colton CA (junction)
            [-115.51, 32.74],  # Imperial CA / El Centro area
            [-114.62, 32.72],  # Yuma AZ
            [-112.07, 33.45],  # Phoenix AZ
            [-111.97, 33.42],  # Mesa / Tempe AZ
            [-110.97, 32.22],  # Tucson AZ
        ],
    },
}


# ── Page: FBO Map ───────────────────────────────────────────────────────────

def page_fbo_map():
    st.title("FBO & Pipeline Map")

    fbos = load_fbo_data()
    if not fbos:
        st.warning("No FBO data available. Run `python main.py load-data` first.")
        return

    st.caption(f"{len(fbos)} FBO locations  |  3 major jet fuel pipelines")

    df = pd.DataFrame(fbos)
    df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
    df = df.dropna(subset=["lat", "lon"])

    # Filters
    col_f1, col_f2 = st.columns(2)

    with col_f1:
        regions = sorted(df["region_label"].dropna().unique())
        selected_regions = st.multiselect(
            "Filter FBOs by region", regions, default=regions
        )
    with col_f2:
        pipeline_names = list(PIPELINE_ROUTES.keys())
        selected_pipelines = st.multiselect(
            "Show pipelines", pipeline_names, default=pipeline_names
        )

    if selected_regions:
        df_filtered = df[df["region_label"].isin(selected_regions)]
    else:
        df_filtered = df

    # ── Build pydeck layers ─────────────────────────────────
    layers = []

    # Add tooltip fields to FBO data
    df_map = df_filtered.copy()
    df_map["tooltip_title"] = df_map["code"] + " — " + df_map["city"] + ", " + df_map["state"]
    df_map["tooltip_detail"] = "Region: " + df_map["region_label"].fillna("N/A")

    # FBO scatter layer
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=df_map,
            get_position=["lon", "lat"],
            get_fill_color=[59, 130, 246, 200],  # blue dots
            get_radius=6000,
            radius_min_pixels=3,
            radius_max_pixels=8,
            pickable=True,
            auto_highlight=True,
        )
    )

    # Pipeline path layers
    for name in selected_pipelines:
        route = PIPELINE_ROUTES[name]
        path_data = pd.DataFrame({
            "tooltip_title": [name],
            "tooltip_detail": [
                f"{route['description']}<br/>"
                f"Capacity: {route['capacity']}<br/>"
                f"Products: {route['products']}"
            ],
            "path": [route["path"]],
            "color": [route["color"]],
        })
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=path_data,
                get_path="path",
                get_color="color",
                width_min_pixels=3,
                width_max_pixels=6,
                pickable=True,
                auto_highlight=True,
            )
        )

    # View state — center on continental U.S.
    view = pdk.ViewState(
        latitude=36.5,
        longitude=-96.0,
        zoom=3.8,
        pitch=0,
    )

    tooltip = {
        "html": "<b>{tooltip_title}</b><br/>{tooltip_detail}",
        "style": {
            "backgroundColor": "#0f172a",
            "color": "#e2e8f0",
            "fontSize": "13px",
            "padding": "8px 12px",
            "borderRadius": "6px",
        },
    }

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=view,
            tooltip=tooltip,
            map_style="light",
        ),
        use_container_width=True,
        height=560,
    )

    # ── Pipeline legend ─────────────────────────────────────
    st.markdown("### Pipeline Details")
    for name in selected_pipelines:
        route = PIPELINE_ROUTES[name]
        r, g, b, _ = route["color"]
        st.markdown(
            f"<span style='display:inline-block;width:14px;height:14px;"
            f"background:rgb({r},{g},{b});border-radius:3px;margin-right:8px;"
            f"vertical-align:middle'></span>"
            f"**{name}** — {route['description']}  \n"
            f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Capacity: {route['capacity']} | "
            f"Products: {route['products']}",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── FBO Directory table ─────────────────────────────────
    st.markdown("### FBO Directory")
    display_cols = [
        "code", "city", "state", "region_label",
        "index_publisher", "index_name", "sample_differential",
    ]
    available_cols = [c for c in display_cols if c in df_filtered.columns]
    st.dataframe(
        df_filtered[available_cols].sort_values("code"),
        use_container_width=True,
        hide_index=True,
        column_config={
            "code": st.column_config.TextColumn("ICAO"),
            "city": st.column_config.TextColumn("City"),
            "state": st.column_config.TextColumn("State"),
            "region_label": st.column_config.TextColumn("Region"),
            "index_publisher": st.column_config.TextColumn("Index"),
            "index_name": st.column_config.TextColumn("Index Name"),
            "sample_differential": st.column_config.NumberColumn(
                "Differential $/gal", format="$%.2f"
            ),
        },
    )


# ── Page: Market News ──────────────────────────────────────────────────────

def page_news():
    st.title("Market News")

    articles = load_news()

    if not articles:
        st.warning("No news available. Check NEWS_API_KEY in config/settings.py.")
        return

    st.caption(f"{len(articles)} articles from the last 3 days")

    # Topic filter
    topics = sorted(set(a["topic"] for a in articles))
    selected_topics = st.multiselect("Filter by topic", topics, default=topics)

    filtered = [a for a in articles if a["topic"] in selected_topics]

    if not filtered:
        st.info("No articles match the selected filters.")
        return

    # Topic summary bar
    topic_counts = {}
    for a in filtered:
        topic_counts[a["topic"]] = topic_counts.get(a["topic"], 0) + 1

    cols = st.columns(min(len(topic_counts), 6))
    for i, (topic, count) in enumerate(sorted(topic_counts.items(), key=lambda x: -x[1])):
        cols[i % len(cols)].metric(topic, count)

    st.markdown("---")

    # Article list
    for article in filtered:
        with st.container():
            st.markdown(
                f"**[{article['title']}]({article['url']})**  \n"
                f"`{article['topic']}` · {article['source']} · {article['published']}"
            )
            if article["description"]:
                st.caption(article["description"][:300])


# ── Router ──────────────────────────────────────────────────────────────────

if page == "Daily Recommendations":
    page_daily()
elif page == "Market Overview":
    page_market()
elif page == "Market News":
    page_news()
elif page == "Inventory & Supply":
    page_inventory()
elif page == "FBO & Pipeline Map":
    page_fbo_map()
