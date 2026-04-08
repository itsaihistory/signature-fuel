"""
Jet Fuel Market Update Generator — 3x daily email reports.

Pulls from EIA (spot prices, inventories, production, demand, crack spreads)
and NewsAPI (breaking headlines) to produce a concise market intelligence
email with immediate + 30/60/90-day outlook.

Designed to run at:
  - 9:30 AM ET  (morning briefing)
  - 12:30 PM ET (midday update)
  - 4:30 PM ET  (afternoon close)
"""

import json
import logging
import requests
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import yfinance as yf

from src.api.eia_client import EIAClient
from src.api.news_client import NewsClient
from src.api.a4a_client import A4AClient

logger = logging.getLogger(__name__)

# FBO states for weather alerts (same as dashboard)
FBO_STATES = ["FL", "NY", "NJ", "CA", "TX", "CO", "IL", "GA", "VA", "DC", "WA", "MN"]

# Strong keywords — any one of these makes an article relevant
_STRONG_KEYWORDS = [
    "fuel", "oil", "crude", "refin", "pipeline", "opec",
    "gasoline", "diesel", "jet fuel", "kerosene", "barrel", "bbl",
    "sanction", "embargo", "tariff", "iran", "russia", "saudi",
    "crack spread", "inventory drawdown", "fuel shortage",
    "lng", "natural gas", "petrochemical", "tanker", "freight",
    "blending mandate", "carbon credit", "emissions regulation",
]

# Weak keywords — require at least one energy-context word alongside them
_WEAK_KEYWORDS = [
    "airline", "aviation", "flight", "airport", "energy", "supply",
    "demand", "shortage", "storm", "hurricane", "flood", "wildfire",
    "carbon", "emissions", "epa",
]
_ENERGY_CONTEXT = [
    "fuel", "oil", "crude", "refin", "pipeline", "price", "barrel",
    "bbl", "gallon", "gal", "opec", "energy market", "supply chain",
    "outage", "shut", "production", "export", "import", "inventory",
]

# Sources that never publish relevant energy content
_SOURCE_BLOCKLIST = [
    "kotaku", "mental floss", "jalopnik", "ign", "gamespot", "polygon",
    "the verge", "pcgamer", "rock paper shotgun", "eurogamer", "gizmodo",
    "lifehacker", "buzzfeed", "tmz", "people", "us weekly", "e! online",
]


def _load_fbo_data() -> list[dict]:
    """Load FBO reference data from JSON."""
    fbo_path = Path("data/reference/fbos.json")
    if fbo_path.exists():
        return json.loads(fbo_path.read_text())
    return []


def _is_relevant_article(title: str, description: str, source: str = "") -> bool:
    """Check if a news article is relevant to energy/fuel markets.

    Uses two-tier keyword matching:
    - Strong keywords (fuel, oil, crude, etc.) → always relevant
    - Weak keywords (airline, flight, storm, etc.) → only relevant if
      an energy-context word also appears
    Also blocks known irrelevant sources.
    """
    text = (title + " " + description).lower()
    source_lower = source.lower()

    # Block irrelevant sources
    if any(src in source_lower for src in _SOURCE_BLOCKLIST):
        return False

    # Block ABC/network TV short video clips (rarely relevant energy content)
    if title.strip().startswith("WATCH:"):
        return False

    # Strong keyword → immediately relevant
    if any(kw in text for kw in _STRONG_KEYWORDS):
        return True

    # Weak keyword → only if energy context present
    has_weak = any(kw in text for kw in _WEAK_KEYWORDS)
    has_context = any(kw in text for kw in _ENERGY_CONTEXT)
    return has_weak and has_context


def _fetch_live_crude():
    """Fetch live WTI and Brent from Yahoo Finance."""
    results = {}
    for sym, label in [("CL=F", "WTI Crude"), ("BZ=F", "Brent Crude")]:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.fast_info
            price = info.last_price
            prev = info.previous_close
            change = price - prev if prev else None
            pct = (change / prev * 100) if prev else None
            results[sym] = {"label": label, "price": price, "change": change, "pct": pct}
        except Exception:
            try:
                hist = yf.Ticker(sym).history(period="2d")
                if len(hist) >= 1:
                    price = float(hist["Close"].iloc[-1])
                    prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else None
                    change = (price - prev) if prev else None
                    pct = (change / prev * 100) if prev else None
                    results[sym] = {"label": label, "price": price, "change": change, "pct": pct}
            except Exception as e:
                logger.warning(f"Failed to fetch {sym}: {e}")
    return results


def _flatten_coords(coords):
    """Flatten nested GeoJSON coordinate arrays to list of [lng, lat] pairs."""
    if not coords:
        return []
    if isinstance(coords[0], (int, float)):
        return [coords]
    flat = []
    for item in coords:
        flat.extend(_flatten_coords(item))
    return flat


def _fetch_weather_alerts():
    """Fetch NWS weather alerts matched to Signature FBO locations.

    Uses alert GeoJSON geometry (bounding box + buffer) to determine which
    FBOs are actually in the affected area, rather than matching by state.
    Falls back to areaDesc county/city name matching when geometry is absent.
    """
    headers = {"User-Agent": "(SignatureEnergy Dashboard, contact@example.com)"}
    fbos = _load_fbo_data()
    if not fbos:
        return []

    fbos_by_state = {}
    for fbo in fbos:
        fbos_by_state.setdefault(fbo["state"], []).append(fbo)

    states = sorted(fbos_by_state.keys())
    matched_alerts = []

    for state in states:
        try:
            resp = requests.get(
                f"https://api.weather.gov/alerts/active/area/{state}",
                headers=headers, timeout=10,
            )
            if resp.status_code != 200:
                continue

            for feature in resp.json().get("features", []):
                props = feature.get("properties", {})
                sev = props.get("severity", "Unknown")
                if sev not in ("Severe", "Extreme"):
                    continue

                state_fbos = fbos_by_state.get(state, [])
                geometry = feature.get("geometry")
                affected = []

                if geometry and geometry.get("coordinates"):
                    # Use bounding box of alert polygon + buffer to match FBOs
                    coords = _flatten_coords(geometry["coordinates"])
                    if coords:
                        min_lat = min(c[1] for c in coords)
                        max_lat = max(c[1] for c in coords)
                        min_lng = min(c[0] for c in coords)
                        max_lng = max(c[0] for c in coords)
                        # ~15 mile buffer (0.2 degrees)
                        buf = 0.2
                        for fbo in state_fbos:
                            if (min_lat - buf <= fbo["latitude"] <= max_lat + buf and
                                    min_lng - buf <= fbo["longitude"] <= max_lng + buf):
                                affected.append(fbo["code"])

                if not affected:
                    # Fallback: check if FBO city appears in areaDesc
                    areas_lower = (props.get("areaDesc", "")).lower()
                    for fbo in state_fbos:
                        if fbo["city"].lower() in areas_lower:
                            affected.append(fbo["code"])

                if affected:
                    matched_alerts.append({
                        "state": state,
                        "event": props.get("event", "Unknown"),
                        "severity": sev,
                        "headline": props.get("headline", ""),
                        "areas": props.get("areaDesc", ""),
                        "affected_fbos": affected,
                    })
        except Exception:
            pass

    return matched_alerts


class MarketUpdateGenerator:
    """Generates concise jet fuel market updates for email delivery."""

    def __init__(self, eia_client: Optional[EIAClient] = None,
                 news_client: Optional[NewsClient] = None,
                 a4a_client: Optional[A4AClient] = None):
        self.eia = eia_client or EIAClient()
        self.news = news_client or NewsClient()
        self.a4a = a4a_client or A4AClient()

    def _load_all_data(self):
        """Load all data sources."""
        data = self.eia.fetch_all(use_cache=True)
        data["a4a"] = self.a4a.get_prices(use_cache=True)
        data["live_crude"] = _fetch_live_crude()
        data["weather_alerts"] = _fetch_weather_alerts()
        return data

    def generate(self, time_of_day: str = "morning") -> str:
        """Generate a market update email body.

        Args:
            time_of_day: "morning", "midday", or "afternoon"
        """
        data = self._load_all_data()
        headlines = [
            h for h in self.news.get_energy_headlines(days_back=2, max_results=20)
            if _is_relevant_article(h.get("title", ""), h.get("description", ""), h.get("source", ""))
        ]

        label = {
            "morning": "MORNING BRIEFING",
            "midday": "MIDDAY UPDATE",
            "afternoon": "AFTERNOON CLOSE",
        }.get(time_of_day, "MARKET UPDATE")

        sections = [
            self._header(label),
            self._market_snapshot(data),
            self._supply_demand_pulse(data),
            self._breaking_news(headlines),
            self._weather_alerts(data),
            self._immediate_concerns(data, headlines),
            self._outlook_30_day(data),
            self._outlook_60_day(data),
            self._outlook_90_day(data),
            self._crack_spread(data),
            self._inventory_alert(data),
            self._footer(),
        ]

        return "\n\n".join(s for s in sections if s)

    def generate_html(self, time_of_day: str = "morning") -> str:
        """Generate an HTML-formatted email body."""
        data = self._load_all_data()
        headlines = [
            h for h in self.news.get_energy_headlines(days_back=2, max_results=20)
            if _is_relevant_article(h.get("title", ""), h.get("description", ""), h.get("source", ""))
        ]

        label = {
            "morning": "MORNING BRIEFING",
            "midday": "MIDDAY UPDATE",
            "afternoon": "AFTERNOON CLOSE",
        }.get(time_of_day, "MARKET UPDATE")

        now = datetime.now()

        # Build all content
        snapshot = self._market_snapshot_data(data)
        crack = self._crack_spread_data(data)
        inv_alerts = self._inventory_alert_data(data)
        sd_pulse = self._supply_demand_data(data)
        news_items = self._categorize_news(headlines)
        immediate = self._immediate_concerns_data(data, headlines)
        outlook_30 = self._outlook_30_data(data)
        outlook_60 = self._outlook_60_data(data)
        outlook_90 = self._outlook_90_data(data)

        html = f"""
<html>
<head>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #1a1a2e; background: #f5f5f5; margin: 0; padding: 0; }}
  .container {{ max-width: 680px; margin: 0 auto; background: #ffffff; }}
  .header {{ background: linear-gradient(135deg, #0f172a, #1e3a5f); color: white; padding: 28px 32px; }}
  .header h1 {{ margin: 0 0 4px 0; font-size: 22px; letter-spacing: 1px; }}
  .header .subtitle {{ color: #94a3b8; font-size: 13px; }}
  .section {{ padding: 20px 32px; border-bottom: 1px solid #e8e8e8; }}
  .section-title {{ font-size: 14px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; border-left: 3px solid #2563eb; padding-left: 10px; }}
  .metric-row {{ display: flex; justify-content: space-between; margin-bottom: 8px; }}
  .metric {{ text-align: center; flex: 1; }}
  .metric .value {{ font-size: 22px; font-weight: 700; color: #0f172a; }}
  .metric .label {{ font-size: 11px; color: #64748b; text-transform: uppercase; }}
  .metric .change {{ font-size: 12px; font-weight: 600; }}
  .up {{ color: #dc2626; }}
  .down {{ color: #16a34a; }}
  .neutral {{ color: #64748b; }}
  .alert-box {{ background: #fef2f2; border-left: 3px solid #dc2626; padding: 10px 14px; margin: 8px 0; font-size: 13px; }}
  .alert-box.warn {{ background: #fffbeb; border-left-color: #f59e0b; }}
  .alert-box.info {{ background: #eff6ff; border-left-color: #2563eb; }}
  .outlook-box {{ background: #f8fafc; border-radius: 6px; padding: 14px 16px; margin: 8px 0; }}
  .outlook-box h4 {{ margin: 0 0 6px 0; font-size: 13px; color: #334155; }}
  .outlook-box ul {{ margin: 4px 0; padding-left: 18px; font-size: 13px; line-height: 1.6; }}
  .news-item {{ font-size: 13px; margin-bottom: 6px; line-height: 1.4; }}
  .news-source {{ color: #94a3b8; font-size: 11px; }}
  .footer {{ background: #f8fafc; padding: 16px 32px; font-size: 11px; color: #94a3b8; text-align: center; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  td, th {{ padding: 6px 10px; text-align: left; }}
  th {{ background: #f1f5f9; font-weight: 600; font-size: 11px; text-transform: uppercase; color: #475569; }}
  tr:nth-child(even) {{ background: #fafafa; }}
</style>
</head>
<body>
<div class="container">

<div class="header">
  <h1>SIGNATURE ENERGY &mdash; {label}</h1>
  <div class="subtitle">{now.strftime('%A, %B %d, %Y &bull; %I:%M %p ET')} &bull; Jet Fuel Market Intelligence</div>
</div>
"""

        # Market Snapshot
        html += '<div class="section"><div class="section-title">Market Snapshot</div>'
        if snapshot:
            html += '<table><tr>'
            for s in snapshot:
                chg_class = "up" if s.get("direction") == "up" else ("down" if s.get("direction") == "down" else "neutral")
                html += f"""
                <td style="text-align:center; padding: 10px;">
                  <div style="font-size:11px; color:#64748b; text-transform:uppercase;">{s['label']}</div>
                  <div style="font-size:20px; font-weight:700;">{s['value']}</div>
                  <div class="{chg_class}" style="font-size:12px; font-weight:600;">{s.get('change', '')}</div>
                </td>"""
            html += '</tr></table>'
        html += '</div>'

        # Supply/Demand
        if sd_pulse:
            html += '<div class="section"><div class="section-title">Supply / Demand Pulse</div>'
            for item in sd_pulse:
                html += f'<div style="font-size:13px; margin-bottom:4px;">{item}</div>'
            html += '</div>'

        # Breaking News
        if news_items:
            html += '<div class="section"><div class="section-title">Market-Moving Headlines</div>'
            for item in news_items[:8]:
                topic_color = {"Jet Fuel": "#dc2626", "Weather": "#f59e0b", "Refinery": "#ea580c",
                               "OPEC": "#7c3aed", "Geopolitical": "#be123c", "Supply Chain": "#0891b2",
                               "Demand": "#2563eb"}.get(item.get("topic", ""), "#64748b")
                url = item.get('url', '')
                title_html = f'<a href="{url}" style="color:#1a1a2e; text-decoration:none; border-bottom:1px solid #cbd5e1;">{item["title"]}</a>' if url else item['title']
                html += f"""
                <div class="news-item">
                  <span style="background:{topic_color}; color:white; padding:1px 6px; border-radius:3px; font-size:10px; font-weight:600;">{item.get('topic','')}</span>
                  &nbsp;{title_html}
                  <span class="news-source">&mdash; {item['source']}, {item.get('published','')}</span>
                </div>"""
            html += '</div>'

        # Weather Alerts — matched to Signature FBO bases
        weather = data.get("weather_alerts", [])
        if weather:
            html += '<div class="section"><div class="section-title">&#9888; Weather Alerts &mdash; Signature Bases (NWS)</div>'
            html += '<table><tr><th>Event</th><th>Severity</th><th>Affected Bases</th></tr>'
            seen = set()
            for w in weather[:15]:
                fbos_str = ", ".join(w.get("affected_fbos", []))
                key = (w["event"], fbos_str)
                if key in seen:
                    continue
                seen.add(key)
                sev_color = "#dc2626" if w["severity"] == "Extreme" else "#f59e0b"
                html += f'<tr><td>{w["event"]}</td>'
                html += f'<td style="color:{sev_color};font-weight:600;">{w["severity"]}</td>'
                html += f'<td style="font-size:12px; font-weight:600;">{fbos_str}</td></tr>'
            html += '</table></div>'

        # Immediate Concerns
        if immediate:
            html += '<div class="section"><div class="section-title">&#9888; Immediate Concerns</div>'
            for concern in immediate:
                html += f'<div class="alert-box">{concern}</div>'
            html += '</div>'

        # 30/60/90 Outlook
        html += '<div class="section"><div class="section-title">Forward Outlook</div>'

        if outlook_30:
            html += '<div class="outlook-box"><h4>&#128197; 30-Day Outlook (April 2026)</h4><ul>'
            for item in outlook_30:
                html += f'<li>{item}</li>'
            html += '</ul></div>'

        if outlook_60:
            html += '<div class="outlook-box"><h4>&#128197; 60-Day Outlook (May 2026)</h4><ul>'
            for item in outlook_60:
                html += f'<li>{item}</li>'
            html += '</ul></div>'

        if outlook_90:
            html += '<div class="outlook-box"><h4>&#128197; 90-Day Outlook (June 2026)</h4><ul>'
            for item in outlook_90:
                html += f'<li>{item}</li>'
            html += '</ul></div>'

        html += '</div>'

        # Crack Spread (supporting data — after outlook)
        if crack:
            html += '<div class="section"><div class="section-title">Refining Margin (Crack Spread)</div>'
            level_class = "alert-box" if crack['level'] == 'high' else ("alert-box warn" if crack['level'] == 'medium' else "alert-box info")
            html += f"""
            <div style="font-size: 14px; margin-bottom: 8px;">
              Jet Fuel vs WTI: <strong>${crack['spread_gal']:.4f}/gal</strong> (${crack['spread_bbl']:.2f}/bbl)
            </div>
            <div class="{level_class}">{crack['interpretation']}</div>
            </div>"""

        # Inventory Watch (supporting data — after outlook)
        if inv_alerts:
            html += '<div class="section"><div class="section-title">Inventory Watch</div>'
            html += '<table><tr><th>Region</th><th>Level</th><th>W/W Change</th><th>Date</th><th>Signal</th></tr>'
            for inv in inv_alerts:
                chg_class = "up" if inv['change_val'] > 0 else ("down" if inv['change_val'] < 0 else "neutral")
                html += f'<tr><td>{inv["area"]}</td><td>{inv["level"]}</td><td class="{chg_class}">{inv["change"]}</td><td>{inv.get("date","")}</td><td>{inv["signal"]}</td></tr>'
            html += '</table></div>'

        # Footer
        html += f"""
<div class="footer">
  CONFIDENTIAL &mdash; Signature Energy Internal Use Only<br>
  Fuel Pricing Intelligence System &bull; Parker Gordon, Manager - Pricing<br>
  <em>Data: EIA, NewsAPI &bull; Generated {now.strftime('%Y-%m-%d %H:%M ET')}</em>
</div>

</div>
</body>
</html>"""

        return html

    # ── Data extraction helpers (for HTML) ────────────────────────────────

    def _market_snapshot_data(self, data: dict) -> list[dict]:
        """Extract key metrics for the snapshot row."""
        metrics = []

        # A4A/Argus Jet Fuel Index — the primary jet fuel benchmark
        a4a = data.get("a4a", [])
        if a4a:
            current = a4a[-1]["value"]
            latest_date = a4a[-1]["date"]
            prev = a4a[-2]["value"] if len(a4a) >= 2 else current
            chg = current - prev
            pct = (chg / prev * 100) if prev else 0
            d = "up" if chg > 0 else ("down" if chg < 0 else "neutral")
            sign = "+" if chg > 0 else ""
            metrics.append({
                "label": f"Jet Fuel A4A/Argus ({latest_date})",
                "value": f"${current:.4f}/gal",
                "change": f"{sign}{chg:.4f} ({sign}{pct:.1f}%)",
                "direction": d,
            })

        gc = data.get("jet_spot_gc", [])
        if gc:
            current = gc[-1]["value"]
            latest_date = gc[-1]["date"]
            prev = gc[-2]["value"] if len(gc) >= 2 else current
            chg = current - prev
            pct = (chg / prev * 100) if prev else 0
            d = "up" if chg > 0 else ("down" if chg < 0 else "neutral")
            sign = "+" if chg > 0 else ""
            metrics.append({
                "label": f"GC Jet Spot ({latest_date})",
                "value": f"${current:.4f}/gal",
                "change": f"{sign}{chg:.4f} ({sign}{pct:.1f}%)",
                "direction": d,
            })

        # Use live Yahoo Finance for WTI/Brent if available, fallback to EIA
        live_crude = data.get("live_crude", {})
        for sym, name in [("CL=F", "WTI Crude"), ("BZ=F", "Brent Crude")]:
            if sym in live_crude:
                lc = live_crude[sym]
                current = lc["price"]
                chg = lc["change"]
                pct = lc["pct"]
                d = "up" if (chg or 0) > 0 else ("down" if (chg or 0) < 0 else "neutral")
                sign = "+" if (chg or 0) > 0 else ""
                chg_str = f"{sign}${chg:.2f} ({sign}{pct:.1f}%)" if chg is not None else ""
                metrics.append({
                    "label": f"{name} (Live)",
                    "value": f"${current:.2f}/bbl",
                    "change": chg_str,
                    "direction": d,
                })
            else:
                # Fallback to EIA
                key = "wti" if "WTI" in name else "brent"
                prices = data.get(key, [])
                if prices:
                    current = prices[-1]["value"]
                    latest_date = prices[-1]["date"]
                    prev = prices[-2]["value"] if len(prices) >= 2 else current
                    chg = current - prev
                    d = "up" if chg > 0 else ("down" if chg < 0 else "neutral")
                    sign = "+" if chg > 0 else ""
                    metrics.append({
                        "label": f"{name} (EIA {latest_date})",
                        "value": f"${current:.2f}/bbl",
                        "change": f"{sign}${chg:.2f}",
                        "direction": d,
                    })

        return metrics

    def _crack_spread_data(self, data: dict) -> Optional[dict]:
        gc = data.get("jet_spot_gc", [])
        wti = data.get("wti", [])
        if not gc or not wti:
            return None
        jet_gal = gc[-1]["value"]
        wti_bbl = wti[-1]["value"]
        wti_gal = wti_bbl / 42
        crack_gal = jet_gal - wti_gal
        crack_bbl = crack_gal * 42

        if crack_bbl > 35:
            level, interp = "high", f"WIDE spread at ${crack_bbl:.2f}/bbl — jet fuel is expensive relative to crude. Indicates tight jet supply, strong demand, or refining constraints. Upward price pressure likely."
        elif crack_bbl > 20:
            level, interp = "medium", f"Moderate spread at ${crack_bbl:.2f}/bbl — refining margins are healthy. Normal market conditions."
        else:
            level, interp = "low", f"Narrow spread at ${crack_bbl:.2f}/bbl — jet fuel is cheap relative to crude. Downside price risk is limited."

        return {"spread_gal": crack_gal, "spread_bbl": crack_bbl, "level": level, "interpretation": interp}

    def _inventory_alert_data(self, data: dict) -> list[dict]:
        inv = data.get("jet_inventories", [])
        if not inv:
            return []

        by_area = {}
        for r in inv:
            area = r.get("area-name", "Unknown")
            by_area.setdefault(area, []).append(r)

        alerts = []
        padd_names = {"PADD 1": "East Coast", "PADD 2": "Midwest", "PADD 3": "Gulf Coast",
                      "PADD 4": "Rocky Mtn", "PADD 5": "West Coast", "U.S.": "Total U.S."}

        for area in ["U.S.", "PADD 1", "PADD 2", "PADD 3", "PADD 4", "PADD 5"]:
            records = by_area.get(area, [])
            if len(records) < 2:
                continue
            latest = records[-1]
            prev = records[-2]
            val = float(latest["value"])
            chg = val - float(prev["value"])
            sign = "+" if chg > 0 else ""

            if chg < -500:
                signal = "⚠️ SIGNIFICANT DRAW"
            elif chg < -200:
                signal = "📉 Draw"
            elif chg > 500:
                signal = "📈 Large build"
            elif chg > 200:
                signal = "📈 Build"
            else:
                signal = "➡️ Stable"

            label = padd_names.get(area, area)
            latest_date = latest.get("date", "")
            alerts.append({
                "area": f"{area} ({label})",
                "level": f"{val/1000:.1f}M bbl ({val:,.0f} KB)",
                "change": f"{sign}{chg:,.0f} KB",
                "change_val": chg,
                "signal": signal,
                "date": latest_date,
            })

        return alerts

    def _supply_demand_data(self, data: dict) -> list[str]:
        items = []
        prod = data.get("jet_production", [])
        demand = data.get("jet_demand", [])

        us_prod = [r for r in prod if r.get("area-name") == "U.S."]
        if us_prod:
            p = float(us_prod[-1]["value"])
            items.append(f"<strong>U.S. Jet Fuel Production:</strong> {p/1000:.2f}M bbl/day ({p:,.0f} kbd) ({us_prod[-1]['date']})")

        if demand:
            d = float(demand[-1]["value"])
            items.append(f"<strong>U.S. Jet Fuel Demand (product supplied):</strong> {d/1000:.2f}M bbl/day ({d:,.0f} kbd) ({demand[-1]['date']})")

            if us_prod:
                balance = float(us_prod[-1]["value"]) - d
                sign = "+" if balance >= 0 else ""
                if balance >= 0:
                    balance_note = "surplus available for export or storage builds"
                else:
                    balance_note = "deficit must be covered by imports or inventory draws"
                items.append(f"<strong>Domestic balance:</strong> {sign}{balance/1000:.2f}M bbl/day ({sign}{balance:,.0f} kbd) — {balance_note}")

            if len(demand) >= 4:
                avg4 = sum(float(r["value"]) for r in demand[-4:]) / 4
                items.append(f"<strong>4-week avg jet fuel demand:</strong> {avg4/1000:.2f}M bbl/day ({avg4:,.0f} kbd)")

        return items

    def _categorize_news(self, headlines: list[dict]) -> list[dict]:
        # Sort: jet fuel first, then weather, then by recency
        priority = {"Jet Fuel": 0, "Weather": 1, "Refinery": 2, "OPEC": 3,
                     "Geopolitical": 4, "Supply Chain": 5, "Demand": 6}
        return sorted(headlines, key=lambda h: (priority.get(h.get("topic", ""), 99), h.get("published", "")))

    def _immediate_concerns_data(self, data: dict, headlines: list[dict]) -> list[str]:
        concerns = []

        # ── 1. A4A Jet Fuel price momentum (our primary benchmark) ──
        a4a = data.get("a4a", [])
        if len(a4a) >= 6:
            current = a4a[-1]["value"]
            prev_day = a4a[-2]["value"]
            day_chg = current - prev_day
            day_pct = (day_chg / prev_day * 100) if prev_day else 0
            avg_5d = sum(p["value"] for p in a4a[-6:-1]) / 5

            if current > avg_5d * 1.03:
                pct_above = ((current - avg_5d) / avg_5d) * 100
                concerns.append(
                    f"<strong>Jet fuel price spike:</strong> A4A/Argus at ${current:.4f}/gal is "
                    f"{pct_above:.1f}% above the 5-day average (${avg_5d:.4f}). "
                    f"Day/day move: {'+' if day_chg > 0 else ''}{day_pct:.1f}%. "
                    f"Monitor for sustained rally — consider accelerating near-term procurement."
                )
            elif current < avg_5d * 0.97:
                pct_below = ((avg_5d - current) / avg_5d) * 100
                concerns.append(
                    f"<strong>Jet fuel price decline:</strong> A4A/Argus at ${current:.4f}/gal is "
                    f"{pct_below:.1f}% below 5-day average (${avg_5d:.4f}). "
                    f"Potential buying opportunity if downtrend continues."
                )
            elif abs(day_pct) > 1.5:
                direction = "up" if day_chg > 0 else "down"
                concerns.append(
                    f"<strong>Notable daily move:</strong> A4A/Argus {direction} "
                    f"{abs(day_pct):.1f}% to ${current:.4f}/gal. "
                    f"{'Upward pressure building.' if day_chg > 0 else 'Watch for continued softening.'}"
                )

        # ── 2. Crude oil momentum (live from Yahoo Finance) ──
        live_crude = data.get("live_crude", {})
        for sym, label in [("CL=F", "WTI"), ("BZ=F", "Brent")]:
            if sym in live_crude:
                lc = live_crude[sym]
                pct = lc.get("pct") or 0
                price = lc["price"]
                if abs(pct) > 2.0:
                    direction = "surging" if pct > 0 else "dropping"
                    concerns.append(
                        f"<strong>{label} crude {direction}:</strong> ${price:.2f}/bbl "
                        f"({'+' if pct > 0 else ''}{pct:.1f}% today). "
                        f"{'Crude strength will flow through to jet fuel costs within 1-2 days.' if pct > 0 else 'Crude weakness may ease jet fuel prices short-term.'}"
                    )

        # ── 3. Crack spread analysis ──
        gc_data = data.get("jet_spot_gc", [])
        wti = data.get("wti", [])
        if gc_data and wti:
            jet_gal = gc_data[-1]["value"]
            wti_bbl = wti[-1]["value"]
            crack_bbl = (jet_gal - wti_bbl / 42) * 42
            if crack_bbl > 50:
                concerns.append(
                    f"<strong>Extreme crack spread:</strong> ${crack_bbl:.2f}/bbl — jet fuel is "
                    f"sharply overpriced vs crude input. Indicates severe refining constraints, "
                    f"supply disruption, or exceptional seasonal demand. "
                    f"This premium may persist if refinery capacity remains tight."
                )
            elif crack_bbl > 35:
                concerns.append(
                    f"<strong>Wide crack spread:</strong> ${crack_bbl:.2f}/bbl — jet fuel elevated "
                    f"vs crude input. Likely driven by refining capacity constraints or strong "
                    f"seasonal demand. Watch for refinery restarts that could compress margins."
                )

        # ── 4. Inventory analysis (national + regional) ──
        inv = data.get("jet_inventories", [])
        us_inv = [r for r in inv if r.get("area-name") == "U.S."]
        if len(us_inv) >= 2:
            latest = float(us_inv[-1]["value"])
            prev = float(us_inv[-2]["value"])
            chg = latest - prev
            if chg < -1000:
                concerns.append(
                    f"<strong>Major inventory draw:</strong> U.S. jet fuel stocks fell "
                    f"{abs(chg):,.0f} KB last week to {latest:,.0f} KB. "
                    f"Back-to-back draws of this magnitude signal supply stress — "
                    f"monitor PADD 3 (Gulf Coast) refinery output closely."
                )
            elif chg < -500:
                concerns.append(
                    f"<strong>Inventory draw:</strong> U.S. jet fuel stocks declined "
                    f"{abs(chg):,.0f} KB to {latest:,.0f} KB. Consecutive draws would "
                    f"signal tightening — next EIA report (Wednesday 10:30 AM ET) is key."
                )

        # PADD-level regional concerns
        padd_names = {"PADD 1": "East Coast", "PADD 3": "Gulf Coast", "PADD 5": "West Coast"}
        for padd, region in padd_names.items():
            padd_inv = [r for r in inv if r.get("area-name") == padd]
            if len(padd_inv) >= 2:
                latest_p = float(padd_inv[-1]["value"])
                prev_p = float(padd_inv[-2]["value"])
                chg_p = latest_p - prev_p
                pct_p = (chg_p / prev_p * 100) if prev_p else 0
                if pct_p < -5:
                    concerns.append(
                        f"<strong>{region} ({padd}) supply tightening:</strong> Regional stocks "
                        f"down {abs(pct_p):.1f}% ({abs(chg_p):,.0f} KB) to {latest_p:,.0f} KB. "
                        f"Regional premium risk for {region} deliveries."
                    )

        # ── 5. Production vs demand imbalance ──
        prod = data.get("jet_production", [])
        demand = data.get("jet_demand", [])
        us_prod = [r for r in prod if r.get("area-name") == "U.S."]
        if us_prod and demand:
            prod_val = float(us_prod[-1]["value"])
            demand_val = float(demand[-1]["value"])
            balance = prod_val - demand_val
            if balance < 0:
                concerns.append(
                    f"<strong>Production deficit:</strong> U.S. jet fuel production "
                    f"({prod_val/1000:.2f}M bbl/d) is below demand ({demand_val/1000:.2f}M bbl/d). "
                    f"Deficit of {abs(balance/1000):.2f}M bbl/d must be met by imports or "
                    f"inventory drawdowns — bullish for prices."
                )

        # ── 6. NWS weather alerts affecting supply infrastructure ──
        weather_alerts = data.get("weather_alerts", [])
        # Focus on states with major refining/pipeline infrastructure
        supply_states = {"TX", "LA", "FL", "NJ", "CA", "IL"}
        supply_alerts = [a for a in weather_alerts if a["state"] in supply_states]
        if supply_alerts:
            states_affected = sorted(set(a["state"] for a in supply_alerts))
            events = sorted(set(a["event"] for a in supply_alerts))
            concerns.append(
                f"<strong>Active weather threats:</strong> {', '.join(events)} in "
                f"{', '.join(states_affected)}. These states host critical refining and "
                f"pipeline infrastructure — monitor for operational impacts."
            )

        # ── 7. Geopolitical and supply chain headline risks ──
        risk_topics = {"Weather", "Supply Chain", "Geopolitical"}
        risk_news = [h for h in headlines if h.get("topic") in risk_topics]
        if risk_news:
            for h in risk_news[:2]:
                url = h.get('url', '')
                title_part = f'<a href="{url}" style="color:#1a1a2e;">{h["title"]}</a>' if url else h['title']
                concerns.append(
                    f"<strong>{h['topic']} risk:</strong> {title_part} "
                    f"<em>({h['source']})</em>"
                )

        if not concerns:
            concerns.append(
                "No immediate red flags. Markets appear stable with balanced fundamentals. "
                "Continue monitoring EIA weekly data (Wednesday 10:30 AM ET) and "
                "OPEC+ compliance reports for shifts."
            )

        return concerns

    def _outlook_30_data(self, data: dict) -> list[str]:
        items = []
        now = datetime.now()
        next_month = now + timedelta(days=30)

        gc = data.get("jet_spot_gc", [])
        if len(gc) >= 20:
            prices = [p["value"] for p in gc]
            ma5 = sum(prices[-5:]) / 5
            ma20 = sum(prices[-20:]) / 20
            if ma5 > ma20 * 1.005:
                items.append("<strong>Trend: BULLISH</strong> — 5-day moving average above 20-day moving average. Prices trending up. PWA contracts likely cheaper than PDA.")
            elif ma5 < ma20 * 0.995:
                items.append("<strong>Trend: BEARISH</strong> — 5-day moving average below 20-day moving average. Prices trending down. PDA contracts may offer savings.")
            else:
                items.append("<strong>Trend: NEUTRAL</strong> — Moving averages converging. Minimal spread between contract types.")

        # Inventory trend (last 4 weeks)
        inv = data.get("jet_inventories", [])
        us_inv = [r for r in inv if r.get("area-name") == "U.S."]
        if len(us_inv) >= 4:
            recent_4 = [float(r["value"]) for r in us_inv[-4:]]
            trend = recent_4[-1] - recent_4[0]
            if trend < -1000:
                items.append(f"<strong>Inventories declining:</strong> U.S. stocks down {abs(trend):,.0f} KB over last 4 weeks. Sustained draws support higher prices.")
            elif trend > 1000:
                items.append(f"<strong>Inventories building:</strong> U.S. stocks up {trend:,.0f} KB over last 4 weeks. Builds should ease price pressure.")
            else:
                items.append("Inventories relatively stable over last 4 weeks.")

        items.append("Spring refinery maintenance season — watch for unplanned outages that could tighten regional supply.")
        items.append("EIA weekly reports (every Wednesday 10:30 AM ET) remain the key near-term data catalyst.")

        return items

    def _outlook_60_data(self, data: dict) -> list[str]:
        items = []
        items.append("<strong>Seasonal demand ramp:</strong> Memorial Day (May 25) kicks off summer travel. Airline capacity additions will drive jet fuel demand higher.")
        items.append("Refinery turnarounds should be completing — watch for restart announcements that could ease supply.")
        items.append("OPEC+ production decisions: any quota changes will flow through to crude costs within 4-6 weeks.")
        items.append("Monitor Atlantic hurricane season forecasts (official NOAA outlook due in May) — Gulf Coast refining at risk.")

        # Demand trend
        demand = data.get("jet_demand", [])
        if len(demand) >= 8:
            recent = sum(float(r["value"]) for r in demand[-4:]) / 4
            prior = sum(float(r["value"]) for r in demand[-8:-4]) / 4
            if recent > prior * 1.03:
                items.append(f"<strong>Demand accelerating:</strong> 4-week avg ({recent/1000:.2f}M bbl/d) is {((recent-prior)/prior*100):.1f}% above prior 4-week period. Summer ramp may be starting early.")

        return items

    def _outlook_90_data(self, data: dict) -> list[str]:
        items = []
        items.append("<strong>Peak summer demand:</strong> June is historically the start of peak jet fuel consumption. Plan procurement accordingly.")
        items.append("Atlantic hurricane season begins June 1 — Gulf Coast supply disruption risk increases significantly.")
        items.append("SAF (Sustainable Aviation Fuel) blending mandates and EPA regulatory updates may affect supply/pricing dynamics.")
        items.append("Forward curve positioning: consider locking in favorable rates if current prices are below 90-day moving average.")

        # Long-term price context
        gc = data.get("jet_spot_gc", [])
        if len(gc) >= 60:
            current = gc[-1]["value"]
            avg_90d = sum(p["value"] for p in gc[-60:]) / 60
            if current < avg_90d * 0.95:
                items.append(f"<strong>Below average:</strong> Current spot (${current:.4f}) is {((avg_90d-current)/avg_90d*100):.1f}% below 60-day average (${avg_90d:.4f}). May present a hedging opportunity.")
            elif current > avg_90d * 1.05:
                items.append(f"<strong>Above average:</strong> Current spot (${current:.4f}) is {((current-avg_90d)/avg_90d*100):.1f}% above 60-day average (${avg_90d:.4f}). Prices are elevated.")

        return items

    # ── Plain text sections (for text-only email fallback) ────────────────

    def _header(self, label: str) -> str:
        now = datetime.now()
        return "\n".join([
            "=" * 72,
            f"  SIGNATURE ENERGY — {label}",
            f"  {now.strftime('%A, %B %d, %Y • %I:%M %p ET')}",
            f"  Jet Fuel Market Intelligence",
            "=" * 72,
        ])

    def _market_snapshot(self, data: dict) -> str:
        lines = ["MARKET SNAPSHOT", "-" * 40]

        a4a = data.get("a4a", [])
        if a4a:
            current = a4a[-1]["value"]
            lines.append(f"  Jet Fuel (A4A/Argus):  ${current:.4f}/gal ({a4a[-1]['date']})")
            if len(a4a) >= 2:
                prev = a4a[-2]["value"]
                chg = current - prev
                sign = "+" if chg > 0 else ""
                pct = (chg / prev * 100) if prev else 0
                lines.append(f"    Day/Day: {sign}${chg:.4f} ({sign}{pct:.1f}%)")

        gc = data.get("jet_spot_gc", [])
        if gc:
            current = gc[-1]["value"]
            lines.append(f"  GC Jet Spot:  ${current:.4f}/gal ({gc[-1]['date']})")
            if len(gc) >= 2:
                prev = gc[-2]["value"]
                chg = current - prev
                sign = "+" if chg > 0 else ""
                pct = (chg / prev * 100) if prev else 0
                lines.append(f"    Day/Day: {sign}${chg:.4f} ({sign}{pct:.1f}%)")

        live_crude = data.get("live_crude", {})
        for sym, name in [("CL=F", "WTI Crude"), ("BZ=F", "Brent Crude")]:
            if sym in live_crude:
                lc = live_crude[sym]
                lines.append(f"  {name}:  ${lc['price']:.2f}/bbl (Live)")
                if lc["change"] is not None:
                    sign = "+" if lc["change"] > 0 else ""
                    lines.append(f"    Day/Day: {sign}${lc['change']:.2f} ({sign}{lc['pct']:.1f}%)")
            else:
                key = "wti" if "WTI" in name else "brent"
                prices = data.get(key, [])
                if prices:
                    lines.append(f"  {name}:  ${prices[-1]['value']:.2f}/bbl (EIA {prices[-1]['date']})")

        return "\n".join(lines)

    def _crack_spread(self, data: dict) -> str:
        gc = data.get("jet_spot_gc", [])
        wti = data.get("wti", [])
        if not gc or not wti:
            return ""
        jet_gal = gc[-1]["value"]
        wti_gal = wti[-1]["value"] / 42
        crack = jet_gal - wti_gal
        crack_bbl = crack * 42
        lines = [
            "CRACK SPREAD (Refining Margin)", "-" * 40,
            f"  Jet vs WTI: ${crack:.4f}/gal (${crack_bbl:.2f}/bbl)",
        ]
        if crack_bbl > 35:
            lines.append("  *** WIDE — upward price pressure, tight supply ***")
        elif crack_bbl < 15:
            lines.append("  Narrow — limited downside risk")
        return "\n".join(lines)

    def _inventory_alert(self, data: dict) -> str:
        inv = data.get("jet_inventories", [])
        if not inv:
            return ""
        lines = ["INVENTORY WATCH", "-" * 40]
        by_area = {}
        for r in inv:
            by_area.setdefault(r.get("area-name", ""), []).append(r)
        for area in ["U.S.", "PADD 3"]:
            records = by_area.get(area, [])
            if len(records) >= 2:
                latest = float(records[-1]["value"])
                latest_date = records[-1].get("date", "")
                chg = latest - float(records[-2]["value"])
                sign = "+" if chg > 0 else ""
                flag = " *** DRAW ***" if chg < -500 else ""
                lines.append(f"  {area}: {latest:,.0f} KB ({sign}{chg:,.0f} w/w) [{latest_date}]{flag}")
        return "\n".join(lines)

    def _supply_demand_pulse(self, data: dict) -> str:
        lines = ["JET FUEL SUPPLY/DEMAND", "-" * 40]
        prod = [r for r in data.get("jet_production", []) if r.get("area-name") == "U.S."]
        demand = data.get("jet_demand", [])
        if prod:
            p = float(prod[-1]['value'])
            lines.append(f"  U.S. Jet Fuel Production: {p/1000:.2f}M bbl/d ({p:,.0f} kbd)")
        if demand:
            d = float(demand[-1]['value'])
            lines.append(f"  U.S. Jet Fuel Demand:     {d/1000:.2f}M bbl/d ({d:,.0f} kbd)")
        return "\n".join(lines)

    def _breaking_news(self, headlines: list[dict]) -> str:
        if not headlines:
            return ""
        lines = ["MARKET-MOVING HEADLINES", "-" * 40]
        for h in headlines[:6]:
            lines.append(f"  [{h.get('topic','')}] {h['title']}")
            lines.append(f"    — {h['source']}, {h.get('published','')}")
        return "\n".join(lines)

    def _weather_alerts(self, data: dict) -> str:
        weather = data.get("weather_alerts", [])
        if not weather:
            return ""
        lines = ["WEATHER ALERTS - SIGNATURE BASES (NWS)", "-" * 40]
        seen = set()
        for w in weather[:10]:
            fbos = ", ".join(w.get("affected_fbos", []))
            key = (w["event"], fbos)
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"  [{w['severity']}] {w['event']} -- Bases: {fbos}")
            if w.get("headline"):
                lines.append(f"    {w['headline'][:100]}")
        return "\n".join(lines)

    def _immediate_concerns(self, data: dict, headlines: list[dict]) -> str:
        concerns = self._immediate_concerns_data(data, headlines)
        lines = ["⚠ IMMEDIATE CONCERNS", "-" * 40]
        # Strip HTML tags for plain text
        import re
        for c in concerns:
            clean = re.sub(r'<[^>]+>', '', c)
            lines.append(f"  • {clean}")
        return "\n".join(lines)

    def _outlook_30_day(self, data: dict) -> str:
        import re
        items = self._outlook_30_data(data)
        lines = ["30-DAY OUTLOOK", "-" * 40]
        for item in items:
            clean = re.sub(r'<[^>]+>', '', item)
            lines.append(f"  • {clean}")
        return "\n".join(lines)

    def _outlook_60_day(self, data: dict) -> str:
        import re
        items = self._outlook_60_data(data)
        lines = ["60-DAY OUTLOOK", "-" * 40]
        for item in items:
            clean = re.sub(r'<[^>]+>', '', item)
            lines.append(f"  • {clean}")
        return "\n".join(lines)

    def _outlook_90_day(self, data: dict) -> str:
        import re
        items = self._outlook_90_data(data)
        lines = ["90-DAY OUTLOOK", "-" * 40]
        for item in items:
            clean = re.sub(r'<[^>]+>', '', item)
            lines.append(f"  • {clean}")
        return "\n".join(lines)

    def _footer(self) -> str:
        return "\n".join([
            "=" * 72,
            "  CONFIDENTIAL — Signature Energy Internal Use Only",
            f"  Fuel Pricing Intelligence System • Parker Gordon",
            f"  Data: EIA, NewsAPI • {datetime.now().strftime('%Y-%m-%d %H:%M ET')}",
            "=" * 72,
        ])
