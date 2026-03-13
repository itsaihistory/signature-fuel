"""
Platts (S&P Global Commodity Insights) API client.

Fetches daily jet fuel price assessments — the actual indices that
Signature's supply contracts price off of.

REQUIRES: Enterprise subscription to S&P Global Market Data API.
API docs: https://developer.platts.com/

This module is READY TO ACTIVATE once Platts credentials are available.
Until then, the system uses EIA Gulf Coast spot as a proxy.
"""

import json
import logging
from datetime import date, timedelta
from typing import Optional
from urllib.request import urlopen, Request
from urllib.parse import urlencode

from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)

# Platts symbol codes for jet fuel assessments
# These are the actual symbols — confirm with Platts subscription docs
PLATTS_SYMBOLS = {
    # Gulf Coast
    "gc_jet_pda":  "AAJKC00",   # US Gulf Coast Jet 54 (Prior Day)
    "gc_jet_pwa":  "AAJKC03",   # US Gulf Coast Jet 54 (Prior Week Avg)

    # NY Harbor
    "nyh_jet_pda": "AAJKN00",   # NY Harbor Jet 54 (Prior Day)
    "nyh_jet_pwa": "AAJKN03",   # NY Harbor Jet 54 (Prior Week Avg)

    # Los Angeles
    "la_jet_pda":  "AAJKL00",   # LA Jet 54 (Prior Day)
    "la_jet_pwa":  "AAJKL03",   # LA Jet 54 (Prior Week Avg)

    # Rocky Mountain / Group 3
    "rm_jet_pda":  "AAJKR00",   # Rocky Mountain / Group 3 Jet (Prior Day)

    # Chicago
    "chi_jet_pda": "AAJKH00",   # Chicago Jet (Prior Day)
}

# Regional mapping — which Platts symbols to use per market
MARKET_TO_PLATTS = {
    "South Florida":        {"pwa": "gc_jet_pwa",  "pda": "gc_jet_pda"},
    "Houston":              {"pwa": "gc_jet_pwa",  "pda": "gc_jet_pda"},
    "Dallas / Fort Worth":  {"pwa": "gc_jet_pwa",  "pda": "gc_jet_pda"},
    "Atlanta":              {"pwa": "gc_jet_pwa",  "pda": "gc_jet_pda"},
    "New York / New Jersey":{"pwa": "nyh_jet_pwa", "pda": "nyh_jet_pda"},
    "Washington D.C.":      {"pwa": "nyh_jet_pwa", "pda": "nyh_jet_pda"},
    "Los Angeles":          {"pwa": "la_jet_pwa",  "pda": "la_jet_pda"},
    "San Francisco":        {"pwa": "la_jet_pwa",  "pda": "la_jet_pda"},
    "Seattle":              {"pwa": "la_jet_pwa",  "pda": "la_jet_pda"},
    "Denver":               {"pwa": "gc_jet_pwa",  "pda": "rm_jet_pda"},
    "Chicago":              {"pwa": "gc_jet_pwa",  "pda": "chi_jet_pda"},
    "Minneapolis":          {"pwa": "gc_jet_pwa",  "pda": "chi_jet_pda"},
}


class PlattsClient:
    """
    Fetches daily Platts jet fuel assessments via the S&P Global Market Data API.

    Authentication: API key + app key, provided with enterprise subscription.
    Base URL: https://api.platts.com/market-data/v3/
    """

    def __init__(self, api_key: str = "", app_key: str = ""):
        import os
        self.api_key = api_key or os.environ.get("PLATTS_API_KEY", "")
        self.app_key = app_key or os.environ.get("PLATTS_APP_KEY", "")
        self.base_url = "https://api.platts.com/market-data/v3"

        if not self.api_key:
            logger.warning("No Platts API key configured. Set PLATTS_API_KEY env var.")

    @property
    def available(self) -> bool:
        return bool(self.api_key and self.app_key)

    def get_assessment(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[dict]:
        """
        Fetch daily assessment for a Platts symbol.
        Returns [{"date": "YYYY-MM-DD", "value": float}] sorted ascending.
        """
        if not self.available:
            return []

        start = start_date or (date.today() - timedelta(days=30))
        end = end_date or date.today()

        url = (
            f"{self.base_url}/assessments?"
            f"symbol={symbol}"
            f"&startDate={start.isoformat()}"
            f"&endDate={end.isoformat()}"
        )

        headers = {
            "appkey": self.app_key,
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            return self._parse_response(body)
        except Exception as e:
            logger.error(f"Platts API error for {symbol}: {e}")
            return []

    def get_market_prices(
        self,
        market_name: str,
        days: int = 30,
    ) -> dict[str, list[dict]]:
        """
        Fetch PWA and PDA prices for a market.
        Returns {"pwa": [...], "pda": [...]}.
        """
        mapping = MARKET_TO_PLATTS.get(market_name, {})
        if not mapping:
            logger.warning(f"No Platts mapping for market: {market_name}")
            return {"pwa": [], "pda": []}

        start = date.today() - timedelta(days=days)
        pwa_sym = PLATTS_SYMBOLS.get(mapping["pwa"], "")
        pda_sym = PLATTS_SYMBOLS.get(mapping["pda"], "")

        return {
            "pwa": self.get_assessment(pwa_sym, start) if pwa_sym else [],
            "pda": self.get_assessment(pda_sym, start) if pda_sym else [],
        }

    def get_all_jet_assessments(self, days: int = 30) -> dict[str, list[dict]]:
        """Fetch all jet fuel symbols at once."""
        start = date.today() - timedelta(days=days)
        result = {}
        for key, symbol in PLATTS_SYMBOLS.items():
            result[key] = self.get_assessment(symbol, start)
            if result[key]:
                latest = result[key][-1]
                logger.info(f"  Platts {key}: {len(result[key])} records, latest ${latest['value']:.4f}")
        return result

    def _parse_response(self, body: dict) -> list[dict]:
        """Parse Platts API response into normalized records."""
        records = []
        # Platts v3 response structure varies — adapt to actual schema
        data = body.get("results", body.get("data", []))
        for row in data:
            dt = row.get("assessDate", row.get("date", ""))
            val = row.get("value", row.get("price"))
            if dt and val is not None:
                records.append({"date": str(dt)[:10], "value": float(val)})
        records.sort(key=lambda r: r["date"])
        return records
