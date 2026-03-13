"""
Argus Media API client.

Fetches daily jet fuel price assessments from Argus Direct API.
Some Signature contracts price off Argus indices rather than Platts.

REQUIRES: Enterprise subscription to Argus Direct.
API docs: https://direct.argusmedia.com/integration/

This module is READY TO ACTIVATE once Argus credentials are available.
"""

import json
import logging
from datetime import date, timedelta
from typing import Optional
from urllib.request import urlopen, Request

from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)

# Argus price codes for jet fuel assessments
# Confirm exact codes with Argus subscription documentation
ARGUS_SYMBOLS = {
    # Gulf Coast
    "gc_jet":       "PA0002956",  # US Gulf Coast Jet 54
    "gc_jet_pwa":   "PA0002957",  # US Gulf Coast Jet 54 Prior Week Avg

    # NY Harbor / East Coast
    "nyh_jet":      "PA0002960",  # NY Harbor Jet 54
    "nyh_jet_pwa":  "PA0002961",  # NY Harbor Jet 54 Prior Week Avg

    # Los Angeles / West Coast
    "la_jet":       "PA0002964",  # LA Jet 54
    "la_jet_pwa":   "PA0002965",  # LA Jet 54 Prior Week Avg

    # Group 3 / Midcontinent
    "group3_jet":   "PA0002968",  # Group 3 Jet 54

    # Chicago
    "chi_jet":      "PA0002970",  # Chicago Jet 54
}

# Regional mapping — which Argus symbols to use per market
MARKET_TO_ARGUS = {
    "South Florida":        {"pwa": "gc_jet_pwa",   "pda": "gc_jet"},
    "Houston":              {"pwa": "gc_jet_pwa",   "pda": "gc_jet"},
    "Dallas / Fort Worth":  {"pwa": "gc_jet_pwa",   "pda": "gc_jet"},
    "Atlanta":              {"pwa": "gc_jet_pwa",   "pda": "gc_jet"},
    "New York / New Jersey":{"pwa": "nyh_jet_pwa",  "pda": "nyh_jet"},
    "Washington D.C.":      {"pwa": "nyh_jet_pwa",  "pda": "nyh_jet"},
    "Los Angeles":          {"pwa": "la_jet_pwa",   "pda": "la_jet"},
    "San Francisco":        {"pwa": "la_jet_pwa",   "pda": "la_jet"},
    "Seattle":              {"pwa": "la_jet_pwa",   "pda": "la_jet"},
    "Denver":               {"pwa": "gc_jet_pwa",   "pda": "group3_jet"},
    "Chicago":              {"pwa": "gc_jet_pwa",   "pda": "chi_jet"},
    "Minneapolis":          {"pwa": "gc_jet_pwa",   "pda": "chi_jet"},
}


class ArgusClient:
    """
    Fetches daily Argus jet fuel assessments via Argus Direct API.

    Authentication: API key or OAuth token, provided with subscription.
    Base URL: https://api.argusmedia.com/v1/
    """

    def __init__(self, api_key: str = ""):
        import os
        self.api_key = api_key or os.environ.get("ARGUS_API_KEY", "")
        self.base_url = "https://api.argusmedia.com/v1"

        if not self.api_key:
            logger.warning("No Argus API key configured. Set ARGUS_API_KEY env var.")

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def get_assessment(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[dict]:
        """
        Fetch daily assessment for an Argus price code.
        Returns [{"date": "YYYY-MM-DD", "value": float}] sorted ascending.
        """
        if not self.available:
            return []

        start = start_date or (date.today() - timedelta(days=30))
        end = end_date or date.today()

        url = (
            f"{self.base_url}/prices?"
            f"priceCode={symbol}"
            f"&startDate={start.isoformat()}"
            f"&endDate={end.isoformat()}"
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            return self._parse_response(body)
        except Exception as e:
            logger.error(f"Argus API error for {symbol}: {e}")
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
        mapping = MARKET_TO_ARGUS.get(market_name, {})
        if not mapping:
            logger.warning(f"No Argus mapping for market: {market_name}")
            return {"pwa": [], "pda": []}

        start = date.today() - timedelta(days=days)
        pwa_sym = ARGUS_SYMBOLS.get(mapping["pwa"], "")
        pda_sym = ARGUS_SYMBOLS.get(mapping["pda"], "")

        return {
            "pwa": self.get_assessment(pwa_sym, start) if pwa_sym else [],
            "pda": self.get_assessment(pda_sym, start) if pda_sym else [],
        }

    def _parse_response(self, body: dict) -> list[dict]:
        """Parse Argus API response into normalized records."""
        records = []
        data = body.get("results", body.get("data", []))
        for row in data:
            dt = row.get("assessmentDate", row.get("date", ""))
            val = row.get("value", row.get("price"))
            if dt and val is not None:
                records.append({"date": str(dt)[:10], "value": float(val)})
        records.sort(key=lambda r: r["date"])
        return records
