"""
OPIS (Oil Price Information Service) API client.

Fetches terminal-level rack pricing — the most granular fuel pricing data
available. Maps directly to Signature's actual supply cost at each terminal.

REQUIRES: OPIS subscription (owned by Dow Jones/News Corp).
OPIS provides rack prices via API feeds and FTP data delivery.

This module is READY TO ACTIVATE once OPIS credentials are available.
"""

import json
import logging
from datetime import date, timedelta
from typing import Optional
from urllib.request import urlopen, Request

from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)


class OPISClient:
    """
    Fetches terminal rack prices from OPIS.

    OPIS rack data gives $/gallon at specific terminals (e.g., Tampa rack,
    Houston rack, LA rack). This is the closest data to Signature's actual
    cost basis since fuel is physically pulled from these terminals.
    """

    def __init__(self, api_key: str = "", username: str = "", password: str = ""):
        import os
        self.api_key = api_key or os.environ.get("OPIS_API_KEY", "")
        self.username = username or os.environ.get("OPIS_USERNAME", "")
        self.password = password or os.environ.get("OPIS_PASSWORD", "")
        # OPIS base URL — confirm with subscription documentation
        self.base_url = "https://api.opisnet.com/v1"

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def get_rack_price(
        self,
        terminal_city: str,
        product: str = "jet_a",
        days: int = 30,
    ) -> list[dict]:
        """
        Fetch rack price for a product at a terminal city.
        Returns [{"date": "YYYY-MM-DD", "value": float, "terminal": str}].
        """
        if not self.available:
            return []

        # OPIS uses terminal city codes — these need to be confirmed with
        # actual OPIS documentation and terminal directory
        start = (date.today() - timedelta(days=days)).isoformat()

        try:
            url = (
                f"{self.base_url}/rack-prices"
                f"?apikey={self.api_key}"
                f"&city={terminal_city}"
                f"&product={product}"
                f"&startDate={start}"
            )
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            return self._parse_response(body, terminal_city)
        except Exception as e:
            logger.error(f"OPIS API error for {terminal_city}: {e}")
            return []

    def get_terminal_prices(
        self,
        terminal_cities: list[str],
        product: str = "jet_a",
    ) -> dict[str, list[dict]]:
        """Fetch rack prices for multiple terminals."""
        result = {}
        for city in terminal_cities:
            data = self.get_rack_price(city, product)
            result[city] = data
            if data:
                latest = data[-1]
                logger.info(f"  OPIS {city}: ${latest['value']:.4f}/gal ({latest['date']})")
        return result

    def _parse_response(self, body: dict, terminal: str) -> list[dict]:
        records = []
        data = body.get("results", body.get("data", []))
        for row in data:
            dt = row.get("date", row.get("priceDate", ""))
            val = row.get("price", row.get("value"))
            if dt and val is not None:
                records.append({
                    "date": str(dt)[:10],
                    "value": float(val),
                    "terminal": terminal,
                })
        records.sort(key=lambda r: r["date"])
        return records
