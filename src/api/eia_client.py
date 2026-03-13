"""
EIA API v2 client for fetching jet fuel spot prices, inventories,
refinery utilization, crude benchmarks, production, imports/exports,
and demand data.

Uses the v2 faceted data API: https://www.eia.gov/opendata/documentation.php
"""

import json
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.parse import urlencode, quote

from config.settings import EIA_API_KEY, CACHE_DIR

logger = logging.getLogger(__name__)

BASE_URL = "https://api.eia.gov/v2"


class EIAClient:
    """Fetches and caches EIA energy data via v2 faceted API."""

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or EIA_API_KEY
        if not self.api_key:
            logger.warning(
                "No EIA API key set. Set EIA_API_KEY env var or pass to constructor. "
                "Register at https://www.eia.gov/opendata/register.php"
            )
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Core fetch ────────────────────────────────────────────────────────────

    def _fetch(self, url: str, cache_key: str, use_cache: bool = True) -> list[dict]:
        """Fetch from EIA v2 API with caching and retry."""
        if not self.api_key:
            return self._load_from_cache(cache_key)

        cache_path = self._cache_path(cache_key)
        if use_cache and cache_path.exists():
            age = datetime.now().timestamp() - cache_path.stat().st_mtime
            if age < 3600:
                return self._load_from_cache(cache_key)

        records = self._do_fetch(url)
        if records:
            self._save_to_cache(cache_key, records)
        return records

    def _do_fetch(self, url: str, retries: int = 2) -> list[dict]:
        """HTTP GET with retry and parsing."""
        for attempt in range(retries + 1):
            try:
                req = Request(url, headers={"User-Agent": "SignatureFuel/1.0"})
                with urlopen(req, timeout=30) as resp:
                    body = json.loads(resp.read().decode())
                return self._parse_response(body)
            except Exception as e:
                if attempt < retries:
                    time.sleep(2)
                    logger.debug(f"Retry {attempt + 1} for {url[:80]}...")
                else:
                    logger.error(f"EIA API error: {e}")
                    return []

    def _parse_response(self, body: dict) -> list[dict]:
        """Parse v2 response into normalized [{"date", "value", ...}] records."""
        data = body.get("response", {}).get("data", [])
        records = []
        for row in data:
            dt = row.get("period", "")
            val = row.get("value")
            if val is None:
                continue
            record = {"date": str(dt), "value": float(val)}
            # Preserve useful metadata
            for key in ("area-name", "duoarea", "process-name", "series-description"):
                if key in row:
                    record[key] = row[key]
            records.append(record)
        records.sort(key=lambda r: r["date"])
        return records

    # ── Jet Fuel Spot Prices ─────────────────────────────────────────────────
    # EIA only publishes daily jet fuel spot for Gulf Coast.
    # NY Harbor and LA require Platts/OPIS (Tier 3).

    def get_jet_spot_prices(self, days: int = 90, use_cache: bool = True) -> list[dict]:
        """Get Gulf Coast jet fuel spot prices (daily, $/gal)."""
        start = (date.today() - timedelta(days=days)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/pri/spt/data/"
            f"?api_key={self.api_key}&frequency=daily&data[0]=value"
            f"&facets[product][]=EPJK"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, "jet_spot_gc", use_cache)

    # ── Crude Benchmarks ─────────────────────────────────────────────────────

    def get_crude_prices(self, benchmark: str = "wti", days: int = 90, use_cache: bool = True) -> list[dict]:
        """Get WTI or Brent crude prices (daily, $/bbl)."""
        product = "EPCWTI" if benchmark == "wti" else "EPCBRENT"
        start = (date.today() - timedelta(days=days)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/pri/spt/data/"
            f"?api_key={self.api_key}&frequency=daily&data[0]=value"
            f"&facets[product][]={product}"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, f"crude_{benchmark}", use_cache)

    # ── Jet Fuel Inventories (Weekly, by PADD) ───────────────────────────────

    def get_jet_inventories(self, padd: Optional[int] = None, weeks: int = 26, use_cache: bool = True) -> list[dict]:
        """
        Get jet fuel ending stocks (weekly, thousand barrels).
        If padd is None, returns all PADDs + U.S. total.
        """
        start = (date.today() - timedelta(weeks=weeks)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/stoc/wstk/data/"
            f"?api_key={self.api_key}&frequency=weekly&data[0]=value"
            f"&facets[product][]=EPJK&facets[process][]=SAE"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        if padd:
            area_code = f"R{padd}0"
            url += f"&facets[duoarea][]={area_code}"
        cache_key = f"jet_inv_padd{padd or 'all'}"
        return self._fetch(url, cache_key, use_cache)

    # ── Jet Fuel Production (Weekly, by PADD) ────────────────────────────────

    def get_jet_production(self, padd: Optional[int] = None, weeks: int = 26, use_cache: bool = True) -> list[dict]:
        """
        Get jet fuel refiner/blender net production (weekly, MBBL/day).
        """
        start = (date.today() - timedelta(weeks=weeks)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/pnp/wprodrb/data/"
            f"?api_key={self.api_key}&frequency=weekly&data[0]=value"
            f"&facets[product][]=EPJK"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        if padd:
            url += f"&facets[duoarea][]=R{padd}0"
        cache_key = f"jet_prod_padd{padd or 'all'}"
        return self._fetch(url, cache_key, use_cache)

    # ── Jet Fuel Imports & Exports (Weekly) ──────────────────────────────────

    def get_jet_imports_exports(self, weeks: int = 26, use_cache: bool = True) -> list[dict]:
        """Get jet fuel weekly imports and exports (MBBL/day)."""
        start = (date.today() - timedelta(weeks=weeks)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/move/wkly/data/"
            f"?api_key={self.api_key}&frequency=weekly&data[0]=value"
            f"&facets[product][]=EPJK"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, "jet_imports_exports", use_cache)

    # ── Refinery Utilization (Weekly, by PADD) ───────────────────────────────

    def get_refinery_utilization(self, weeks: int = 26, use_cache: bool = True) -> list[dict]:
        """Get gross inputs into refineries (weekly, MBBL/day) by PADD."""
        start = (date.today() - timedelta(weeks=weeks)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/pnp/wiup/data/"
            f"?api_key={self.api_key}&frequency=weekly&data[0]=value"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, "refinery_util", use_cache)

    # ── Jet Fuel Demand / Product Supplied (Weekly) ──────────────────────────

    def get_jet_demand(self, weeks: int = 26, use_cache: bool = True) -> list[dict]:
        """Get U.S. jet fuel product supplied — demand proxy (weekly, MBBL/day)."""
        start = (date.today() - timedelta(weeks=weeks)).isoformat()
        url = (
            f"{BASE_URL}/petroleum/cons/wpsup/data/"
            f"?api_key={self.api_key}&frequency=weekly&data[0]=value"
            f"&facets[product][]=EPJK"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, "jet_demand", use_cache)

    # ── Inter-PADD Movements (Monthly) ───────────────────────────────────────

    def get_jet_movements(self, months: int = 12, use_cache: bool = True) -> list[dict]:
        """Get jet fuel inter-PADD pipeline/tanker/barge movements (monthly, MBBL)."""
        start = (date.today() - timedelta(days=months * 30)).strftime("%Y-%m")
        url = (
            f"{BASE_URL}/petroleum/move/ptb/data/"
            f"?api_key={self.api_key}&frequency=monthly&data[0]=value"
            f"&facets[product][]=EPJK"
            f"&start={start}"
            f"&sort[0][column]=period&sort[0][direction]=asc&length=5000"
        )
        return self._fetch(url, "jet_movements", use_cache)

    # ── Convenience: fetch all data at once ──────────────────────────────────

    def fetch_all(self, use_cache: bool = True) -> dict[str, list[dict]]:
        """Fetch all jet fuel data series. Returns dict keyed by series name."""
        logger.info("Fetching all EIA jet fuel data...")
        data = {}
        data["jet_spot_gc"] = self.get_jet_spot_prices(use_cache=use_cache)
        data["wti"] = self.get_crude_prices("wti", use_cache=use_cache)
        data["brent"] = self.get_crude_prices("brent", use_cache=use_cache)
        data["jet_inventories"] = self.get_jet_inventories(use_cache=use_cache)
        data["jet_production"] = self.get_jet_production(use_cache=use_cache)
        data["jet_imports_exports"] = self.get_jet_imports_exports(use_cache=use_cache)
        data["refinery_util"] = self.get_refinery_utilization(use_cache=use_cache)
        data["jet_demand"] = self.get_jet_demand(use_cache=use_cache)
        data["jet_movements"] = self.get_jet_movements(use_cache=use_cache)

        for key, records in data.items():
            logger.info(f"  {key}: {len(records)} records")
        return data

    # ── Caching ───────────────────────────────────────────────────────────────

    def _cache_path(self, cache_key: str) -> Path:
        safe_name = cache_key.replace(".", "_").replace("/", "_").replace(" ", "_")
        return CACHE_DIR / f"eia_{safe_name}.json"

    def _save_to_cache(self, cache_key: str, data: list[dict]):
        path = self._cache_path(cache_key)
        path.write_text(json.dumps(data, indent=2))

    def _load_from_cache(self, cache_key: str) -> list[dict]:
        path = self._cache_path(cache_key)
        if path.exists():
            logger.info(f"Loading from cache: {path.name}")
            return json.loads(path.read_text())
        return []
