"""
A4A / Argus US Jet Fuel Index scraper.

Scrapes the publicly available jet fuel spot price from:
  https://www.airlines.org/dataset/argus-us-jet-fuel-index/

This is the Argus daily simple-average jet-fuel price for
Chicago, Houston, Los Angeles, and New York — a much better
national benchmark than EIA's Gulf Coast-only spot price.

No API key required. Data is embedded as JavaScript arrays
in the page source (Highcharts).
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request

from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)

A4A_URL = "https://www.airlines.org/dataset/argus-us-jet-fuel-index/"


class A4AClient:
    """Fetches and caches A4A/Argus jet fuel index prices."""

    def __init__(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def get_prices(self, use_cache: bool = True) -> list[dict]:
        """Get A4A/Argus jet fuel prices (daily, $/gal).

        Returns [{"date": "2026-03-30", "value": 4.62}, ...]
        sorted by date ascending.
        """
        cache_path = CACHE_DIR / "a4a_jet_fuel.json"

        # Cache for 2 hours (page updates once daily)
        if use_cache and cache_path.exists():
            age = datetime.now().timestamp() - cache_path.stat().st_mtime
            if age < 7200:
                logger.info("Loading A4A data from cache")
                return json.loads(cache_path.read_text())

        try:
            records = self._scrape()
            if records:
                cache_path.write_text(json.dumps(records, indent=2))
                logger.info(f"A4A: scraped {len(records)} price records, latest: {records[-1]['date']} @ ${records[-1]['value']}")
            return records
        except Exception as e:
            logger.error(f"A4A scrape failed: {e}")
            if cache_path.exists():
                return json.loads(cache_path.read_text())
            return []

    def get_latest(self, use_cache: bool = True) -> Optional[dict]:
        """Get just the latest price point."""
        prices = self.get_prices(use_cache=use_cache)
        return prices[-1] if prices else None

    def _scrape(self) -> list[dict]:
        """Scrape price data from the A4A page."""
        req = Request(A4A_URL, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8")

        # Page has multiple Highcharts datasets embedded as JS arrays:
        #   var the_categories = ['31-Dec','02-Jan',...];   (60-day, dd-Mon format)
        #   var the_data = [2.08,2.11,...];
        # and later:
        #   var the_categories = ['03/31/25','04/01/25',...];  (1-year, MM/DD/YY format)
        #   var the_data = [2.27,2.27,...];
        # Also a mobile override block reassigns shorter arrays.
        # We use re.DOTALL to match across newlines.

        cat_pattern = re.compile(r'var\s+the_categories\s*=\s*\[(.*?)\]', re.DOTALL)
        data_pattern = re.compile(r'var\s+the_data\s*=\s*\[(.*?)\]', re.DOTALL)

        cat_matches = cat_pattern.findall(html)
        data_matches = data_pattern.findall(html)

        if not cat_matches or not data_matches:
            logger.warning("Could not find chart data in A4A page")
            return []

        # Pick the dataset with the most data points that uses dd-Mon format
        # (the 60-day chart). The 1-year chart uses MM/DD/YY format.
        # We prefer the 60-day chart for freshest data, but use 1-year if needed.
        best_idx = None
        best_len = 0

        for i, cats in enumerate(cat_matches):
            dates = re.findall(r"'([^']+)'", cats)
            if not dates:
                continue
            # Skip very short mobile override datasets (< 10 points)
            if len(dates) < 10:
                continue
            if len(dates) > best_len:
                best_len = len(dates)
                best_idx = i

        if best_idx is None or best_idx >= len(data_matches):
            best_idx = 0

        raw_cats = cat_matches[best_idx]
        raw_data = data_matches[best_idx]

        dates_raw = re.findall(r"'([^']+)'", raw_cats)
        prices_raw = re.findall(r'[\d.]+', raw_data)

        if len(dates_raw) != len(prices_raw):
            logger.warning(f"A4A date/price mismatch: {len(dates_raw)} dates, {len(prices_raw)} prices")
            min_len = min(len(dates_raw), len(prices_raw))
            dates_raw = dates_raw[:min_len]
            prices_raw = prices_raw[:min_len]

        records = []
        current_year = datetime.now().year

        for date_str, price_str in zip(dates_raw, prices_raw):
            try:
                price = float(price_str)
            except ValueError:
                continue

            # Try MM/DD/YY format first (1-year chart)
            dt = None
            try:
                dt = datetime.strptime(date_str, "%m/%d/%y")
            except ValueError:
                pass

            # Try dd-Mon format (60-day chart)
            if dt is None:
                try:
                    dt = datetime.strptime(date_str, "%d-%b")
                    # Assign year: if month is Dec and we're early in the year,
                    # it's from last year
                    if dt.month == 12 and datetime.now().month <= 3:
                        dt = dt.replace(year=current_year - 1)
                    else:
                        dt = dt.replace(year=current_year)
                except ValueError:
                    continue

            if dt:
                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "value": price,
                })

        records.sort(key=lambda r: r["date"])
        return records

    @property
    def available(self) -> bool:
        """Always available — no API key needed."""
        return True
