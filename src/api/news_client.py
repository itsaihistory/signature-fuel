"""
NewsAPI client for energy market news.

Fetches headlines relevant to jet fuel pricing: oil markets, refinery
operations, OPEC, weather disruptions, and supply chain events.

Free tier: 100 requests/day, localhost only.
Docs: https://newsapi.org/docs
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.parse import urlencode, quote

from config.settings import CACHE_DIR, NEWS_API_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://newsapi.org/v2"

# Keywords grouped by topic — searched with OR logic within groups
ENERGY_QUERIES = [
    '"jet fuel" OR "aviation fuel" OR "Jet-A"',
    'refinery outage OR refinery shutdown OR refinery maintenance',
    '"crude oil" price OR OPEC OR "oil production"',
    '"Gulf Coast" hurricane OR "Gulf Coast" storm OR "pipeline disruption"',
    '"fuel supply" OR "fuel shortage" OR "crack spread"',
]

# Single combined query for efficient API usage (1 call instead of 5)
# Covers: jet fuel, refinery ops, crude/OPEC, weather/natural disasters,
# supply chain, geopolitics, demand/airlines, regulation/SAF, logistics
DEFAULT_QUERY = (
    '"jet fuel" OR "aviation fuel" OR refinery OR "crude oil" OR OPEC '
    'OR "fuel supply" OR pipeline OR "crack spread" '
    'OR hurricane OR tornado OR "tropical storm" '
    'OR "oil sanctions" OR "Iran oil" OR "Russia oil" OR "oil embargo" '
    'OR "airline demand" OR "air travel" OR "flight demand" '
    'OR "sustainable aviation fuel" OR SAF OR "renewable fuel" OR RFS '
    'OR "Colonial Pipeline" OR "Jones Act" OR "fuel logistics"'
)


class NewsClient:
    """Fetches and caches energy market news from NewsAPI."""

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or NEWS_API_KEY
        if not self.api_key:
            logger.warning("No NewsAPI key set. Set NEWS_API_KEY env var.")

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def get_energy_headlines(
        self,
        days_back: int = 3,
        max_results: int = 20,
        use_cache: bool = True,
    ) -> list[dict]:
        """
        Fetch recent energy/fuel news headlines.
        Returns [{"title", "source", "url", "published", "description", "topic"}].
        """
        if not self.available:
            return self._load_from_cache("news_energy")

        # Check cache (1-hour TTL)
        cache_path = CACHE_DIR / "news_energy.json"
        if use_cache and cache_path.exists():
            age = datetime.now().timestamp() - cache_path.stat().st_mtime
            if age < 3600:
                return self._load_from_cache("news_energy")

        from_date = (date.today() - timedelta(days=days_back)).isoformat()

        params = urlencode({
            "q": DEFAULT_QUERY,
            "from": from_date,
            "language": "en",
            "sortBy": "relevancy",
            "pageSize": str(max_results),
            "apiKey": self.api_key,
        })
        url = f"{BASE_URL}/everything?{params}"

        try:
            req = Request(url, headers={"User-Agent": "SignatureFuel/1.0"})
            with urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read().decode())

            articles = self._parse_response(body)
            self._save_to_cache("news_energy", articles)
            return articles

        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
            return self._load_from_cache("news_energy")

    def get_topic_headlines(
        self,
        query: str,
        days_back: int = 3,
        max_results: int = 10,
    ) -> list[dict]:
        """Fetch headlines for a specific query."""
        if not self.available:
            return []

        from_date = (date.today() - timedelta(days=days_back)).isoformat()

        params = urlencode({
            "q": query,
            "from": from_date,
            "language": "en",
            "sortBy": "relevancy",
            "pageSize": str(max_results),
            "apiKey": self.api_key,
        })
        url = f"{BASE_URL}/everything?{params}"

        try:
            req = Request(url, headers={"User-Agent": "SignatureFuel/1.0"})
            with urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read().decode())
            return self._parse_response(body)
        except Exception as e:
            logger.error(f"NewsAPI error for query '{query[:40]}': {e}")
            return []

    def _parse_response(self, body: dict) -> list[dict]:
        """Parse NewsAPI response into normalized article records."""
        articles = []
        for item in body.get("articles", []):
            # Skip removed/unavailable articles
            title = item.get("title", "")
            if not title or title == "[Removed]":
                continue

            published = item.get("publishedAt", "")
            if published:
                # Convert ISO to readable format
                try:
                    dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    published = dt.strftime("%Y-%m-%d %H:%M")
                except (ValueError, TypeError):
                    published = published[:16]

            source = item.get("source", {}).get("name", "Unknown")

            articles.append({
                "title": title,
                "source": source,
                "url": item.get("url", ""),
                "published": published,
                "description": item.get("description", "") or "",
                "topic": self._classify_topic(title, item.get("description", "") or ""),
            })

        return articles

    def _classify_topic(self, title: str, description: str) -> str:
        """Assign a topic tag based on keywords in the headline.

        Order matters — more specific topics are checked first so that
        e.g. "jet fuel refinery" classifies as Jet Fuel, not Refinery.
        """
        text = (title + " " + description).lower()

        # Most specific first
        if any(w in text for w in ["jet fuel", "aviation fuel", "jet-a"]):
            return "Jet Fuel"
        if any(w in text for w in [
            "hurricane", "tropical storm", "tornado", "cyclone",
            "flood", "wildfire", "ice storm", "winter storm", "blizzard",
        ]):
            return "Weather"
        if any(w in text for w in ["refinery", "refining", "crack spread", "turnaround"]):
            return "Refinery"
        if any(w in text for w in ["opec", "production cut", "oil output", "opec+"]):
            return "OPEC"
        if any(w in text for w in [
            "iran", "russia", "sanction", "embargo", "geopolit",
            "middle east", "red sea", "strait of hormuz",
        ]):
            return "Geopolitical"
        if any(w in text for w in [
            "airline", "air travel", "flight demand", "passenger",
            "travel season", "tsa throughput",
        ]):
            return "Demand"
        if any(w in text for w in [
            "sustainable aviation", "saf", "renewable fuel",
            "rfs", "carbon credit", "epa", "blending mandate",
        ]):
            return "Regulation"
        if any(w in text for w in [
            "colonial pipeline", "pipeline", "supply disruption",
            "shortage", "jones act", "fuel logistics", "port",
        ]):
            return "Supply Chain"
        if any(w in text for w in ["crude oil", "oil price", "brent", "wti"]):
            return "Crude Oil"
        return "Energy"

    def _cache_path(self, key: str) -> Path:
        return CACHE_DIR / f"{key}.json"

    def _save_to_cache(self, key: str, data: list[dict]):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = self._cache_path(key)
        path.write_text(json.dumps(data, indent=2))

    def _load_from_cache(self, key: str) -> list[dict]:
        path = self._cache_path(key)
        if path.exists():
            logger.info(f"Loading news from cache: {path.name}")
            return json.loads(path.read_text())
        return []
