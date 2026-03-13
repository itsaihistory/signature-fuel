"""
Central configuration for the Signature Fuel Pricing Intelligence System.
All configurable parameters live here - never hardcode index names, suppliers,
or pricing formulas in the core logic.
"""

import os
from pathlib import Path

# -- Paths -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REFERENCE_DIR = DATA_DIR / "reference"
CACHE_DIR = DATA_DIR / "cache"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

# -- API Keys ----------------------------------------------------------------
# EIA: register at https://www.eia.gov/opendata/register.php
EIA_API_KEY = os.environ.get("EIA_API_KEY", "55jhrW8E2ZfaBnOg4zxbe7ddfh5tMOeYzaoxolpw")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "19f554431a4c401c956eb5769073e918")

# -- EIA API v2 Endpoints ----------------------------------------------------
# The EIA client now uses the v2 faceted API directly (petroleum/pri/spt/data, etc.)
# rather than legacy series IDs. See src/api/eia_client.py for all endpoints.
#
# Available jet fuel data from EIA:
#   - Daily:  Gulf Coast jet fuel spot (EPJK) -- only daily jet fuel series available
#   - Daily:  WTI and Brent crude benchmarks
#   - Weekly: Jet fuel inventories by PADD (ending stocks)
#   - Weekly: Jet fuel production by PADD (refiner/blender net)
#   - Weekly: Jet fuel imports by PADD + U.S. exports
#   - Weekly: Jet fuel product supplied (demand proxy)
#   - Weekly: Refinery utilization (gross inputs) by PADD
#   - Monthly: Inter-PADD jet fuel movements (pipeline/tanker/barge)
#
# NOTE: NY Harbor and LA jet fuel daily spot are NOT available from EIA.
# Those require Platts/OPIS subscriptions (Tier 3).

# -- Pricing Index Definitions ------------------------------------------------
# Configurable - update when actual contract terms are confirmed
PRICING_INDICES = {
    "platts_gc_pwa": {
        "name": "Platts Gulf Coast Prior Week Average",
        "abbreviation": "GC PWA",
        "publisher": "Platts",
        "frequency": "weekly_avg",
        "region": "Gulf Coast",
    },
    "platts_gc_pda": {
        "name": "Platts Gulf Coast Prior Day",
        "abbreviation": "GC PDA",
        "publisher": "Platts",
        "frequency": "prior_day",
        "region": "Gulf Coast",
    },
    "platts_nyh_pda": {
        "name": "Platts NY Harbor Prior Day",
        "abbreviation": "NYH PDA",
        "publisher": "Platts",
        "frequency": "prior_day",
        "region": "NY Harbor",
    },
    "platts_nyh_pwa": {
        "name": "Platts NY Harbor Prior Week Average",
        "abbreviation": "NYH PWA",
        "publisher": "Platts",
        "frequency": "weekly_avg",
        "region": "NY Harbor",
    },
    "platts_rm_pda": {
        "name": "Platts Rocky Mountain Prior Day",
        "abbreviation": "RM PDA",
        "publisher": "Platts",
        "frequency": "prior_day",
        "region": "Rocky Mountain",
    },
    "platts_la_pda": {
        "name": "Platts LA Prior Day",
        "abbreviation": "LA PDA",
        "publisher": "Platts",
        "frequency": "prior_day",
        "region": "West Coast",
    },
}

# -- Default Contract Templates -----------------------------------------------
DEFAULT_CONTRACT_PAIR = {
    "contract_a": {
        "index_key": "platts_gc_pwa",
        "supplier": "TBD",
        "differential_per_gal": 0.0,
    },
    "contract_b": {
        "index_key": "platts_gc_pda",
        "supplier": "TBD",
        "differential_per_gal": 0.0,
    },
}

# -- PADD Region Mapping -----------------------------------------------------
STATE_TO_PADD = {
    # PADD 1 - East Coast
    "CT": 1, "ME": 1, "MA": 1, "NH": 1, "RI": 1, "VT": 1,  # 1A New England
    "DE": 1, "DC": 1, "MD": 1, "NJ": 1, "NY": 1, "PA": 1,  # 1B Central Atlantic
    "FL": 1, "GA": 1, "NC": 1, "SC": 1, "VA": 1, "WV": 1,  # 1C Lower Atlantic
    # PADD 2 - Midwest
    "IL": 2, "IN": 2, "IA": 2, "KS": 2, "KY": 2, "MI": 2,
    "MN": 2, "MO": 2, "NE": 2, "ND": 2, "OH": 2, "OK": 2,
    "SD": 2, "TN": 2, "WI": 2,
    # PADD 3 - Gulf Coast
    "AL": 3, "AR": 3, "LA": 3, "MS": 3, "NM": 3, "TX": 3,
    # PADD 4 - Rocky Mountain
    "CO": 4, "ID": 4, "MT": 4, "UT": 4, "WY": 4,
    # PADD 5 - West Coast
    "AK": 5, "AZ": 5, "CA": 5, "HI": 5, "NV": 5, "OR": 5, "WA": 5,
}

# -- Scheduling ---------------------------------------------------------------
PRICING_CUTOFF_HOUR_ET = 17  # 5 PM ET
WEEKLY_BRIEF_DAY = "Monday"
WEEKLY_BRIEF_HOUR_ET = 7     # 7 AM ET Monday delivery
