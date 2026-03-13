"""
Data models for FBO locations, terminals, and supply chain mapping.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FBO:
    code: str                    # Airport/FBO code (e.g., "MIA", "TEB")
    city: str
    state: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    padd: Optional[int] = None
    region_label: Optional[str] = None
    primary_terminal: Optional[str] = None
    secondary_terminal: Optional[str] = None
    index_publisher: str = "Platts"
    index_name: str = ""
    sample_differential: float = 0.0

    @property
    def market_key(self) -> str:
        return f"{self.code}_{self.state}"


@dataclass
class Terminal:
    name: str
    city: str
    state: str
    delivery_method: str = ""     # "Rack" or "Pipeline"
    supplier: str = ""
    pricing_index: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    rack_rate: Optional[float] = None


@dataclass
class TransportRate:
    origin_terminal: str
    destination_fbo: str
    distance_miles: float
    carrier: str = ""
    rate_per_gal: float = 0.0
    min_gallons: int = 8000


@dataclass
class Contract:
    """Represents one side of a dual-supply contract for a market."""
    contract_id: str             # e.g., "south_florida_a"
    supplier: str
    index_key: str               # Key into PRICING_INDICES config
    differential_per_gal: float  # $/gal added to index price
    terminal: str = ""
    delivery_method: str = ""
    transport_cost_per_gal: float = 0.0

    def total_cost(self, index_price: float) -> float:
        return index_price + self.differential_per_gal + self.transport_cost_per_gal


@dataclass
class MarketConfig:
    """Dual-contract configuration for a market (one or more FBOs)."""
    market_name: str             # e.g., "South Florida"
    fbo_codes: list = field(default_factory=list)
    contract_a: Optional[Contract] = None
    contract_b: Optional[Contract] = None
    notes: str = ""
