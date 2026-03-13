"""Quick test script to fetch and display all jet fuel data from EIA."""
import json
import time
from urllib.request import urlopen, Request

API_KEY = "55jhrW8E2ZfaBnOg4zxbe7ddfh5tMOeYzaoxolpw"
BASE = "https://api.eia.gov/v2"


def fetch(url, retries=2):
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "SignatureFuel/1.0"})
            with urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                print(f"  [ERROR] {e}")
                return {"response": {"data": []}}


def show_jet_spot_prices():
    print("=" * 80)
    print("JET FUEL SPOT PRICES (Daily) - All regions, last 30 days")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/pri/spt/data/"
        f"?api_key={API_KEY}&frequency=daily&data[0]=value"
        f"&facets[product][]=EPJK"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=30"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        series = row.get("series-description", "")
        val = row.get("value", "N/A")
        print(f"  {row['period']:12s}  {area:25s}  ${val}/gal  {series}")


def show_jet_stocks():
    print("\n" + "=" * 80)
    print("JET FUEL INVENTORIES (Weekly) - By PADD, ending stocks")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/stoc/wstk/data/"
        f"?api_key={API_KEY}&frequency=weekly&data[0]=value"
        f"&facets[product][]=EPJK&facets[process][]=SAE"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=30"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        val = row.get("value", "N/A")
        units = row.get("units", "")
        desc = row.get("series-description", "")
        print(f"  {row['period']:12s}  {area:20s}  {val:>10} {units}  {desc}")


def show_jet_production():
    print("\n" + "=" * 80)
    print("JET FUEL PRODUCTION (Weekly) - Refiner & Blender Net Production")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/pnp/wprodrb/data/"
        f"?api_key={API_KEY}&frequency=weekly&data[0]=value"
        f"&facets[product][]=EPJK"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=20"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        val = row.get("value", "N/A")
        units = row.get("units", "")
        desc = row.get("series-description", "")
        print(f"  {row['period']:12s}  {area:20s}  {val:>10} {units}  {desc}")


def show_jet_imports_exports():
    print("\n" + "=" * 80)
    print("JET FUEL IMPORTS & EXPORTS (Weekly)")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/move/wkly/data/"
        f"?api_key={API_KEY}&frequency=weekly&data[0]=value"
        f"&facets[product][]=EPJK"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=20"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        process = row.get("process-name", row.get("process", ""))
        val = row.get("value", "N/A")
        units = row.get("units", "")
        print(f"  {row['period']:12s}  {area:20s}  {process:25s}  {val:>10} {units}")


def show_refinery_utilization():
    print("\n" + "=" * 80)
    print("REFINERY UTILIZATION (Weekly) - By PADD")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/pnp/wiup/data/"
        f"?api_key={API_KEY}&frequency=weekly&data[0]=value"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=12"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        val = row.get("value", "N/A")
        desc = row.get("series-description", "")
        print(f"  {row['period']:12s}  {area:20s}  {val:>8}  {desc}")


def show_jet_product_supplied():
    print("\n" + "=" * 80)
    print("JET FUEL PRODUCT SUPPLIED / DEMAND PROXY (Weekly)")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/cons/wpsup/data/"
        f"?api_key={API_KEY}&frequency=weekly&data[0]=value"
        f"&facets[product][]=EPJK"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=10"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        val = row.get("value", "N/A")
        units = row.get("units", "")
        desc = row.get("series-description", "")
        print(f"  {row['period']:12s}  {area:20s}  {val:>10} {units}  {desc}")


def show_crude_prices():
    print("\n" + "=" * 80)
    print("CRUDE OIL BENCHMARKS (Daily) - WTI & Brent")
    print("=" * 80)
    for product, name in [("EPCWTI", "WTI"), ("EPCBRENT", "Brent")]:
        url = (
            f"{BASE}/petroleum/pri/spt/data/"
            f"?api_key={API_KEY}&frequency=daily&data[0]=value"
            f"&facets[product][]={product}"
            f"&sort[0][column]=period&sort[0][direction]=desc&length=5"
        )
        data = fetch(url)
        print(f"\n  {name}:")
        for row in data["response"]["data"]:
            val = row.get("value", "N/A")
            print(f"    {row['period']:12s}  ${val}/bbl")


def show_inter_padd_movements():
    print("\n" + "=" * 80)
    print("JET FUEL INTER-PADD MOVEMENTS (Monthly - Pipeline/Tanker/Barge)")
    print("=" * 80)
    url = (
        f"{BASE}/petroleum/move/ptb/data/"
        f"?api_key={API_KEY}&frequency=monthly&data[0]=value"
        f"&facets[product][]=EPJK"
        f"&sort[0][column]=period&sort[0][direction]=desc&length=20"
    )
    data = fetch(url)
    for row in data["response"]["data"]:
        area = row.get("area-name", row.get("duoarea", ""))
        process = row.get("process-name", row.get("process", ""))
        val = row.get("value", "N/A")
        units = row.get("units", "")
        print(f"  {row['period']:10s}  {area:30s}  {process:30s}  {val:>10} {units}")


if __name__ == "__main__":
    show_jet_spot_prices()
    show_crude_prices()
    show_jet_stocks()
    show_jet_production()
    show_jet_imports_exports()
    show_refinery_utilization()
    show_jet_product_supplied()
    show_inter_padd_movements()
    print("\n\nDone - all available EIA jet fuel data series tested.")
