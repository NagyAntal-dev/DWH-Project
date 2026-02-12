from __future__ import annotations

from typing import Dict, List
import requests

BASE_URL = "https://api.coingecko.com/api/v3"
UA = "coingecko-simple-values/1.0 (+python requests)"


def get_coin_values(ids: List[str], vs: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Returns ONLY the coin -> currency -> value mapping.
    Example:
      {"bitcoin": {"usd": 65000.0, "eur": 60000.0}, "ethereum": {"usd": 3500.0}}
    """
    if not ids or not vs:
        return {}

    params = {
        "ids": ",".join(ids),
        "vs_currencies": ",".join(vs),
    }

    r = requests.get(
        f"{BASE_URL}/simple/price",
        params=params,
        timeout=20,
        headers={"User-Agent": UA},
    )

    # If rate-limited or error, fail loudly with the actual response
    if not r.ok:
        raise RuntimeError(f"CoinGecko error HTTP {r.status_code}: {r.text[:300]}")

    data = r.json()

    # Ensure consistent output: only requested ids, missing ones become empty dict
    out: Dict[str, Dict[str, float]] = {}
    for coin_id in ids:
        coin_data = data.get(coin_id, {})
        # Keep only requested currencies (and only numeric values)
        out[coin_id] = {
            cur: float(coin_data[cur])
            for cur in vs
            if cur in coin_data and isinstance(coin_data[cur], (int, float))
        }

    return out


if __name__ == "__main__":
    values = get_coin_values(["bitcoin", "ethereum"], ["usd", "eur", "huf"])
    print(values)
