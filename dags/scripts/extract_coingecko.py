"""
CoinGecko API extraction helpers.

Uses the free Demo API (https://api.coingecko.com/api/v3).
If COINGECKO_API_KEY is set, it is sent as x-cg-demo-key header.
Includes retry logic and rate-limit back-off (5 s between calls).
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"
UA = "crypto-dwh-airflow/1.0 (+python requests)"
CALL_DELAY = 5  # seconds between consecutive API calls


def _headers() -> dict:
    """Build request headers, including optional Demo API key."""
    h: dict = {"User-Agent": UA, "Accept": "application/json"}
    key = os.environ.get("COINGECKO_API_KEY", "").strip()
    if key:
        h["x-cg-demo-key"] = key
    return h


def _get(endpoint: str, params: Optional[dict] = None, retries: int = 3) -> Any:
    """GET with retry + rate-limit back-off."""
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=30)
            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning("Rate-limited (429). Waiting %d s …", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("Attempt %d/%d failed for %s: %s", attempt, retries, endpoint, exc)
            if attempt == retries:
                raise
            time.sleep(10 * attempt)
    return None


# ──────────────────────────────────────────────────────────────
# Public API helpers
# ──────────────────────────────────────────────────────────────

def fetch_markets(
    vs_currency: str = "usd",
    per_page: int = 50,
    page: int = 1,
) -> List[Dict[str, Any]]:
    """
    /coins/markets — top coins by market cap.
    Returns list of coin dicts with price, mcap, volume, etc.
    """
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
    }
    data = _get("/coins/markets", params)
    log.info("Fetched %d coins from /coins/markets", len(data) if data else 0)
    return data or []


def fetch_trending() -> List[Dict[str, Any]]:
    """
    /search/trending — top trending coins on CoinGecko (last 24 h).
    Returns normalized list of trending coin dicts.
    """
    data = _get("/search/trending")
    coins = []
    if data and "coins" in data:
        for item in data["coins"]:
            c = item.get("item", {})
            coins.append({
                "coin_id": c.get("id", ""),
                "name": c.get("name", ""),
                "symbol": c.get("symbol", ""),
                "market_cap_rank": c.get("market_cap_rank"),
                "score": c.get("score"),
                "price_btc": c.get("price_btc"),
            })
    log.info("Fetched %d trending coins", len(coins))
    return coins


def fetch_global() -> Dict[str, Any]:
    """
    /global — overall crypto market data.
    Returns the data dict with total_market_cap, total_volume, etc.
    """
    data = _get("/global")
    if data and "data" in data:
        log.info("Fetched global market data")
        return data["data"]
    return {}


def fetch_ohlc(
    coin_id: str,
    vs_currency: str = "usd",
    days: int = 14,
) -> List[Dict[str, Any]]:
    """
    /coins/{id}/ohlc — OHLC candle data.
    days=14 → 4-hour candles; we aggregate to daily in the loader.
    Returns list of dicts with keys: timestamp, open, high, low, close.
    """
    params = {"vs_currency": vs_currency, "days": days}
    data = _get(f"/coins/{coin_id}/ohlc", params)
    candles = []
    if data:
        for row in data:
            # CoinGecko returns [timestamp_ms, open, high, low, close]
            if isinstance(row, list) and len(row) == 5:
                candles.append({
                    "timestamp_ms": row[0],
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                })
    log.info("Fetched %d OHLC candles for %s", len(candles), coin_id)
    return candles
