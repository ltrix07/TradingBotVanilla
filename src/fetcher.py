"""
fetcher.py — Async data-fetching module for Binance Futures USDT-M.

All public functions are async (httpx.AsyncClient). Every REST request uses a
strict 5-second timeout.

Exposes:
  REST:
    fetch_binance_futures_klines_async  — OHLCV candles from /fapi/v1/klines
    fetch_binance_futures_book_async    — order-book snapshot with depth metrics
    fetch_funding_rate_async            — current funding rate for a symbol
  WebSocket:
    BinanceFuturesTradesFeed — live aggTrade → candle builder (wss://fstream)
    BinanceFuturesBookFeed   — live depth20@100ms order-book feed

Notes on URLs (2026-04 status):
  - REST base: https://fapi.binance.com (stable)
  - WS base:   wss://fstream.binance.com (legacy; Binance is migrating to
    /public|/market|/private paths, but legacy URLs remain during transition).
    When migrating to live trading, consider updating to:
      wss://fstream.binance.com/market/ws/<stream>
    Here we use the legacy form for maximum compatibility.
"""

import asyncio
import json
import logging
from typing import Optional

import httpx

_log = logging.getLogger(__name__)


# ── REST helpers ──────────────────────────────────────────────────────────────

def _rest_base(cfg: dict) -> str:
    """Return the Binance Futures REST base URL from config, with a safe default."""
    return cfg.get("endpoints", {}).get(
        "binance_futures", "https://fapi.binance.com"
    ).rstrip("/")


def _symbol(cfg: dict) -> str:
    """Return the trading symbol from config. Defaults to BTCUSDT."""
    return cfg.get("exchange", {}).get("symbol", "BTCUSDT").upper()


# ── REST: Klines (OHLCV candles) ─────────────────────────────────────────────

async def fetch_binance_futures_klines_async(cfg: dict) -> list[dict]:
    """Fetch the most recent OHLCV candles for the configured symbol.

    Endpoint: GET /fapi/v1/klines
    Response format is identical to Binance Spot klines (12-element arrays),
    so the parser below matches the structure used previously.
    """
    base_url = _rest_base(cfg)
    url = f"{base_url}/fapi/v1/klines"

    tf = cfg.get("strategy", {}).get("timeframe", "1m")
    limit = int(cfg.get("trading", {}).get("binance_ws_candle_history", 300))
    params = {"symbol": _symbol(cfg), "interval": tf, "limit": limit}

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

    return [
        {
            "timestamp": int(e[0]),
            "open":      float(e[1]),
            "high":      float(e[2]),
            "low":       float(e[3]),
            "close":     float(e[4]),
            "volume":    float(e[5]),
        }
        for e in response.json()
    ]


# ── REST: Order book snapshot ─────────────────────────────────────────────────

async def fetch_binance_futures_book_async(cfg: dict) -> dict:
    """Fetch the current order-book snapshot with full depth metrics.

    Endpoint: GET /fapi/v1/depth
    Returns the same dict shape as BinanceFuturesBookFeed.get_latest():
        best_ask, best_bid              — top-of-book prices
        ask_volume, bid_volume          — summed size over top depth_levels
        book_imbalance                  — bid_volume / (bid+ask), in [0, 1]
        top_asks, top_bids              — list[{price, size}], sorted by price
        symbol                          — e.g. "BTCUSDT"
    """
    base_url = _rest_base(cfg)
    url = f"{base_url}/fapi/v1/depth"

    depth_levels = cfg.get("strategy", {}).get("order_book", {}).get("depth_levels", 5)
    # Binance accepts limits 5/10/20/50/100/500/1000; clamp to a valid tier.
    valid_limits = (5, 10, 20, 50, 100, 500, 1000)
    api_limit = next((v for v in valid_limits if v >= depth_levels), 20)

    params = {"symbol": _symbol(cfg), "limit": api_limit}

    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()

    data = response.json()
    # Binance depth response: {"lastUpdateId":..., "bids":[["p","q"],...], "asks":[["p","q"],...]}
    asks_raw = data.get("asks", [])
    bids_raw = data.get("bids", [])

    if not asks_raw:
        raise ValueError(f"Empty asks side from {url}")
    if not bids_raw:
        raise ValueError(f"Empty bids side from {url}")

    # Binance already returns asks ascending, bids descending — but sort to be safe.
    asks_sorted = sorted(
        [{"price": float(a[0]), "size": float(a[1])} for a in asks_raw],
        key=lambda x: x["price"],
    )
    bids_sorted = sorted(
        [{"price": float(b[0]), "size": float(b[1])} for b in bids_raw],
        key=lambda x: x["price"],
        reverse=True,
    )

    top_asks = asks_sorted[:depth_levels]
    top_bids = bids_sorted[:depth_levels]
    ask_volume = sum(a["size"] for a in top_asks)
    bid_volume = sum(b["size"] for b in top_bids)
    total = ask_volume + bid_volume
    book_imbalance = bid_volume / total if total > 0 else 0.5

    return {
        "best_ask":       top_asks[0]["price"],
        "best_bid":       top_bids[0]["price"],
        "symbol":         _symbol(cfg),
        "ask_volume":     ask_volume,
        "bid_volume":     bid_volume,
        "book_imbalance": book_imbalance,
        "top_asks":       top_asks,
        "top_bids":       top_bids,
    }


# ── REST: Funding rate ────────────────────────────────────────────────────────

async def fetch_funding_rate_async(cfg: dict) -> Optional[dict]:
    """Fetch the current funding rate for the configured symbol.

    Endpoint: GET /fapi/v1/premiumIndex
    Returns dict with:
        funding_rate       — float, e.g. 0.0001 = 0.01% per 8h
        funding_rate_bps   — funding_rate * 10_000
        mark_price         — current mark price
        next_funding_time  — ms timestamp of the next funding event
    Returns None on fetch failure (caller should treat absence as "unknown" / skip guard).
    """
    base_url = _rest_base(cfg)
    url = f"{base_url}/fapi/v1/premiumIndex"
    params = {"symbol": _symbol(cfg)}

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
        data = response.json()
    except Exception as exc:
        _log.warning("Funding rate fetch failed: %s", exc)
        return None

    try:
        rate = float(data.get("lastFundingRate", 0.0))
        return {
            "funding_rate":      rate,
            "funding_rate_bps":  rate * 10_000.0,
            "mark_price":        float(data.get("markPrice", 0.0)),
            "next_funding_time": int(data.get("nextFundingTime", 0)),
        }
    except (ValueError, TypeError) as exc:
        _log.warning("Funding rate parse failed: %s", exc)
        return None


# ── WS base ───────────────────────────────────────────────────────────────────

def _ws_base(cfg: dict) -> str:
    """Return the Binance Futures WebSocket base URL (no trailing slash)."""
    return cfg.get("endpoints", {}).get(
        "binance_futures_ws", "wss://fstream.binance.com"
    ).rstrip("/")


# ── WS: aggTrade → candles feed ───────────────────────────────────────────────

class BinanceFuturesTradesFeed:
    """WebSocket feed that builds live OHLCV candles from Binance Futures aggTrade events.

    Connects to wss://fstream.binance.com/ws/<symbol>@aggTrade and assembles
    candles on-the-fly. The last candle is always "open" (not yet closed) and
    updates with every incoming tick.

    Usage mirrors the consumer pattern already used in main.py:
        feed = BinanceFuturesTradesFeed()
        await feed.start(cfg)
        if feed.is_ready(): candles = feed.get_candles()
    """

    _RECONNECT_DELAY = 3
    _STALE_THRESHOLD_SEC = 120  # 2 min without trades → consider feed stale

    def __init__(self) -> None:
        self._symbol: str = "BTCUSDT"
        self._ws_url: str = ""
        self._timeframe_sec: int = 60
        self._max_history: int = 60
        self._candles: list[dict] = []
        self._current_candle: Optional[dict] = None
        self._last_price: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._tick_event: asyncio.Event = asyncio.Event()
        # Diagnostics: track WS health
        self._trade_count: int = 0
        self._candle_close_count: int = 0
        self._last_trade_time: float = 0.0  # monotonic timestamp of last received trade
        self._ws_connected: bool = False
        self._cfg: dict = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self, cfg: dict) -> None:
        """Start the background WebSocket listener.

        Bootstraps history from REST so MACD has meaningful values from cycle 1.
        """
        self._symbol = _symbol(cfg)
        self._ws_url = f"{_ws_base(cfg)}/ws/{self._symbol.lower()}@aggTrade"

        trading = cfg.get("trading", {})
        self._max_history = int(trading.get("binance_ws_candle_history", 60))
        tf_str = cfg.get("strategy", {}).get("timeframe", "1m")
        self._timeframe_sec = self._parse_timeframe(tf_str)

        self._running = True
        self._candles.clear()
        self._current_candle = None
        self._last_price = None
        self._tick_event.clear()

        self._cfg = cfg
        self._trade_count = 0
        self._candle_close_count = 0
        self._last_trade_time = 0.0
        self._ws_connected = False

        self._cfg = cfg
        self._trade_count = 0
        self._candle_close_count = 0
        self._last_trade_time = 0.0
        self._ws_connected = False

        await self._bootstrap_from_rest(cfg)
        self._task = asyncio.create_task(self._listener_loop())

    async def _bootstrap_from_rest(self, cfg: dict) -> None:
        """Pre-load historical candles from Binance Futures REST."""
        try:
            rest_candles = await fetch_binance_futures_klines_async(cfg)
            if not rest_candles:
                return
            # All except last are completed; last may still be open.
            self._candles = rest_candles[:-1]
            self._current_candle = rest_candles[-1]
            self._last_price = float(rest_candles[-1]["close"])
            if len(self._candles) > self._max_history:
                self._candles = self._candles[-self._max_history:]
            _log.info(
                "BinanceFuturesTradesFeed bootstrapped with %d REST candles",
                len(self._candles),
            )
        except Exception as exc:
            _log.warning("BinanceFuturesTradesFeed REST bootstrap failed: %s", exc)

    async def stop(self) -> None:
        """Cancel the background listener."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def is_ready(self) -> bool:
        """True when enough candles are available for MACD/strategy."""
        return len(self._candles) >= 20

    def get_candles(self) -> list[dict]:
        """Return completed candles + current open candle.

        The returned list is compatible with fetch_binance_futures_klines_async().
        """
        result = list(self._candles)
        if self._current_candle is not None:
            result.append(dict(self._current_candle))
        return result

    def get_last_price(self) -> Optional[float]:
        """Return the last trade price received from the WebSocket."""
        return self._last_price

    def get_diagnostics(self) -> dict:
        """Return WS feed health diagnostics for status line / debugging."""
        import time
        stale_sec = (
            time.monotonic() - self._last_trade_time
            if self._last_trade_time > 0 else -1
        )
        return {
            "ws_connected": self._ws_connected,
            "trade_count": self._trade_count,
            "candle_closes": self._candle_close_count,
            "stale_sec": round(stale_sec, 1),
        }

    def is_stale(self) -> bool:
        """True when the WS feed hasn't received a trade for too long."""
        import time
        if self._last_trade_time <= 0:
            return True  # never received a trade
        return (time.monotonic() - self._last_trade_time) > self._STALE_THRESHOLD_SEC

    async def refresh_from_rest(self) -> bool:
        """Re-fetch candles from REST when WS feed is stale. Returns True on success."""
        try:
            rest_candles = await fetch_binance_futures_klines_async(self._cfg)
            if not rest_candles:
                return False
            self._candles = rest_candles[:-1]
            self._current_candle = rest_candles[-1]
            self._last_price = float(rest_candles[-1]["close"])
            if len(self._candles) > self._max_history:
                self._candles = self._candles[-self._max_history:]
            _log.info(
                "TradesFeed refreshed from REST (%d candles) — WS stale for %.0fs",
                len(self._candles),
                self.get_diagnostics()["stale_sec"],
            )
            return True
        except Exception as exc:
            _log.warning("TradesFeed REST refresh failed: %s", exc)
            return False

    async def wait_for_tick(self, timeout: Optional[float] = None) -> bool:
        """Block until the next aggTrade tick. Returns True on tick, False on timeout."""
        self._tick_event.clear()
        try:
            await asyncio.wait_for(self._tick_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_timeframe(tf: str) -> int:
        """Convert '1s'/'1m'/'5m'/'1h' to seconds."""
        tf = tf.strip().lower()
        if tf.endswith("s"):
            return max(1, int(tf[:-1]))
        if tf.endswith("m"):
            return int(tf[:-1]) * 60
        if tf.endswith("h"):
            return int(tf[:-1]) * 3600
        return 60

    def _process_trade(self, price: float, qty: float, timestamp_ms: int) -> None:
        """Integrate a single aggTrade event into the candle builder."""
        import time
        self._last_price = price
        self._trade_count += 1
        self._last_trade_time = time.monotonic()

        candle_start_ms = (
            (timestamp_ms // (self._timeframe_sec * 1000))
            * (self._timeframe_sec * 1000)
        )

        if self._current_candle is None or self._current_candle["timestamp"] != candle_start_ms:
            # Close the previous candle, start a new one.
            if self._current_candle is not None:
                self._candles.append(self._current_candle)
                self._candle_close_count += 1
                if len(self._candles) > self._max_history:
                    self._candles = self._candles[-self._max_history:]

            self._current_candle = {
                "timestamp": candle_start_ms,
                "open":      price,
                "high":      price,
                "low":       price,
                "close":     price,
                "volume":    qty,
            }
            if self._candle_close_count > 0 and self._candle_close_count % 10 == 0:
                _log.info(
                    "TradesFeed: %d candles closed, %d trades processed, %d candles in history",
                    self._candle_close_count, self._trade_count, len(self._candles),
                )
        else:
            c = self._current_candle
            c["high"] = max(c["high"], price)
            c["low"]  = min(c["low"],  price)
            c["close"] = price
            c["volume"] += qty

        self._tick_event.set()

    async def _listener_loop(self) -> None:
        """Connect, receive aggTrade frames, reconnect on error."""
        import websockets  # deferred — guarded by _WS_AVAILABLE in main.py

        while self._running:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    self._ws_connected = True
                    _log.info("BinanceFuturesTradesFeed connected to %s", self._ws_url)
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            _log.debug("TradesFeed: no message in 30s, still waiting…")
                            continue

                        try:
                            msg = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        # aggTrade event:
                        # {"e":"aggTrade","E":ts,"s":"BTCUSDT","p":"price","q":"qty","T":trade_ts,...}
                        if msg.get("e") != "aggTrade":
                            continue

                        try:
                            price = float(msg["p"])
                            qty   = float(msg["q"])
                            trade_ts = int(msg["T"])
                        except (KeyError, ValueError, TypeError):
                            continue

                        self._process_trade(price, qty, trade_ts)

                    self._ws_connected = False

            except asyncio.CancelledError:
                self._ws_connected = False
                return
            except Exception as exc:
                self._ws_connected = False
                _log.warning(
                    "BinanceFuturesTradesFeed disconnected (%s) — reconnecting in %ds",
                    exc, self._RECONNECT_DELAY,
                )
                if self._running:
                    await asyncio.sleep(self._RECONNECT_DELAY)


# ── WS: Partial-book depth feed ──────────────────────────────────────────────

class BinanceFuturesBookFeed:
    """WebSocket feed for Binance Futures partial-book depth snapshots.

    Connects to wss://fstream.binance.com/ws/<symbol>@depth20@100ms which pushes
    a full top-20 snapshot every 100ms (no diff handling required — each frame
    replaces the previous state completely).

    Exposes the same contract as the Polymarket book feed did:
        start(cfg)
        stop()
        get_latest() → dict | None
        get_and_reset_extremums() → dict

    Extremums semantics for futures (LONG sells at bid, SHORT buys back at ask):
        highest_bid — peak best_bid, for LONG trailing stop ratchet
        lowest_bid  — trough best_bid, for LONG hard SL check
        lowest_ask  — trough best_ask, for SHORT trailing stop ratchet
        highest_ask — peak best_ask, for SHORT hard SL check
    """

    _RECONNECT_DELAY = 3

    def __init__(self) -> None:
        self._symbol: str = "BTCUSDT"
        self._ws_url: str = ""
        self._cfg: dict = {}
        self._state: Optional[dict] = None
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False
        # Extremums accumulated between polls
        self._highest_bid: float = 0.0
        self._lowest_bid:  float = float("inf")
        self._lowest_ask:  float = float("inf")
        self._highest_ask: float = 0.0

    # ── Public API ────────────────────────────────────────────────────────────

    async def start(self, cfg: dict) -> None:
        """Connect and start the background listener."""
        self._symbol = _symbol(cfg)
        # Partial-book stream: @depth20@100ms pushes top-20 bids+asks every 100ms
        self._ws_url = f"{_ws_base(cfg)}/ws/{self._symbol.lower()}@depth20@100ms"
        self._cfg = cfg
        self._state = None
        self._running = True
        self._highest_bid = 0.0
        self._lowest_bid  = float("inf")
        self._lowest_ask  = float("inf")
        self._highest_ask = 0.0
        self._task = asyncio.create_task(self._listener_loop())

    async def stop(self) -> None:
        """Cancel the background listener."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def get_latest(self) -> Optional[dict]:
        """Return a shallow copy of the current book state, or None if not ready."""
        return dict(self._state) if self._state is not None else None

    def get_and_reset_extremums(self) -> dict:
        """Return accumulated bid/ask extremums, then reset to the current best prices.

        Returned keys (all floats):
            highest_bid, lowest_bid, lowest_ask, highest_ask
        """
        result = {
            "highest_bid": self._highest_bid,
            "lowest_bid":  self._lowest_bid if self._lowest_bid != float("inf") else 0.0,
            "lowest_ask":  self._lowest_ask if self._lowest_ask != float("inf") else 0.0,
            "highest_ask": self._highest_ask,
        }
        if self._state is not None:
            self._highest_bid = self._state["best_bid"]
            self._lowest_bid  = self._state["best_bid"]
            self._lowest_ask  = self._state["best_ask"]
            self._highest_ask = self._state["best_ask"]
        else:
            self._highest_bid = 0.0
            self._lowest_bid  = float("inf")
            self._lowest_ask  = float("inf")
            self._highest_ask = 0.0
        return result

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _depth_levels(self) -> int:
        return (
            self._cfg.get("strategy", {})
            .get("order_book", {})
            .get("depth_levels", 5)
        )

    def _apply_snapshot(self, data: dict) -> None:
        """Replace the state dict from a full depth20 frame."""
        # Partial-book frame format (futures):
        # {"e":"depthUpdate","E":ts,"T":ts,"s":"BTCUSDT","U":...,"u":...,"pu":...,
        #  "b":[["price","qty"],...], "a":[["price","qty"],...]}
        asks_raw = data.get("a") or data.get("asks") or []
        bids_raw = data.get("b") or data.get("bids") or []
        if not asks_raw or not bids_raw:
            return

        depth = self._depth_levels()
        asks_sorted = sorted(
            [{"price": float(a[0]), "size": float(a[1])} for a in asks_raw if float(a[1]) > 0],
            key=lambda x: x["price"],
        )
        bids_sorted = sorted(
            [{"price": float(b[0]), "size": float(b[1])} for b in bids_raw if float(b[1]) > 0],
            key=lambda x: x["price"],
            reverse=True,
        )
        if not asks_sorted or not bids_sorted:
            return

        top_asks = asks_sorted[:depth]
        top_bids = bids_sorted[:depth]
        ask_volume = sum(a["size"] for a in top_asks)
        bid_volume = sum(b["size"] for b in top_bids)
        total = ask_volume + bid_volume
        book_imbalance = bid_volume / total if total > 0 else 0.5

        self._state = {
            "best_ask":       top_asks[0]["price"],
            "best_bid":       top_bids[0]["price"],
            "symbol":         self._symbol,
            "ask_volume":     ask_volume,
            "bid_volume":     bid_volume,
            "book_imbalance": book_imbalance,
            "top_asks":       top_asks,
            "top_bids":       top_bids,
        }

        # Update extremums
        self._highest_bid = max(self._highest_bid, self._state["best_bid"])
        self._lowest_bid  = min(self._lowest_bid,  self._state["best_bid"])
        self._lowest_ask  = min(self._lowest_ask,  self._state["best_ask"])
        self._highest_ask = max(self._highest_ask, self._state["best_ask"])

    async def _listener_loop(self) -> None:
        """Connect, receive depth frames, reconnect on error."""
        import websockets  # deferred — guarded by _WS_AVAILABLE in main.py

        while self._running:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    _log.info("BinanceFuturesBookFeed connected to %s", self._ws_url)
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            msg = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        if not isinstance(msg, dict):
                            continue

                        # depth20@100ms emits "depthUpdate" frames that are actually
                        # full top-20 snapshots (not diff) per the partial-book stream spec.
                        self._apply_snapshot(msg)

            except asyncio.CancelledError:
                return
            except Exception as exc:
                _log.warning(
                    "BinanceFuturesBookFeed disconnected (%s) — reconnecting in %ds",
                    exc, self._RECONNECT_DELAY,
                )
                if self._running:
                    await asyncio.sleep(self._RECONNECT_DELAY)
