"""
binance_client.py — Binance Futures USDT-M authenticated client (LIVE mode).

This file is a skeleton. All methods currently raise NotImplementedError;
fill them in when paper trading proves profitable and you're ready to go live.

── How to wire this up when ready ────────────────────────────────────────────

  1. Generate API keys in Binance Futures account:
     https://www.binance.com/en/my/settings/api-management
     Enable "Enable Futures" permission. For paper→live transition, we
     recommend starting with keys restricted to a specific IP.

  2. Add these fields to your config:
        execution:
          mode: live
        endpoints:
          binance_futures:    "https://fapi.binance.com"    # stable
          binance_futures_ws: "wss://fstream.binance.com"   # legacy; migrate to
                                                            # /public, /market, /private
                                                            # per Binance notice 2026.
        exchange:
          api_key:    "<YOUR_API_KEY>"
          api_secret: "<YOUR_API_SECRET>"
          symbol:     "BTCUSDT"
          leverage:   5
          margin_mode: "isolated"    # or "cross"

  3. Implement the methods below in this file. All signed requests need:
        - X-MBX-APIKEY header
        - timestamp param (ms since epoch)
        - signature param = HMAC-SHA256 of the query string with api_secret

  4. Testnet first:
        https://testnet.binancefuture.com — separate API keys, separate URL
        endpoints.binance_futures: "https://testnet.binancefuture.com"
        endpoints.binance_futures_ws: "wss://stream.binancefuture.com"

  5. Wire this client into src/execution.py:
        In open_position(cfg["execution"]["mode"] == "live"):
            - call set_leverage() once per symbol (idempotent)
            - call place_market_order(side, quantity)
            - on fill, build position dict from Binance response
        In close_position(cfg["execution"]["mode"] == "live"):
            - call place_market_order with reduceOnly=True in opposite direction

── References ────────────────────────────────────────────────────────────────

  REST docs:       https://developers.binance.com/docs/derivatives/usds-margined-futures
  Signed request:  https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
  Error codes:     https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code
  Python SDK:      pip install binance-connector   (official; can be used instead
                   of hand-rolling this file)
"""

import hashlib
import hmac
import time
from urllib.parse import urlencode


# ── Helpers (these are the ONLY pre-implemented functions) ────────────────────

def _timestamp_ms() -> int:
    """Binance expects millisecond unix timestamps on signed requests."""
    return int(time.time() * 1000)


def _sign(query_string: str, api_secret: str) -> str:
    """HMAC-SHA256 signature of a URL-encoded query string."""
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _signed_params(params: dict, api_secret: str) -> dict:
    """Add timestamp + signature to a params dict for a signed request."""
    params = dict(params)
    params["timestamp"] = _timestamp_ms()
    qs = urlencode(params)
    params["signature"] = _sign(qs, api_secret)
    return params


# ── Account / position queries (GET, signed) ─────────────────────────────────

async def get_account_info(cfg: dict) -> dict:
    """GET /fapi/v2/account — balance, margin, open positions summary.

    Required headers: X-MBX-APIKEY
    Required params:  timestamp, signature
    Returns: {totalWalletBalance, availableBalance, positions: [...], ...}
    """
    raise NotImplementedError(
        "binance_client.get_account_info: implement signed GET /fapi/v2/account"
    )


async def get_position(cfg: dict, symbol: str) -> dict | None:
    """GET /fapi/v2/positionRisk?symbol=<SYMBOL> — single-position snapshot.

    Returns dict with keys like:
        entryPrice, positionAmt (+/-), unrealizedProfit, leverage, liquidationPrice
    Returns None if no open position for this symbol.
    """
    raise NotImplementedError(
        "binance_client.get_position: implement signed GET /fapi/v2/positionRisk"
    )


# ── Order placement (POST, signed) ────────────────────────────────────────────

async def place_market_order(
    cfg: dict,
    symbol: str,
    side: str,            # "BUY" or "SELL"
    quantity: float,
    reduce_only: bool = False,
) -> dict:
    """POST /fapi/v1/order with type=MARKET.

    Required params:
        symbol, side, type=MARKET, quantity, timestamp, signature
    Optional:
        reduceOnly=true  (pass when closing an existing position)
        newClientOrderId (useful for idempotency on retries)

    Quantity is in BASE units (BTC, not USD). Compute from notional:
        quantity = notional_usd / current_price
    Round to the symbol's quantityPrecision (fetched via /fapi/v1/exchangeInfo).

    Mapping futures-bot side → Binance side:
        LONG  open  → BUY
        LONG  close → SELL with reduceOnly=True
        SHORT open  → SELL
        SHORT close → BUY  with reduceOnly=True
    """
    raise NotImplementedError(
        "binance_client.place_market_order: implement signed POST /fapi/v1/order (type=MARKET)"
    )


async def place_limit_order(
    cfg: dict,
    symbol: str,
    side: str,
    quantity: float,
    price: float,
    time_in_force: str = "GTC",
    reduce_only: bool = False,
) -> dict:
    """POST /fapi/v1/order with type=LIMIT.

    Useful for maker-rebate TP (no slippage). Current paper-mode logic uses
    `skip_slippage=True` for Hard TP — in live mode replace that path with
    a pre-placed reduceOnly LIMIT order at the TP price.

    Required: symbol, side, type=LIMIT, quantity, price, timeInForce, timestamp, signature
    """
    raise NotImplementedError(
        "binance_client.place_limit_order: implement signed POST /fapi/v1/order (type=LIMIT)"
    )


async def cancel_order(cfg: dict, symbol: str, order_id: int) -> dict:
    """DELETE /fapi/v1/order?symbol=X&orderId=N — cancel a pending order.

    Needed when: SL/TP limit orders become stale and must be replaced, or
    when the bot is shutting down and wants to flatten all pending orders.
    """
    raise NotImplementedError(
        "binance_client.cancel_order: implement signed DELETE /fapi/v1/order"
    )


async def cancel_all_open_orders(cfg: dict, symbol: str) -> dict:
    """DELETE /fapi/v1/allOpenOrders?symbol=X — cancel everything for symbol.

    Useful on bot startup (flatten any leftovers from a previous crash).
    """
    raise NotImplementedError(
        "binance_client.cancel_all_open_orders: implement signed DELETE /fapi/v1/allOpenOrders"
    )


# ── Account configuration (POST, signed) ──────────────────────────────────────

async def set_leverage(cfg: dict, symbol: str, leverage: int) -> dict:
    """POST /fapi/v1/leverage?symbol=X&leverage=N — set per-symbol leverage.

    Idempotent. Call once at startup; Binance remembers the setting.
    Max leverage depends on notional tier — see the brackets endpoint if unsure.
    """
    raise NotImplementedError(
        "binance_client.set_leverage: implement signed POST /fapi/v1/leverage"
    )


async def set_margin_mode(cfg: dict, symbol: str, mode: str) -> dict:
    """POST /fapi/v1/marginType?symbol=X&marginType=(ISOLATED|CROSSED).

    Call once at startup. Binance returns an error if there's already an
    open position on this symbol — cancel/flatten first.
    """
    raise NotImplementedError(
        "binance_client.set_margin_mode: implement signed POST /fapi/v1/marginType"
    )


# ── Exchange metadata (GET, public) ───────────────────────────────────────────

async def get_exchange_info(cfg: dict, symbol: str | None = None) -> dict:
    """GET /fapi/v1/exchangeInfo — symbol filters, tick size, step size.

    You NEED this to round order quantity and price to the allowed precision:
        quantityPrecision, pricePrecision, LOT_SIZE filter (stepSize),
        PRICE_FILTER (tickSize), MIN_NOTIONAL filter.

    Cache the result — it changes rarely (pull once per hour at most).
    """
    raise NotImplementedError(
        "binance_client.get_exchange_info: implement public GET /fapi/v1/exchangeInfo"
    )


# ── User data stream (WS, requires listenKey) ────────────────────────────────

async def start_user_data_stream(cfg: dict) -> str:
    """POST /fapi/v1/listenKey — returns a listenKey for the user WS.

    The user data stream pushes real-time fills, margin calls, liquidation
    warnings, and position updates. Necessary for reliable live trading —
    don't poll /fapi/v2/account in a loop.

    The listenKey expires after 60 minutes; PUT /fapi/v1/listenKey every
    30 minutes to refresh.
    """
    raise NotImplementedError(
        "binance_client.start_user_data_stream: implement signed POST /fapi/v1/listenKey"
    )


async def keepalive_user_data_stream(cfg: dict) -> None:
    """PUT /fapi/v1/listenKey — refresh the listenKey (call every 30 min)."""
    raise NotImplementedError(
        "binance_client.keepalive_user_data_stream: implement signed PUT /fapi/v1/listenKey"
    )
