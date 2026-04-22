"""
risk.py — Risk Management for Crypto Futures Paper Trading.

Risk Management is absolute priority — all trading decisions pass through here.

Exports:
  calculate_atr            — Average True Range from OHLCV candles
  normalize_atr            — ATR as a fraction of price (atr_raw / price)
  compute_dynamic_sl_tp    — ATR-based SL/TP percentages (overrides config fixed values)
  get_exit_price           — LONG → best_bid, SHORT → best_ask
  update_trailing_stop     — ratcheting stop (floor for LONG, ceiling for SHORT) +
                              optional breakeven snap after N×ATR profit
  check_sl_tp              — direction-aware SL/TP/trailing-stop trigger
  should_open_trade        — gate: no open pos, not halted, cooldown ok, balance ok
  calculate_position_size  — risk-based sizing: size_usd = risk_usd / sl_pct
  is_trading_halted        — read halt flag
  update_halt_if_needed    — set halt until 00:00 UTC next day on daily-loss breach
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd


# ── ATR computation ───────────────────────────────────────────────────────────

def calculate_atr(candles: list[dict], period: int = 14) -> float | None:
    """Calculate Average True Range from OHLCV candles.

    True Range = max(high-low, |high-prev_close|, |low-prev_close|)
    ATR = rolling mean of True Range over `period` bars.

    Returns the raw ATR in price units (e.g., USD for BTC/USDT).
    Returns None when insufficient data.
    """
    if len(candles) < period + 1:
        return None
    try:
        df = pd.DataFrame(candles)
        df["high"]  = pd.to_numeric(df["high"])
        df["low"]   = pd.to_numeric(df["low"])
        df["close"] = pd.to_numeric(df["close"])
        prev_close  = df["close"].shift(1)
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"]  - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        return float(atr) if pd.notna(atr) else None
    except Exception:
        return None


def normalize_atr(
    atr_raw: float,
    last_close: float | None = None,
    cfg: dict | None = None,
) -> float:
    """Convert raw ATR (in price units) to a relative fraction of entry price.

    Formula: normalized = atr_raw / last_close
    Example: ATR=800 at BTC=80,000 → 0.01 (1% per bar move)

    This fraction is passed directly to compute_dynamic_sl_tp().
    Clamped to [0.001, 0.20] (0.1%–20% of price) to prevent extreme values.
    """
    fallback_price = (cfg or {}).get("risk_management", {}).get(
        "btc_price_fallback", 90_000.0
    )
    btc_price  = last_close if (last_close and last_close > 0) else fallback_price
    normalized = atr_raw / btc_price
    return max(0.001, min(normalized, 0.20))


def compute_dynamic_sl_tp(
    atr_normalized: float | None,
    cfg: dict,
) -> tuple[float, float]:
    """Compute ATR-based SL and TP percentages for the position.

    atr_normalized should be atr_raw / entry_price (a fraction, e.g. 0.01 for 1%).
    Multiply by the respective multiplier to get SL/TP as a fraction of entry price.

    Example: atr_pct=0.01, sl_mult=3.0 → sl_pct=0.03 (SL at -3% from entry)

    Falls back to config stop_loss_pct / take_profit_pct when ATR is unavailable.

    A minimum sl_pct floor (risk_management.min_sl_pct, default 0.005 = 0.5%)
    prevents risk-based sizing from exploding during low-volatility windows
    (size_usd = risk_usd / sl_pct). When the floor kicks in, tp_pct is scaled
    by the same factor so the configured risk:reward ratio is preserved.

    Returns (sl_pct, tp_pct) as fractions (e.g., 0.03 = 3%).
    """
    risk_cfg = cfg.get("risk_management", {})
    use_atr  = risk_cfg.get("use_atr_dynamic", True)
    base_sl  = float(risk_cfg.get("stop_loss_pct",   0.03))
    base_tp  = float(risk_cfg.get("take_profit_pct", 0.045))
    min_sl   = float(risk_cfg.get("min_sl_pct", 0.005))

    if not use_atr or atr_normalized is None:
        return max(base_sl, min_sl), base_tp

    sl_mult = float(risk_cfg.get("atr_sl_multiplier", 3.0))
    tp_mult = float(risk_cfg.get("atr_tp_multiplier", 4.5))

    sl_pct = atr_normalized * sl_mult
    tp_pct = atr_normalized * tp_mult

    # Floor sl_pct and scale tp_pct proportionally to preserve R:R.
    if sl_pct < min_sl and sl_pct > 0:
        scale  = min_sl / sl_pct
        sl_pct = min_sl
        tp_pct = tp_pct * scale

    return sl_pct, tp_pct


# ── Exit price helper ─────────────────────────────────────────────────────────

def get_exit_price(position: dict, best_bid: float, best_ask: float) -> float:
    """Get the realistic exit price for an open futures position.

    LONG  exits by selling into the bid → best_bid
    SHORT exits by buying back at the ask → best_ask
    """
    if position["side"] == "LONG":
        return best_bid
    else:  # SHORT
        return best_ask


# ── Trailing stop + Breakeven ─────────────────────────────────────────────────

def update_trailing_stop(
    position: dict,
    best_bid: float,
    best_ask: float,
    atr: float | None,    #    Raw ATR in price units (e.g., USD for BTC)
    cfg: dict,
    ws_extremums: dict | None = None,
) -> dict:
    """Update trailing stop price and enforce breakeven for futures positions.

    Futures-aware direction logic:

    LONG (stop is a price FLOOR — triggers when price drops below it):
      - Uses best_bid (or ws_extremums["highest_bid"]) as the peak price.
      - Trailing stop = peak - trail_dist, ratchets UP only.
      - Breakeven: if profit >= breakeven_atr_multiplier * atr, snap stop
        to entry_price. After activation, stop cannot drop below entry_price.

    SHORT (stop is a price CEILING — triggers when price rises above it):
      - Uses best_ask (or ws_extremums["lowest_ask"]) as the trough price.
      - Trailing stop = trough + trail_dist, ratchets DOWN only.
      - Breakeven: if profit >= breakeven_atr_multiplier * atr, snap stop
        to entry_price. After activation, stop cannot rise above entry_price.

    trail_dist = trailing_stop_atr_multiplier * atr (raw USD distance).
    Clamped to [0.5%, 20%] of entry price. Fallback: 3% of entry when ATR absent.

    Breakeven logic (config: breakeven_atr_multiplier):
      When set to 0 or absent, breakeven is disabled.
      Sets position["breakeven_activated"] = True on first trigger.
    """
    risk_cfg = cfg.get("risk_management", {})
    if not risk_cfg.get("trailing_stop_enabled", False):
        return position

    trail_mult     = float(risk_cfg.get("trailing_stop_atr_multiplier", 3.0))
    breakeven_mult = float(risk_cfg.get("breakeven_atr_multiplier", 0.0))
    entry_price    = position.get("entry_price", 0.0)
    side           = position.get("side", "LONG")

    # Trail distance in price units (e.g., USD)
    if atr is not None:
        trail_dist = trail_mult * atr
    else:
        trail_dist = entry_price * 0.03  # 3% of entry as fallback

    # Clamp trail distance to 0.5%–20% of entry price (prevents absurd values)
    trail_dist = max(entry_price * 0.005, min(trail_dist, entry_price * 0.20))

    if side == "LONG":
        # Best exit price observed (peak bid): use WS high-watermark if available
        current_price = ws_extremums["highest_bid"] if ws_extremums else best_bid
        profit = current_price - entry_price  # positive when price rose

        # ── Breakeven: snap stop to entry_price on first ATR-profit trigger ──
        if breakeven_mult > 0 and atr is not None:
            breakeven_dist = breakeven_mult * atr
            if profit >= breakeven_dist and not position.get("breakeven_activated"):
                position["breakeven_activated"] = True
                current_stop = position.get("trailing_stop_price")
                # Immediately protect entry: move stop up to entry_price
                if current_stop is None or current_stop < entry_price:
                    position["trailing_stop_price"] = round(entry_price, 2)
        # ─────────────────────────────────────────────────────────────────────

        # Regular trailing stop: only activates once profit covers full trail_dist
        if profit >= trail_dist:
            new_stop = current_price - trail_dist
            # Enforce breakeven floor: trailing stop can never drop below entry
            if position.get("breakeven_activated"):
                new_stop = max(new_stop, entry_price)
            current_stop = position.get("trailing_stop_price")
            if current_stop is None or new_stop > current_stop:
                position["trailing_stop_price"] = round(new_stop, 2)

        # Maintain floor between ticks (guards against any rounding edge cases)
        if position.get("breakeven_activated"):
            current_stop = position.get("trailing_stop_price") or 0.0
            if current_stop < entry_price:
                position["trailing_stop_price"] = round(entry_price, 2)

    else:  # SHORT
        # Best exit price observed (trough ask): use WS low-watermark if available
        current_price = ws_extremums["lowest_ask"] if ws_extremums else best_ask
        profit = entry_price - current_price  # positive when price fell

        # ── Breakeven: snap stop to entry_price on first ATR-profit trigger ──
        if breakeven_mult > 0 and atr is not None:
            breakeven_dist = breakeven_mult * atr
            if profit >= breakeven_dist and not position.get("breakeven_activated"):
                position["breakeven_activated"] = True
                current_stop = position.get("trailing_stop_price")
                # Immediately protect entry: move stop down to entry_price
                if current_stop is None or current_stop > entry_price:
                    position["trailing_stop_price"] = round(entry_price, 2)
        # ─────────────────────────────────────────────────────────────────────

        # Regular trailing stop: ceiling ratchets DOWN as price falls
        if profit >= trail_dist:
            new_stop = current_price + trail_dist
            # Enforce breakeven ceiling: stop can never rise above entry
            if position.get("breakeven_activated"):
                new_stop = min(new_stop, entry_price)
            current_stop = position.get("trailing_stop_price")
            if current_stop is None or new_stop < current_stop:
                position["trailing_stop_price"] = round(new_stop, 2)

        # Maintain ceiling between ticks
        if position.get("breakeven_activated"):
            current_stop = position.get("trailing_stop_price") or float("inf")
            if current_stop > entry_price:
                position["trailing_stop_price"] = round(entry_price, 2)

    return position


# ── SL / TP check ─────────────────────────────────────────────────────────────

def check_sl_tp(
    portfolio: dict,
    best_bid: float,
    best_ask: float,
    cfg: dict | None = None,
) -> Optional[str]:
    """Check whether the active futures position has hit Stop-Loss or Take-Profit.

    Direction-aware checks for LONG/SHORT positions.

    Priority order:
      1. Trailing stop:
         LONG  → SL if best_bid <= trailing_stop_price  (stop is a floor)
         SHORT → SL if best_ask >= trailing_stop_price  (stop is a ceiling)
      2. Fixed/dynamic TP (stored as tp_pct in position at open time):
         LONG  → TP if best_bid >= entry * (1 + tp_pct)
         SHORT → TP if best_ask <= entry * (1 - tp_pct)
      3. Fixed/dynamic SL (stored as sl_pct in position at open time):
         LONG  → SL if best_bid <= entry * (1 - sl_pct)
         SHORT → SL if best_ask >= entry * (1 + sl_pct)

    Returns "SL", "TP", or None.
    """
    position = portfolio.get("active_position")
    if position is None:
        return None

    entry_price = position.get("entry_price")
    if entry_price is None or entry_price <= 0:
        return None

    risk_cfg = (cfg or {}).get("risk_management", {})
    side     = position.get("side", "LONG")

    #    Exit price: LONG sells at bid, SHORT buys back at ask
    exit_price = best_bid if side == "LONG" else best_ask

    # 1. Trailing stop check
    trailing_stop = position.get("trailing_stop_price")
    if trailing_stop is not None:
        if side == "LONG" and exit_price <= trailing_stop:
            return "SL"
        if side == "SHORT" and exit_price >= trailing_stop:
            return "SL"

    # 2 & 3. SL/TP thresholds stored as fractions in position at open time
    sl_pct = float(position.get("sl_pct", risk_cfg.get("stop_loss_pct",   0.03)))
    tp_pct = float(position.get("tp_pct", risk_cfg.get("take_profit_pct", 0.045)))

    if side == "LONG":
        if exit_price >= entry_price * (1.0 + tp_pct):
            return "TP"
        if exit_price <= entry_price * (1.0 - sl_pct):
            return "SL"
    else:  # SHORT: profit = price falling
        if exit_price <= entry_price * (1.0 - tp_pct):
            return "TP"
        if exit_price >= entry_price * (1.0 + sl_pct):
            return "SL"

    return None


# ── Trade gate ────────────────────────────────────────────────────────────────

def should_open_trade(
    portfolio: dict,
    cfg: dict | None = None,
) -> bool:
    """Return True only if all conditions allow opening a new trade.

    Uses risk_per_trade_pct for minimum balance check,
    falls back to position_size_pct if risk_per_trade_pct is absent.

    Conditions:
    - No active position exists.
    - Trading is not halted (prop firm daily loss limit).
    - Balance is sufficient (positive after applying risk pct).
    - Cooldown after last SL has elapsed (if configured).
    """
    cfg = cfg or {}
    risk_cfg = cfg.get("risk_management", {})

    # Prefer risk_per_trade_pct; fall back to position_size_pct
    risk_pct = float(
        risk_cfg.get("risk_per_trade_pct", risk_cfg.get("position_size_pct", 0.01))
    )

    if portfolio.get("active_position") is not None:
        return False
    if is_trading_halted(portfolio):
        return False
    if portfolio.get("balance_usd", 0.0) * risk_pct <= 0:
        return False

    cooldown_sec = risk_cfg.get("cooldown_after_sl_sec")
    if cooldown_sec:
        last_sl = portfolio.get("last_sl_timestamp")
        if last_sl:
            last_sl_dt = datetime.fromisoformat(last_sl)
            if last_sl_dt.tzinfo is None:
                last_sl_dt = last_sl_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - last_sl_dt).total_seconds() < cooldown_sec:
                return False

    return True


def calculate_position_size(
    portfolio: dict,
    cfg: dict,
    entry_price: float = 0.0,
    sl_pct: float = 0.0,
) -> float:
    """Return notional position size in USD (qty * entry_price).

    Risk-based sizing (futures standard):
      risk_usd          = balance * risk_per_trade_pct
      sl_distance_price = entry_price * sl_pct
      qty               = risk_usd / sl_distance_price
      size_usd          = qty * entry_price = risk_usd / sl_pct

    Example: balance=1000, risk_pct=0.01, entry=80000, sl_pct=0.03
      → risk_usd = 10 USD, qty = 10/2400 = 0.00417 BTC, size_usd = 333 USD
      If SL hit: loss = qty * 2400 = 10 USD = 1% of balance ✓

    Hard cap: size_usd is clamped to balance * leverage * size_cap_leverage_factor
    (default 0.9) so tiny sl_pct from low-volatility ATR cannot produce a notional
    that exceeds the margin the account can actually support.

    Falls back to balance * position_size_pct when:
      - risk_per_trade_pct is not in config, OR
      - entry_price or sl_pct are not provided (legacy callers).
    """
    risk_cfg           = cfg.get("risk_management", {})
    balance            = portfolio.get("balance_usd", 0.0)
    risk_per_trade_pct = risk_cfg.get("risk_per_trade_pct")
    leverage           = float(cfg.get("exchange", {}).get("leverage", 1.0) or 1.0)
    if leverage <= 0:
        leverage = 1.0
    safety_factor      = float(risk_cfg.get("size_cap_leverage_factor", 0.9))
    max_notional       = balance * leverage * safety_factor if balance > 0 else 0.0

    if risk_per_trade_pct is not None and entry_price > 0 and sl_pct > 0:
        risk_usd = balance * float(risk_per_trade_pct)
        # size_usd = risk_usd / sl_pct  (algebraically: qty*entry = (risk/sl_dist)*entry)
        size_usd = risk_usd / float(sl_pct)
        if max_notional > 0:
            size_usd = min(size_usd, max_notional)
        return size_usd

    # Legacy fallback: fixed percentage of balance
    position_size_pct = float(risk_cfg.get("position_size_pct", 0.05))
    size_usd = balance * position_size_pct
    if max_notional > 0:
        size_usd = min(size_usd, max_notional)
    return size_usd


# ── Daily loss halt ───────────────────────────────────────────────────────────

def is_trading_halted(portfolio: dict) -> bool:
    """Return True if trading_halted_until is set and is in the future."""
    halted_until = portfolio.get("trading_halted_until")
    if halted_until is None:
        return False
    if not isinstance(halted_until, str):
        return False
    halted_until_dt = datetime.fromisoformat(halted_until)
    if halted_until_dt.tzinfo is None:
        halted_until_dt = halted_until_dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) < halted_until_dt


def update_halt_if_needed(portfolio: dict, cfg: dict) -> dict:
    """Set trading_halted_until to 00:00 UTC next day if daily_pnl breaches max loss.

    Uses initial_balance_usd (not current balance) as reference — this is
    the prop firm standard: daily loss limit is fixed at session start, not floating.

    Halt expires at 00:00 UTC next day (was now + 24h) so the trading
    day boundary aligns with the UTC calendar reset.
    """
    risk_cfg           = cfg.get("risk_management", {})
    max_daily_loss_pct = float(risk_cfg.get("max_daily_loss_pct", 0.04))
    initial_balance    = float(risk_cfg.get("initial_balance_usd", 1000.0))
    daily_pnl          = portfolio.get("daily_pnl", 0.0)
    max_daily_loss_usd = initial_balance * max_daily_loss_pct

    if daily_pnl <= -max_daily_loss_usd:
        now           = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        portfolio["trading_halted_until"] = next_midnight.isoformat()

    return portfolio
