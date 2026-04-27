#!/usr/bin/env python3
"""
klines_backtest.py — Multi-Strategy Backtest on Binance Klines (1h/4h).

Tests 4 strategies on historical candlestick data:
  A) Mean Reversion     — Bollinger Bands: enter at -2σ, exit at mean
  B) Volatility Breakout — BB squeeze → expansion breakout
  C) Trend Pullback     — EMA trend (4h) + RSI pullback (1h)
  D) Liquidity Sweep    — False breakout of recent high/low (SMC-inspired)

Data source: Binance public klines API (no key needed).
Usage:
  python klines_backtest.py --symbol BTCUSDT --months 6
  python klines_backtest.py --symbol ETHUSDT --months 12 --timeframe 4h
"""

import argparse
import json
import logging
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Data structures
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Candle:
    ts: int          # open time ms
    o: float
    h: float
    l: float
    c: float
    vol: float
    buy_vol: float   # taker buy volume

@dataclass
class Trade:
    entry_time: int
    exit_time: int
    side: str        # LONG / SHORT
    entry_px: float
    exit_px: float
    sl: float
    tp: float
    exit_reason: str  # TP / SL / TIMEOUT / SIGNAL
    pnl_pct: float

INITIAL_BALANCE = 1000.0   # Default starting balance for $ calculations


@dataclass
class StrategyResult:
    name: str
    config: dict
    trades: List[Trade] = field(default_factory=list)

    @property
    def n_trades(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.pnl_pct > 0)
        return wins / len(self.trades) * 100

    @property
    def total_pnl(self) -> float:
        """Simple sum of trade PnL percentages (no compounding)."""
        return sum(t.pnl_pct for t in self.trades)

    # ── Mode 1: Monthly reset compound ──────────────────────────────
    # Inside each month: compound (balance grows trade-by-trade).
    # At month end: take profit, reset balance to initial.
    # Total = sum of monthly % gains. Like a monthly "salary".

    def monthly_compound(self, initial_balance: float = INITIAL_BALANCE) -> dict:
        """Compound within each month, reset at month boundary.

        Returns:
            {
                "months": [{"month": "2025-11", "pnl_pct": 3.5, "pnl_usd": 35.0, "trades": 8}, ...],
                "total_pnl_pct": 19.2,
                "total_pnl_usd": 192.0,
                "avg_monthly_pct": 3.2,
                "best_month_pct": 7.1,
                "worst_month_pct": -2.3,
                "profitable_months": 5,
                "total_months": 6,
            }
        """
        if not self.trades:
            return {"months": [], "total_pnl_pct": 0, "total_pnl_usd": 0,
                    "avg_monthly_pct": 0, "best_month_pct": 0, "worst_month_pct": 0,
                    "profitable_months": 0, "total_months": 0}

        # Group trades by month
        from collections import OrderedDict
        months: dict[str, list] = OrderedDict()
        for t in self.trades:
            dt = datetime.fromtimestamp(t.entry_time / 1000, tz=timezone.utc)
            key = dt.strftime("%Y-%m")
            months.setdefault(key, []).append(t)

        month_results = []
        total_pct = 0.0
        total_usd = 0.0

        for month_key, month_trades in months.items():
            # Compound within the month
            balance = 1.0
            for t in month_trades:
                balance *= (1 + t.pnl_pct / 100)
            month_pct = (balance - 1) * 100
            month_usd = initial_balance * month_pct / 100

            month_results.append({
                "month": month_key,
                "pnl_pct": round(month_pct, 2),
                "pnl_usd": round(month_usd, 2),
                "trades": len(month_trades),
            })
            total_pct += month_pct
            total_usd += month_usd

        pcts = [m["pnl_pct"] for m in month_results]
        return {
            "months": month_results,
            "total_pnl_pct": round(total_pct, 2),
            "total_pnl_usd": round(total_usd, 2),
            "avg_monthly_pct": round(total_pct / len(month_results), 2) if month_results else 0,
            "best_month_pct": round(max(pcts), 2) if pcts else 0,
            "worst_month_pct": round(min(pcts), 2) if pcts else 0,
            "profitable_months": sum(1 for p in pcts if p > 0),
            "total_months": len(month_results),
        }

    # ── Mode 2: Full compound (total reinvest) ─────────────────────
    # Balance grows continuously, never reset.

    def full_compound(self, initial_balance: float = INITIAL_BALANCE) -> dict:
        """Full compound — total reinvestment, balance never resets.

        Returns:
            {
                "final_balance": 1245.0,
                "total_pnl_pct": 24.5,
                "total_pnl_usd": 245.0,
                "max_balance": 1300.0,
                "min_balance": 920.0,
                "max_drawdown_pct": 8.5,
                "equity_curve": [(ts, balance), ...],  # for plotting
            }
        """
        if not self.trades:
            return {"final_balance": initial_balance, "total_pnl_pct": 0,
                    "total_pnl_usd": 0, "max_balance": initial_balance,
                    "min_balance": initial_balance, "max_drawdown_pct": 0,
                    "equity_curve": []}

        balance = initial_balance
        peak = initial_balance
        max_dd = 0.0
        max_bal = initial_balance
        min_bal = initial_balance
        curve = [(self.trades[0].entry_time, initial_balance)]

        for t in self.trades:
            balance *= (1 + t.pnl_pct / 100)
            peak = max(peak, balance)
            dd = (peak - balance) / peak * 100
            max_dd = max(max_dd, dd)
            max_bal = max(max_bal, balance)
            min_bal = min(min_bal, balance)
            curve.append((t.exit_time, round(balance, 2)))

        return {
            "final_balance": round(balance, 2),
            "total_pnl_pct": round((balance / initial_balance - 1) * 100, 2),
            "total_pnl_usd": round(balance - initial_balance, 2),
            "max_balance": round(max_bal, 2),
            "min_balance": round(min_bal, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "equity_curve": curve,
        }

    @property
    def compound_pnl(self) -> float:
        """Shortcut: full compound PnL %."""
        return self.full_compound()["total_pnl_pct"]

    @property
    def avg_pnl(self) -> float:
        if not self.trades:
            return 0.0
        return self.total_pnl / len(self.trades)

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(t.pnl_pct for t in self.trades if t.pnl_pct > 0)
        gross_loss = abs(sum(t.pnl_pct for t in self.trades if t.pnl_pct < 0))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    @property
    def max_drawdown(self) -> float:
        """Max drawdown based on compound equity curve."""
        return self.full_compound()["max_drawdown_pct"]

    @property
    def max_consecutive_losses(self) -> int:
        if not self.trades:
            return 0
        max_streak = 0
        current = 0
        for t in self.trades:
            if t.pnl_pct < 0:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak

    @property
    def sharpe_approx(self) -> float:
        """Approximate Sharpe ratio (no risk-free rate)."""
        if len(self.trades) < 2:
            return 0.0
        pnls = [t.pnl_pct for t in self.trades]
        mean = np.mean(pnls)
        std = np.std(pnls, ddof=1)
        if std == 0:
            return 0.0
        return mean / std * np.sqrt(len(pnls))

# ═══════════════════════════════════════════════════════════════════════════════
# Data fetching
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> List[Candle]:
    """Fetch klines from Binance Futures API (public, no key needed)."""
    candles = []
    current = start_ms

    while current < end_ms:
        url = (
            f"https://fapi.binance.com/fapi/v1/klines"
            f"?symbol={symbol}&interval={interval}"
            f"&startTime={current}&endTime={end_ms}&limit=1500"
        )

        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode())
                break
            except Exception as e:
                if attempt == 4:
                    log.error(f"Failed to fetch klines after 5 attempts: {e}")
                    return candles
                log.warning(f"Retry {attempt+1}/5: {e}")
                time.sleep(2 ** attempt)

        if not data:
            break

        for k in data:
            candles.append(Candle(
                ts=int(k[0]),
                o=float(k[1]),
                h=float(k[2]),
                l=float(k[3]),
                c=float(k[4]),
                vol=float(k[5]),
                buy_vol=float(k[9]),
            ))

        current = int(data[-1][0]) + 1

        if len(data) < 1500:
            break

        time.sleep(0.15)  # rate limit courtesy

    log.info(f"Fetched {len(candles)} candles ({interval}) for {symbol}")
    return candles


# ═══════════════════════════════════════════════════════════════════════════════
# Indicators
# ═══════════════════════════════════════════════════════════════════════════════

def calc_sma(data: np.ndarray, period: int) -> np.ndarray:
    """Simple Moving Average."""
    out = np.full_like(data, np.nan)
    if len(data) < period:
        return out
    cumsum = np.cumsum(data)
    out[period-1:] = (cumsum[period-1:] - np.concatenate([[0], cumsum[:-period]])) / period
    return out

def calc_ema(data: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average."""
    out = np.full_like(data, np.nan)
    if len(data) < period:
        return out
    alpha = 2.0 / (period + 1)
    out[period-1] = np.mean(data[:period])
    for i in range(period, len(data)):
        out[i] = alpha * data[i] + (1 - alpha) * out[i-1]
    return out

def calc_rsi(data: np.ndarray, period: int = 14) -> np.ndarray:
    """Relative Strength Index."""
    out = np.full_like(data, np.nan)
    if len(data) < period + 1:
        return out
    deltas = np.diff(data)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            out[i+1] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i+1] = 100.0 - 100.0 / (1.0 + rs)

    return out

def calc_bollinger(closes: np.ndarray, period: int = 20, num_std: float = 2.0):
    """Bollinger Bands: returns (middle, upper, lower, bandwidth)."""
    mid = calc_sma(closes, period)
    std = np.full_like(closes, np.nan)
    for i in range(period - 1, len(closes)):
        std[i] = np.std(closes[i-period+1:i+1], ddof=0)

    upper = mid + num_std * std
    lower = mid - num_std * std
    bandwidth = np.where(mid > 0, (upper - lower) / mid * 100, np.nan)

    return mid, upper, lower, bandwidth

def calc_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
    """Average True Range."""
    n = len(closes)
    tr = np.full(n, np.nan)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))

    atr = np.full(n, np.nan)
    atr[period-1] = np.mean(tr[:period])
    alpha = 1.0 / period
    for i in range(period, n):
        atr[i] = atr[i-1] * (1 - alpha) + tr[i] * alpha
    return atr


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy A: Mean Reversion (Bollinger Bands)
# ═══════════════════════════════════════════════════════════════════════════════

def strat_mean_reversion(candles: List[Candle], cfg: dict) -> StrategyResult:
    """
    Enter LONG when price touches lower BB (-2σ).
    Enter SHORT when price touches upper BB (+2σ).
    Exit at BB middle (mean). SL at opposite band.
    """
    bb_period = cfg.get("bb_period", 20)
    bb_std = cfg.get("bb_std", 2.0)
    sl_atr_mult = cfg.get("sl_atr_mult", 2.0)
    tp_target = cfg.get("tp_target", "mean")  # "mean" or atr multiplier
    max_hold = cfg.get("max_hold_bars", 20)
    fee_pct = cfg.get("fee_pct", 0.0008)

    closes = np.array([c.c for c in candles])
    highs = np.array([c.h for c in candles])
    lows = np.array([c.l for c in candles])

    mid, upper, lower, bw = calc_bollinger(closes, bb_period, bb_std)
    atr = calc_atr(highs, lows, closes, 14)

    result = StrategyResult(name="MeanReversion", config=cfg)

    i = bb_period + 14  # wait for indicators
    while i < len(candles):
        if np.isnan(mid[i]) or np.isnan(atr[i]) or atr[i] == 0:
            i += 1
            continue

        signal = None
        entry_px = candles[i].c

        # LONG: price at or below lower band
        if closes[i] <= lower[i]:
            signal = "LONG"
            sl = entry_px - sl_atr_mult * atr[i]
            tp = mid[i]  # target = mean
        # SHORT: price at or above upper band
        elif closes[i] >= upper[i]:
            signal = "SHORT"
            sl = entry_px + sl_atr_mult * atr[i]
            tp = mid[i]  # target = mean

        if signal is None:
            i += 1
            continue

        # Simulate trade
        exit_reason = "TIMEOUT"
        exit_px = entry_px
        exit_time = candles[i].ts

        for j in range(i + 1, min(i + max_hold + 1, len(candles))):
            c = candles[j]

            if signal == "LONG":
                if c.l <= sl:
                    exit_px = sl
                    exit_reason = "SL"
                    exit_time = c.ts
                    break
                if c.h >= tp:
                    exit_px = tp
                    exit_reason = "TP"
                    exit_time = c.ts
                    break
            else:  # SHORT
                if c.h >= sl:
                    exit_px = sl
                    exit_reason = "SL"
                    exit_time = c.ts
                    break
                if c.l <= tp:
                    exit_px = tp
                    exit_reason = "TP"
                    exit_time = c.ts
                    break

            exit_px = c.c
            exit_time = c.ts

        if exit_reason == "TIMEOUT":
            exit_px = candles[min(i + max_hold, len(candles) - 1)].c
            exit_time = candles[min(i + max_hold, len(candles) - 1)].ts

        # PnL
        if signal == "LONG":
            pnl = (exit_px - entry_px) / entry_px - fee_pct * 2
        else:
            pnl = (entry_px - exit_px) / entry_px - fee_pct * 2

        result.trades.append(Trade(
            entry_time=candles[i].ts,
            exit_time=exit_time,
            side=signal,
            entry_px=entry_px,
            exit_px=exit_px,
            sl=sl, tp=tp,
            exit_reason=exit_reason,
            pnl_pct=pnl * 100,
        ))

        # Skip ahead past trade duration
        bars_held = max(1, (exit_time - candles[i].ts) // (candles[1].ts - candles[0].ts)) if len(candles) > 1 else 1
        i += max(bars_held, 1) + 1
        continue

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy B: Volatility Breakout (Bollinger Squeeze)
# ═══════════════════════════════════════════════════════════════════════════════

def strat_volatility_breakout(candles: List[Candle], cfg: dict) -> StrategyResult:
    """
    Detect Bollinger Band squeeze (bandwidth < threshold).
    Enter when price breaks out of the squeeze range.
    Direction: breakout side. SL below opposite band.
    """
    bb_period = cfg.get("bb_period", 20)
    squeeze_bw_pct = cfg.get("squeeze_bw_pct", 2.0)  # bandwidth % threshold for squeeze
    breakout_bars = cfg.get("breakout_bars", 3)        # bars after squeeze to look for breakout
    sl_atr_mult = cfg.get("sl_atr_mult", 2.0)
    tp_atr_mult = cfg.get("tp_atr_mult", 3.0)
    max_hold = cfg.get("max_hold_bars", 30)
    fee_pct = cfg.get("fee_pct", 0.0008)

    closes = np.array([c.c for c in candles])
    highs = np.array([c.h for c in candles])
    lows = np.array([c.l for c in candles])

    mid, upper, lower, bw = calc_bollinger(closes, bb_period)
    atr = calc_atr(highs, lows, closes, 14)

    result = StrategyResult(name="VolBreakout", config=cfg)

    # Find squeeze zones
    in_squeeze = False
    squeeze_start = 0

    i = bb_period + 14
    while i < len(candles):
        if np.isnan(bw[i]) or np.isnan(atr[i]) or atr[i] == 0:
            i += 1
            continue

        # Detect squeeze state
        if bw[i] < squeeze_bw_pct:
            if not in_squeeze:
                in_squeeze = True
                squeeze_start = i
            i += 1
            continue

        # Exiting squeeze — check for breakout
        if in_squeeze:
            in_squeeze = False
            signal = None
            entry_px = candles[i].c

            if closes[i] > upper[i-1]:  # Breakout above
                signal = "LONG"
                sl = entry_px - sl_atr_mult * atr[i]
                tp = entry_px + tp_atr_mult * atr[i]
            elif closes[i] < lower[i-1]:  # Breakout below
                signal = "SHORT"
                sl = entry_px + sl_atr_mult * atr[i]
                tp = entry_px - tp_atr_mult * atr[i]

            if signal:
                exit_reason = "TIMEOUT"
                exit_px = entry_px
                exit_time = candles[i].ts

                for j in range(i + 1, min(i + max_hold + 1, len(candles))):
                    c = candles[j]
                    if signal == "LONG":
                        if c.l <= sl:
                            exit_px = sl; exit_reason = "SL"; exit_time = c.ts; break
                        if c.h >= tp:
                            exit_px = tp; exit_reason = "TP"; exit_time = c.ts; break
                    else:
                        if c.h >= sl:
                            exit_px = sl; exit_reason = "SL"; exit_time = c.ts; break
                        if c.l <= tp:
                            exit_px = tp; exit_reason = "TP"; exit_time = c.ts; break
                    exit_px = c.c; exit_time = c.ts

                if exit_reason == "TIMEOUT":
                    k = min(i + max_hold, len(candles) - 1)
                    exit_px = candles[k].c; exit_time = candles[k].ts

                if signal == "LONG":
                    pnl = (exit_px - entry_px) / entry_px - fee_pct * 2
                else:
                    pnl = (entry_px - exit_px) / entry_px - fee_pct * 2

                result.trades.append(Trade(
                    entry_time=candles[i].ts, exit_time=exit_time,
                    side=signal, entry_px=entry_px, exit_px=exit_px,
                    sl=sl, tp=tp, exit_reason=exit_reason, pnl_pct=pnl * 100,
                ))

                bars_held = max(1, (exit_time - candles[i].ts) // (candles[1].ts - candles[0].ts)) if len(candles) > 1 else 1
                i += max(bars_held, 1) + 1
                continue

        i += 1

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy C: Trend Pullback (EMA trend + RSI dip)
# ═══════════════════════════════════════════════════════════════════════════════

def strat_trend_pullback(candles: List[Candle], cfg: dict) -> StrategyResult:
    """
    Determine trend via EMA (fast > slow = uptrend, else downtrend).
    Enter on RSI pullback in trend direction.
    Uptrend: LONG when RSI dips below oversold then recovers.
    Downtrend: SHORT when RSI spikes above overbought then drops.
    """
    ema_fast = cfg.get("ema_fast", 20)
    ema_slow = cfg.get("ema_slow", 50)
    rsi_period = cfg.get("rsi_period", 14)
    rsi_oversold = cfg.get("rsi_oversold", 35)
    rsi_overbought = cfg.get("rsi_overbought", 65)
    sl_atr_mult = cfg.get("sl_atr_mult", 2.0)
    tp_atr_mult = cfg.get("tp_atr_mult", 3.0)
    max_hold = cfg.get("max_hold_bars", 30)
    fee_pct = cfg.get("fee_pct", 0.0008)

    closes = np.array([c.c for c in candles])
    highs = np.array([c.h for c in candles])
    lows = np.array([c.l for c in candles])

    ema_f = calc_ema(closes, ema_fast)
    ema_s = calc_ema(closes, ema_slow)
    rsi = calc_rsi(closes, rsi_period)
    atr = calc_atr(highs, lows, closes, 14)

    result = StrategyResult(name="TrendPullback", config=cfg)

    warmup = max(ema_slow, rsi_period) + 5
    i = warmup
    while i < len(candles):
        if np.isnan(ema_f[i]) or np.isnan(ema_s[i]) or np.isnan(rsi[i]) or np.isnan(atr[i]) or atr[i] == 0:
            i += 1
            continue

        uptrend = ema_f[i] > ema_s[i]
        signal = None
        entry_px = candles[i].c

        # Uptrend + RSI pullback recovery
        if uptrend and i >= 2:
            if rsi[i-1] < rsi_oversold and rsi[i] > rsi_oversold:
                signal = "LONG"
                sl = entry_px - sl_atr_mult * atr[i]
                tp = entry_px + tp_atr_mult * atr[i]

        # Downtrend + RSI overbought rejection
        if not uptrend and i >= 2 and signal is None:
            if rsi[i-1] > rsi_overbought and rsi[i] < rsi_overbought:
                signal = "SHORT"
                sl = entry_px + sl_atr_mult * atr[i]
                tp = entry_px - tp_atr_mult * atr[i]

        if signal is None:
            i += 1
            continue

        exit_reason = "TIMEOUT"
        exit_px = entry_px
        exit_time = candles[i].ts

        for j in range(i + 1, min(i + max_hold + 1, len(candles))):
            c = candles[j]
            if signal == "LONG":
                if c.l <= sl:
                    exit_px = sl; exit_reason = "SL"; exit_time = c.ts; break
                if c.h >= tp:
                    exit_px = tp; exit_reason = "TP"; exit_time = c.ts; break
            else:
                if c.h >= sl:
                    exit_px = sl; exit_reason = "SL"; exit_time = c.ts; break
                if c.l <= tp:
                    exit_px = tp; exit_reason = "TP"; exit_time = c.ts; break
            exit_px = c.c; exit_time = c.ts

        if exit_reason == "TIMEOUT":
            k = min(i + max_hold, len(candles) - 1)
            exit_px = candles[k].c; exit_time = candles[k].ts

        if signal == "LONG":
            pnl = (exit_px - entry_px) / entry_px - fee_pct * 2
        else:
            pnl = (entry_px - exit_px) / entry_px - fee_pct * 2

        result.trades.append(Trade(
            entry_time=candles[i].ts, exit_time=exit_time,
            side=signal, entry_px=entry_px, exit_px=exit_px,
            sl=sl, tp=tp, exit_reason=exit_reason, pnl_pct=pnl * 100,
        ))

        bars_held = max(1, (exit_time - candles[i].ts) // (candles[1].ts - candles[0].ts)) if len(candles) > 1 else 1
        i += max(bars_held, 1) + 1
        continue

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy D: Liquidity Sweep / Stop Hunt (SMC-inspired)
# ═══════════════════════════════════════════════════════════════════════════════

def strat_liquidity_sweep(candles: List[Candle], cfg: dict) -> StrategyResult:
    """
    Smart Money concept: false breakout (liquidity sweep / stop hunt).

    Logic:
      1. Find recent swing high/low over lookback period
      2. Price breaks beyond swing level (sweeps liquidity / stops)
      3. Price reverses back within the range on the SAME or next bar
      4. Enter contrarian to the sweep direction

    This exploits institutional stop hunts: price briefly exceeds a key level
    to trigger retail stops, then reverses.
    """
    lookback = cfg.get("lookback", 20)       # bars to find swing high/low
    sweep_min_pct = cfg.get("sweep_min_pct", 0.001)  # min penetration beyond level
    sweep_max_pct = cfg.get("sweep_max_pct", 0.01)   # max penetration (not a real breakout)
    confirm_bars = cfg.get("confirm_bars", 2)  # bars to confirm reversal
    sl_atr_mult = cfg.get("sl_atr_mult", 1.5)
    tp_atr_mult = cfg.get("tp_atr_mult", 2.5)
    max_hold = cfg.get("max_hold_bars", 20)
    fee_pct = cfg.get("fee_pct", 0.0008)

    closes = np.array([c.c for c in candles])
    highs = np.array([c.h for c in candles])
    lows = np.array([c.l for c in candles])

    atr = calc_atr(highs, lows, closes, 14)

    result = StrategyResult(name="LiqSweep_SMC", config=cfg)

    i = lookback + 14
    while i < len(candles) - confirm_bars:
        if np.isnan(atr[i]) or atr[i] == 0:
            i += 1
            continue

        # Recent swing high/low (excluding current bar)
        swing_high = max(highs[i-lookback:i])
        swing_low = min(lows[i-lookback:i])

        signal = None
        entry_px = None

        c = candles[i]

        # Sweep HIGH: wick above swing high, but close back below
        if c.h > swing_high:
            penetration = (c.h - swing_high) / swing_high
            if sweep_min_pct <= penetration <= sweep_max_pct and c.c < swing_high:
                # Confirm: next bar(s) also close below swing high
                confirmed = True
                for k in range(1, confirm_bars + 1):
                    if i + k < len(candles) and candles[i + k].c > swing_high:
                        confirmed = False
                        break
                if confirmed:
                    signal = "SHORT"
                    entry_px = candles[i + confirm_bars].c if i + confirm_bars < len(candles) else c.c
                    sl = swing_high + sl_atr_mult * atr[i]
                    tp = entry_px - tp_atr_mult * atr[i]

        # Sweep LOW: wick below swing low, but close back above
        if signal is None and c.l < swing_low:
            penetration = (swing_low - c.l) / swing_low
            if sweep_min_pct <= penetration <= sweep_max_pct and c.c > swing_low:
                confirmed = True
                for k in range(1, confirm_bars + 1):
                    if i + k < len(candles) and candles[i + k].c < swing_low:
                        confirmed = False
                        break
                if confirmed:
                    signal = "LONG"
                    entry_px = candles[i + confirm_bars].c if i + confirm_bars < len(candles) else c.c
                    sl = swing_low - sl_atr_mult * atr[i]
                    tp = entry_px + tp_atr_mult * atr[i]

        if signal is None:
            i += 1
            continue

        trade_start = i + confirm_bars
        exit_reason = "TIMEOUT"
        exit_px = entry_px
        exit_time = candles[trade_start].ts if trade_start < len(candles) else candles[i].ts

        for j in range(trade_start + 1, min(trade_start + max_hold + 1, len(candles))):
            cj = candles[j]
            if signal == "LONG":
                if cj.l <= sl:
                    exit_px = sl; exit_reason = "SL"; exit_time = cj.ts; break
                if cj.h >= tp:
                    exit_px = tp; exit_reason = "TP"; exit_time = cj.ts; break
            else:
                if cj.h >= sl:
                    exit_px = sl; exit_reason = "SL"; exit_time = cj.ts; break
                if cj.l <= tp:
                    exit_px = tp; exit_reason = "TP"; exit_time = cj.ts; break
            exit_px = cj.c; exit_time = cj.ts

        if exit_reason == "TIMEOUT":
            k = min(trade_start + max_hold, len(candles) - 1)
            exit_px = candles[k].c; exit_time = candles[k].ts

        if signal == "LONG":
            pnl = (exit_px - entry_px) / entry_px - fee_pct * 2
        else:
            pnl = (entry_px - exit_px) / entry_px - fee_pct * 2

        result.trades.append(Trade(
            entry_time=candles[trade_start].ts if trade_start < len(candles) else candles[i].ts,
            exit_time=exit_time,
            side=signal, entry_px=entry_px, exit_px=exit_px,
            sl=sl, tp=tp, exit_reason=exit_reason, pnl_pct=pnl * 100,
        ))

        bars_held = max(1, (exit_time - candles[trade_start].ts) // (candles[1].ts - candles[0].ts)) if len(candles) > 1 else 1
        i = trade_start + max(bars_held, 1) + 1
        continue

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Config grid
# ═══════════════════════════════════════════════════════════════════════════════

def get_config_grid():
    """Generate parameter configurations to test for each strategy."""

    configs = {}

    # A) Mean Reversion
    configs["MeanReversion"] = []
    for bb_period in [15, 20, 30]:
        for bb_std in [1.5, 2.0, 2.5]:
            for sl_atr in [1.5, 2.0, 3.0]:
                for max_hold in [10, 20, 40]:
                    configs["MeanReversion"].append({
                        "bb_period": bb_period, "bb_std": bb_std,
                        "sl_atr_mult": sl_atr, "max_hold_bars": max_hold,
                    })

    # B) Volatility Breakout
    configs["VolBreakout"] = []
    for bb_period in [15, 20, 30]:
        for squeeze_bw in [1.5, 2.0, 3.0]:
            for sl_atr in [1.5, 2.0]:
                for tp_atr in [2.0, 3.0, 4.0]:
                    configs["VolBreakout"].append({
                        "bb_period": bb_period, "squeeze_bw_pct": squeeze_bw,
                        "sl_atr_mult": sl_atr, "tp_atr_mult": tp_atr,
                        "max_hold_bars": 30,
                    })

    # C) Trend Pullback
    configs["TrendPullback"] = []
    for ema_fast, ema_slow in [(10, 30), (20, 50), (20, 100)]:
        for rsi_os, rsi_ob in [(30, 70), (35, 65), (25, 75)]:
            for sl_atr in [1.5, 2.0]:
                for tp_atr in [2.0, 3.0, 4.0]:
                    configs["TrendPullback"].append({
                        "ema_fast": ema_fast, "ema_slow": ema_slow,
                        "rsi_oversold": rsi_os, "rsi_overbought": rsi_ob,
                        "sl_atr_mult": sl_atr, "tp_atr_mult": tp_atr,
                        "max_hold_bars": 30,
                    })

    # D) Liquidity Sweep (SMC)
    configs["LiqSweep_SMC"] = []
    for lookback in [10, 20, 30]:
        for sweep_min in [0.0005, 0.001]:
            for sweep_max in [0.005, 0.01, 0.02]:
                for confirm in [1, 2]:
                    for sl_atr in [1.5, 2.0]:
                        for tp_atr in [2.0, 3.0]:
                            configs["LiqSweep_SMC"].append({
                                "lookback": lookback, "sweep_min_pct": sweep_min,
                                "sweep_max_pct": sweep_max, "confirm_bars": confirm,
                                "sl_atr_mult": sl_atr, "tp_atr_mult": tp_atr,
                                "max_hold_bars": 20,
                            })

    total = sum(len(v) for v in configs.values())
    for name, cfgs in configs.items():
        log.info(f"  {name}: {len(cfgs)} configs")
    log.info(f"  TOTAL: {total} configs")

    return configs


def get_fine_grid_volbreakout():
    """Fine-grained grid around winning VolBreakout parameters.

    Winners from coarse grid:
      BTC 6m:  bb=20, sq=2.0, sl=2.0, tp=3.0
      BTC 12m: bb=20, sq=2.0, sl=2.0, tp=4.0
      ETH 6m:  bb=30, sq=2.0, sl=2.0, tp=4.0
      SOL 6m:  bb=15, sq=2.0, sl=2.0, tp=3.0

    Fine grid explores around these with small steps.
    """
    configs = {"VolBreakout": []}

    for bb_period in [12, 14, 16, 18, 20, 22, 25, 28, 30, 35]:
        for squeeze_bw in [1.5, 1.75, 2.0, 2.25, 2.5, 3.0]:
            for sl_atr in [1.5, 1.75, 2.0, 2.25, 2.5]:
                for tp_atr in [2.5, 3.0, 3.5, 4.0, 4.5, 5.0]:
                    for max_hold in [20, 25, 30, 40, 50]:
                        configs["VolBreakout"].append({
                            "bb_period": bb_period, "squeeze_bw_pct": squeeze_bw,
                            "sl_atr_mult": sl_atr, "tp_atr_mult": tp_atr,
                            "max_hold_bars": max_hold,
                        })

    total = len(configs["VolBreakout"])
    log.info(f"  VolBreakout FINE grid: {total} configs")

    return configs


# ═══════════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════════

STRATEGY_FUNCS = {
    "MeanReversion": strat_mean_reversion,
    "VolBreakout": strat_volatility_breakout,
    "TrendPullback": strat_trend_pullback,
    "LiqSweep_SMC": strat_liquidity_sweep,
}

def run_all(candles: List[Candle], configs: dict) -> List[StrategyResult]:
    """Run all strategy configs on the candle data."""
    results = []

    for strat_name, cfgs in configs.items():
        func = STRATEGY_FUNCS[strat_name]
        log.info(f"\n{'='*60}")
        log.info(f"Testing {strat_name} -- {len(cfgs)} configs...")
        log.info(f"{'='*60}")fo(f"{'='*60}")

        best = None
        log_step = max(1, len(cfgs) // 10)  # ~10 progress updates
        for idx, cfg in enumerate(cfgs):
            r = func(candles, cfg)
            results.append(r)

            fc = r.full_compound()
            if r.n_trades >= 5 and (best is None or fc["total_pnl_pct"] > best.full_compound()["total_pnl_pct"]):
                best = r

            if (idx + 1) % log_step == 0:
                log.info(f"  ... {idx+1}/{len(cfgs)} done")

        if best:
            fc = best.full_compound()
            mc = best.monthly_compound()
            log.info(
                f"\n  BEST {strat_name}: {best.n_trades} trades, "
                f"WR={best.win_rate:.1f}%, "
                f"Compound={fc['total_pnl_pct']:+.2f}% (${fc['total_pnl_usd']:+,.2f}), "
                f"Mo.Reset={mc['total_pnl_pct']:+.2f}% (${mc['total_pnl_usd']:+,.2f}), "
                f"PF={best.profit_factor:.2f}, MaxDD={fc['max_drawdown_pct']:.2f}%"
            )
            log.info(f"  Config: {best.config}")
        else:
            log.info(f"\n  {strat_name}: No config with ≥5 trades")

    return results


def print_summary(results: List[StrategyResult], symbol: str, timeframe: str,
                   initial_balance: float = INITIAL_BALANCE):
    """Print final comparison table with both PnL modes."""

    # Filter to results with at least 5 trades
    valid = [r for r in results if r.n_trades >= 5]

    if not valid:
        log.info("\n❌ No strategy produced ≥5 trades. Try longer history or different timeframe.\n")
        return

    # Sort by compound PnL (full reinvest)
    valid.sort(key=lambda r: r.full_compound(initial_balance)["total_pnl_pct"], reverse=True)

    log.info(f"\n{'='*120}")
    log.info(f"  FINAL RESULTS — {symbol} {timeframe} — Top 20 by Compound PnL (balance=${initial_balance:,.0f})")
    log.info(f"{'='*120}")
    log.info(
        f"{'#':<4} {'Strategy':<15} {'Trds':>5} {'WR%':>6} "
        f"{'Mo.Reset%':>10} {'Mo.Reset$':>10} "
        f"{'Compound%':>10} {'Compound$':>10} "
        f"{'PF':>6} {'MaxDD%':>7} {'Sharpe':>7} {'ConsL':>6} {'Exits':>20}"
    )
    log.info("-" * 120)

    for rank, r in enumerate(valid[:20], 1):
        mc = r.monthly_compound(initial_balance)
        fc = r.full_compound(initial_balance)

        exits = {}
        for t in r.trades:
            exits[t.exit_reason] = exits.get(t.exit_reason, 0) + 1
        exit_str = " ".join(f"{k}:{v}" for k, v in sorted(exits.items()))

        log.info(
            f"{rank:<4} {r.name:<15} {r.n_trades:>5} {r.win_rate:>5.1f}% "
            f"{mc['total_pnl_pct']:>+9.2f}% {mc['total_pnl_usd']:>+9.2f}$ "
            f"{fc['total_pnl_pct']:>+9.2f}% {fc['total_pnl_usd']:>+9.2f}$ "
            f"{r.profit_factor:>5.2f} {fc['max_drawdown_pct']:>6.2f}% "
            f"{r.sharpe_approx:>6.2f} {r.max_consecutive_losses:>5} {exit_str:>20}"
        )

    # ── Detailed best config ──────────────────────────────────────────
    best = valid[0]
    mc = best.monthly_compound(initial_balance)
    fc = best.full_compound(initial_balance)

    log.info(f"\n{'='*70}")
    log.info(f"  🏆 BEST OVERALL: {best.name}")
    log.info(f"{'='*70}")
    log.info(f"  Config:  {best.config}")
    log.info(f"  Trades:  {best.n_trades}  |  Win Rate: {best.win_rate:.1f}%")
    log.info(f"  Profit Factor: {best.profit_factor:.2f}  |  Sharpe: {best.sharpe_approx:.2f}")
    log.info(f"  Max Consecutive Losses: {best.max_consecutive_losses}")

    log.info(f"\n  ── MODE 1: Monthly Reset (withdraw profit each month) ──")
    log.info(f"  Total:  {mc['total_pnl_pct']:+.2f}%  =  ${mc['total_pnl_usd']:+,.2f}")
    log.info(f"  Avg/month: {mc['avg_monthly_pct']:+.2f}%  =  ${initial_balance * mc['avg_monthly_pct'] / 100:+,.2f}")
    log.info(f"  Best month:  {mc['best_month_pct']:+.2f}%  |  Worst month: {mc['worst_month_pct']:+.2f}%")
    log.info(f"  Profitable months: {mc['profitable_months']}/{mc['total_months']}")

    log.info(f"\n  ── MODE 2: Full Compound (total reinvestment) ──")
    log.info(f"  ${initial_balance:,.0f} → ${fc['final_balance']:,.2f}  ({fc['total_pnl_pct']:+.2f}%)")
    log.info(f"  Max balance: ${fc['max_balance']:,.2f}  |  Min balance: ${fc['min_balance']:,.2f}")
    log.info(f"  Max Drawdown: {fc['max_drawdown_pct']:.2f}%")

    # Monthly breakdown table
    if mc["months"]:
        log.info(f"\n  ── Monthly Breakdown ──")
        log.info(f"  {'Month':<10} {'Trades':>7} {'PnL%':>9} {'PnL$':>10}")
        log.info(f"  {'-'*40}")
        for m in mc["months"]:
            bar = "█" * max(0, int(m["pnl_pct"] / 0.5)) if m["pnl_pct"] > 0 else "▒" * max(0, int(-m["pnl_pct"] / 0.5))
            log.info(f"  {m['month']:<10} {m['trades']:>7} {m['pnl_pct']:>+8.2f}% {m['pnl_usd']:>+9.2f}$ {bar}")

    # Verdict
    cpnl = fc["total_pnl_pct"]
    if cpnl > 5 and best.profit_factor > 1.3 and best.win_rate > 40:
        verdict = "✅ PROMISING — worth implementing and forward-testing!"
    elif cpnl > 0 and best.profit_factor > 1.1:
        verdict = "⚠️  MARGINAL — small edge, may not survive live conditions"
    else:
        verdict = "❌ NO EDGE — none of the strategies are profitable"
    log.info(f"\n  VERDICT: {verdict}")

    # Per-strategy summary
    log.info(f"\n{'='*70}")
    log.info(f"  Per-Strategy Summary")
    log.info(f"{'='*70}")

    strat_names = set(r.name for r in valid)
    for name in sorted(strat_names):
        strat_results = [r for r in valid if r.name == name]
        best_s = max(strat_results, key=lambda r: r.full_compound(initial_balance)["total_pnl_pct"])
        fc_s = best_s.full_compound(initial_balance)
        profitable = sum(1 for r in strat_results if r.full_compound(initial_balance)["total_pnl_pct"] > 0)
        log.info(
            f"  {name:<16}: {len(strat_results)} valid configs, "
            f"{profitable} profitable ({profitable/len(strat_results)*100:.0f}%), "
            f"best compound={fc_s['total_pnl_pct']:+.2f}% (${fc_s['total_pnl_usd']:+,.2f})"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Multi-strategy klines backtest")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (default: BTCUSDT)")
    parser.add_argument("--timeframe", default="1h", help="Candle interval: 1h, 4h (default: 1h)")
    parser.add_argument("--months", type=int, default=6, help="Months of history (default: 6)")
    parser.add_argument("--balance", type=float, default=1000.0,
                        help="Initial balance in USD (default: 1000)")
    parser.add_argument("--strategy", default="all",
                        help="Test specific strategy: MeanReversion, VolBreakout, TrendPullback, LiqSweep_SMC, or 'all'")
    parser.add_argument("--fine", action="store_true",
                        help="Use fine-grained grid for VolBreakout (9000 configs, ~5-10 min)")
    args = parser.parse_args()

    global INITIAL_BALANCE
    INITIAL_BALANCE = args.balance

    mode_str = "FINE GRID" if args.fine else "COARSE GRID"
    log.info(f"\n{'='*60}")
    log.info(f"  Multi-Strategy Backtest [{mode_str}]")
    log.info(f"  {args.symbol} | {args.timeframe} | {args.months} months | ${args.balance:,.0f}")
    log.info(f"{'='*60}\n")

    # Time range
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=args.months * 30)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    log.info(f"Period: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}")

    # Fetch data
    candles = fetch_klines(args.symbol, args.timeframe, start_ms, end_ms)

    if len(candles) < 100:
        log.error(f"Not enough candles ({len(candles)}). Need at least 100.")
        return

    log.info(f"Price range: ${min(c.l for c in candles):,.0f} — ${max(c.h for c in candles):,.0f}")
    log.info(f"Period covered: {datetime.fromtimestamp(candles[0].ts/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} → "
             f"{datetime.fromtimestamp(candles[-1].ts/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")

    # Get configs
    if args.fine:
        configs = get_fine_grid_volbreakout()
    else:
        configs = get_config_grid()

        # Filter if specific strategy requested
        if args.strategy != "all":
            if args.strategy not in configs:
                log.error(f"Unknown strategy: {args.strategy}. Available: {list(configs.keys())}")
                return
            configs = {args.strategy: configs[args.strategy]}

    # Run
    results = run_all(candles, configs)

    # Summary
    print_summary(results, args.symbol, args.timeframe, args.balance)


if __name__ == "__main__":
    main()
