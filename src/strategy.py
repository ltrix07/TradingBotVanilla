"""
strategy.py -- Volatility Breakout Strategy (Bollinger Squeeze).

Signal generation based on Bollinger Bands squeeze and breakout:
  1. Detect low-volatility compression (BB bandwidth < threshold)
  2. When price breaks out of the compressed range:
     - Close above upper BB -> LONG (volatility expansion upward)
     - Close below lower BB -> SHORT (volatility expansion downward)

The strategy exploits the volatility cycle:
  - Markets alternate between compression (squeeze) and expansion (breakout)
  - After a squeeze, the breakout direction tends to be sustained
  - ATR-based SL/TP provide adaptive risk management

Optimal parameters (from 9000-config backtest on BTC/ETH/SOL):
  bb_period=22, squeeze_bw_pct=2.0, sl_atr_mult=1.75, tp_atr_mult=4.0

SL/TP and trailing stops are handled by risk.py (unchanged).
"""

import logging
from typing import Optional

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# -- Indicator calculations ---------------------------------------------------

def _calc_sma(data: list[float], period: int) -> Optional[float]:
    """Simple Moving Average of the last `period` values."""
    if len(data) < period:
        return None
    return sum(data[-period:]) / period


def _calc_std(data: list[float], period: int) -> Optional[float]:
    """Population standard deviation of the last `period` values."""
    if len(data) < period:
        return None
    subset = data[-period:]
    mean = sum(subset) / period
    variance = sum((x - mean) ** 2 for x in subset) / period
    return variance ** 0.5


def _calc_bollinger(closes: list[float], period: int = 22, num_std: float = 2.0) -> Optional[dict]:
    """Calculate Bollinger Bands from close prices.

    Returns dict with: mid, upper, lower, bandwidth_pct
    or None if not enough data.
    """
    if len(closes) < period:
        return None

    mid = _calc_sma(closes, period)
    std = _calc_std(closes, period)

    if mid is None or std is None or mid == 0:
        return None

    upper = mid + num_std * std
    lower = mid - num_std * std
    bandwidth_pct = (upper - lower) / mid * 100

    return {
        "mid": mid,
        "upper": upper,
        "lower": lower,
        "bandwidth_pct": bandwidth_pct,
        "std": std,
    }


def _calc_atr(candles: list[dict], period: int = 14) -> Optional[float]:
    """Calculate ATR (Average True Range) from candle dicts.

    Each candle must have: high, low, close.
    Returns the current ATR value or None if not enough data.
    """
    if len(candles) < period + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    if len(trs) < period:
        return None

    # Wilder's smoothing (EMA-style)
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period

    return atr


# -- Squeeze state tracker ----------------------------------------------------

class SqueezeTracker:
    """Tracks Bollinger Band squeeze state across candle updates.

    A squeeze is active when BB bandwidth < threshold.
    When the squeeze ends (bandwidth expands), a breakout signal is generated
    if price closes beyond the previous BB boundary.
    """

    def __init__(self):
        self._in_squeeze: bool = False
        self._squeeze_bars: int = 0
        self._squeeze_upper: float = 0.0
        self._squeeze_lower: float = 0.0
        self._squeeze_mid: float = 0.0
        self._prev_bandwidth: float = 999.0

    def reset(self):
        self._in_squeeze = False
        self._squeeze_bars = 0
        self._prev_bandwidth = 999.0

    @property
    def in_squeeze(self) -> bool:
        return self._in_squeeze

    @property
    def squeeze_bars(self) -> int:
        return self._squeeze_bars

    @property
    def squeeze_upper(self) -> float:
        return self._squeeze_upper

    @property
    def squeeze_lower(self) -> float:
        return self._squeeze_lower

    def update(self, bb: dict, squeeze_bw_pct: float) -> str:
        """Update squeeze state with new BB values.

        Returns:
            'SQUEEZE_START' - squeeze just started
            'IN_SQUEEZE'    - still in squeeze
            'SQUEEZE_END'   - squeeze just ended (check for breakout)
            'NO_SQUEEZE'    - not in squeeze, no transition
        """
        bw = bb["bandwidth_pct"]

        if bw < squeeze_bw_pct:
            if not self._in_squeeze:
                self._in_squeeze = True
                self._squeeze_bars = 1
                self._squeeze_upper = bb["upper"]
                self._squeeze_lower = bb["lower"]
                self._squeeze_mid = bb["mid"]
                self._prev_bandwidth = bw
                return "SQUEEZE_START"
            else:
                self._squeeze_bars += 1
                # Track tightest bands during squeeze
                self._squeeze_upper = min(self._squeeze_upper, bb["upper"])
                self._squeeze_lower = max(self._squeeze_lower, bb["lower"])
                self._squeeze_mid = bb["mid"]
                self._prev_bandwidth = bw
                return "IN_SQUEEZE"
        else:
            if self._in_squeeze:
                self._in_squeeze = False
                self._prev_bandwidth = bw
                return "SQUEEZE_END"
            else:
                self._prev_bandwidth = bw
                return "NO_SQUEEZE"


# Module-level squeeze tracker (persists across main loop iterations)
_squeeze = SqueezeTracker()


# -- Primary signal: Volatility Breakout --------------------------------------

def generate_signal(
    candles: list[dict],
    cfg: dict,
    position_active: bool = False,
) -> Optional[str]:
    """Generate a trading signal based on Bollinger Bands squeeze breakout.

    When BB bandwidth compresses below threshold (squeeze), then expands:
      - Close above upper BB -> LONG (BUY_YES)
      - Close below lower BB -> SHORT (BUY_NO)

    Args:
        candles:          List of candle dicts with keys:
                            timestamp, open, high, low, close, volume
                          Must have at least bb_period + 1 candles.
        cfg:              Bot configuration dict.
        position_active:  True if there's already an open position.

    Returns:
        'BUY_YES' (-> LONG), 'BUY_NO' (-> SHORT), or None.
    """
    if position_active:
        return None

    if not candles or len(candles) < 30:
        log.warning("Not enough candles for VolBreakout signal (%d)",
                     len(candles) if candles else 0)
        return None

    # -- Read strategy config --
    strat_cfg = cfg.get("strategy", {}).get("vol_breakout", {})
    bb_period = int(strat_cfg.get("bb_period", 22))
    squeeze_bw_pct = float(strat_cfg.get("squeeze_bw_pct", 2.0))

    closes = [c["close"] for c in candles]

    # -- Calculate Bollinger Bands --
    bb = _calc_bollinger(closes, bb_period)
    if bb is None:
        log.warning("BB calculation failed -- not enough data")
        return None

    current_close = closes[-1]

    # -- Update squeeze tracker --
    state = _squeeze.update(bb, squeeze_bw_pct)

    log.info(
        "VOLBREAK: close=$%.2f  BB=[%.2f / %.2f / %.2f]  BW=%.2f%%  "
        "squeeze=%s (bars=%d)  state=%s",
        current_close, bb["lower"], bb["mid"], bb["upper"],
        bb["bandwidth_pct"], _squeeze.in_squeeze, _squeeze.squeeze_bars, state,
    )

    # -- Signal on squeeze exit --
    if state == "SQUEEZE_END":
        if current_close > _squeeze.squeeze_upper:
            log.info(
                "VOLBREAK SIGNAL: LONG -- breakout above squeeze upper "
                "(close $%.2f > squeeze_upper $%.2f, squeeze lasted %d bars)",
                current_close, _squeeze.squeeze_upper, _squeeze.squeeze_bars,
            )
            return "BUY_YES"   # -> LONG

        if current_close < _squeeze.squeeze_lower:
            log.info(
                "VOLBREAK SIGNAL: SHORT -- breakout below squeeze lower "
                "(close $%.2f < squeeze_lower $%.2f, squeeze lasted %d bars)",
                current_close, _squeeze.squeeze_lower, _squeeze.squeeze_bars,
            )
            return "BUY_NO"    # -> SHORT

        log.info(
            "VOLBREAK: squeeze ended but no breakout "
            "(close $%.2f within [%.2f, %.2f])",
            current_close, _squeeze.squeeze_lower, _squeeze.squeeze_upper,
        )

    return None


# -- Reverse-close check ------------------------------------------------------

def should_reverse_close(
    candles: list[dict],
    position_side: str,
    cfg: dict,
) -> bool:
    """Check if price has moved back inside BB mid, suggesting breakout failed.

    Returns True if the breakout thesis is invalidated:
      - LONG position but close drops below BB mid -> failed
      - SHORT position but close rises above BB mid -> failed
    """
    if not candles or len(candles) < 30:
        return False

    strat_cfg = cfg.get("strategy", {}).get("vol_breakout", {})
    bb_period = int(strat_cfg.get("bb_period", 22))

    closes = [c["close"] for c in candles]
    bb = _calc_bollinger(closes, bb_period)
    if bb is None:
        return False

    current_close = closes[-1]

    if position_side == "LONG" and current_close < bb["mid"]:
        log.info(
            "REVERSE: LONG position but close $%.2f < BB mid $%.2f -- closing",
            current_close, bb["mid"],
        )
        return True

    if position_side == "SHORT" and current_close > bb["mid"]:
        log.info(
            "REVERSE: SHORT position but close $%.2f > BB mid $%.2f -- closing",
            current_close, bb["mid"],
        )
        return True

    return False


# -- Status-line helper -------------------------------------------------------

def get_strategy_state(candles: list[dict], cfg: dict) -> dict:
    """Return strategy info for the status line display."""
    if not candles or len(candles) < 30:
        return {
            "bandwidth_pct": None, "in_squeeze": False,
            "squeeze_bars": 0, "bb_mid": None, "bias": None,
        }

    strat_cfg = cfg.get("strategy", {}).get("vol_breakout", {})
    bb_period = int(strat_cfg.get("bb_period", 22))

    closes = [c["close"] for c in candles]
    bb = _calc_bollinger(closes, bb_period)

    if bb is None:
        return {
            "bandwidth_pct": None, "in_squeeze": False,
            "squeeze_bars": 0, "bb_mid": None, "bias": None,
        }

    current_close = closes[-1]

    return {
        "bandwidth_pct": round(bb["bandwidth_pct"], 2),
        "in_squeeze": _squeeze.in_squeeze,
        "squeeze_bars": _squeeze.squeeze_bars,
        "bb_mid": round(bb["mid"], 2),
        "bias": (
            "LONG" if current_close > bb["upper"]
            else "SHORT" if current_close < bb["lower"]
            else "SQUEEZE" if _squeeze.in_squeeze
            else "NEUTRAL"
        ),
    }


def reset_squeeze_state():
    """Reset the squeeze tracker (call on bot restart or strategy change)."""
    _squeeze.reset()
