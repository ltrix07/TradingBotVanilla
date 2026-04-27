"""
strategy.py — Funding Rate Contrarian Strategy.

Signal generation based on Binance Futures funding rate:
  - Extreme positive funding → SHORT (market overleveraged long, contrarian edge)
  - Extreme negative funding → LONG  (market overleveraged short, contrarian edge)

The strategy exploits two synergistic effects:
  1. Directional: extreme funding indicates crowded positioning → mean-reversion edge
  2. Carry: being on the contrarian side COLLECTS the funding payment every 8h

Signal fires when: |funding_rate_bps| > threshold_bps
Direction: contrarian — opposite to the majority of the market.

No MACD, RSI, or order book confirmation needed.
ATR-based SL/TP and trailing stops are handled by risk.py (unchanged).
"""

import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# ── Primary signal: funding rate threshold ───────────────────────────────────

def generate_signal(
    funding_info: dict | None,
    cfg: dict,
    position_active: bool = False,
) -> str | None:
    """Generate a contrarian trading signal based on funding rate extremes.

    When funding rate is strongly positive (longs paying shorts), the market
    is overleveraged long — go SHORT (contrarian).
    When funding rate is strongly negative (shorts paying longs), the market
    is overleveraged short — go LONG (contrarian).

    In both cases we also COLLECT the funding payment (we're on the receiving side).

    Args:
        funding_info:     Dict from fetcher.fetch_funding_rate_async():
                            funding_rate      — float (e.g. 0.0001 = 0.01%/8h)
                            funding_rate_bps  — float (e.g. 1.0 = 1 bps)
                            mark_price        — float
                            next_funding_time — int (ms timestamp)
        cfg:              Bot configuration dict.
        position_active:  True if there's already an open position (skip signal).

    Returns:
        'BUY_YES' (→ LONG), 'BUY_NO' (→ SHORT), or None.
        The caller (main.py) maps BUY_YES→LONG, BUY_NO→SHORT.
    """
    if position_active:
        return None

    if funding_info is None:
        log.warning("Funding rate unavailable — no signal")
        return None

    # ── Read strategy config ─────────────────────────────────────────────
    strat_cfg = cfg.get("strategy", {}).get("funding", {})
    threshold_bps = float(strat_cfg.get("threshold_bps", 25.0))

    rate_bps = float(funding_info.get("funding_rate_bps", 0.0))

    # ── Time to next funding settlement ──────────────────────────────────
    next_funding_ms = funding_info.get("next_funding_time", 0)
    if next_funding_ms > 0:
        next_dt = datetime.fromtimestamp(next_funding_ms / 1000, tz=timezone.utc)
        time_to_funding_sec = (next_dt - datetime.now(timezone.utc)).total_seconds()
        hours_to_funding = time_to_funding_sec / 3600
    else:
        hours_to_funding = -1.0

    log.info(
        "FUNDING: rate=%+.2f bps (%.4f%%)  threshold=+/-%.0f bps  "
        "mark=$%.2f  next_settlement_in=%.1fh",
        rate_bps, rate_bps / 100, threshold_bps,
        funding_info.get("mark_price", 0),
        hours_to_funding,
    )

    # ── Contrarian logic ─────────────────────────────────────────────────
    #  rate > +threshold  →  longs pay shorts  →  SHORT (collect payment)
    #  rate < -threshold  →  shorts pay longs  →  LONG  (collect payment)
    if rate_bps > threshold_bps:
        log.info(
            "FUNDING SIGNAL: SHORT  (rate %+.2f bps > +%.0f threshold — "
            "market overleveraged long, we collect %.4f%%)",
            rate_bps, threshold_bps, abs(rate_bps) / 100,
        )
        return "BUY_NO"   # → SHORT

    if rate_bps < -threshold_bps:
        log.info(
            "FUNDING SIGNAL: LONG  (rate %+.2f bps < -%.0f threshold — "
            "market overleveraged short, we collect %.4f%%)",
            rate_bps, threshold_bps, abs(rate_bps) / 100,
        )
        return "BUY_YES"  # → LONG

    log.info(
        "FUNDING: no signal  (|%+.2f| <= %.0f bps — rate within normal range)",
        rate_bps, threshold_bps,
    )
    return None


# ── Reverse-close check (replaces MACD-based reverse) ────────────────────────

def should_reverse_close(
    funding_info: dict | None,
    position_side: str,
    cfg: dict,
) -> bool:
    """Check if funding rate has flipped against our position.

    Returns True if the funding rate now favors the OPPOSITE direction,
    meaning our contrarian thesis has reversed.

    Example: we went SHORT because rate was +30 bps. If rate drops to
    -25 bps, the market is now overleveraged short → our SHORT is wrong.

    This replaces the MACD-based use_reverse_close logic from the old strategy.
    """
    if funding_info is None:
        return False

    strat_cfg = cfg.get("strategy", {}).get("funding", {})
    threshold_bps = float(strat_cfg.get("threshold_bps", 25.0))
    rate_bps = float(funding_info.get("funding_rate_bps", 0.0))

    # We went SHORT because rate was strongly positive.
    # If rate is now strongly negative → thesis flipped, close.
    if position_side == "SHORT" and rate_bps < -threshold_bps:
        log.info(
            "REVERSE: SHORT position but funding flipped negative "
            "(%+.2f bps < -%.0f) — closing",
            rate_bps, threshold_bps,
        )
        return True

    # We went LONG because rate was strongly negative.
    # If rate is now strongly positive → thesis flipped, close.
    if position_side == "LONG" and rate_bps > threshold_bps:
        log.info(
            "REVERSE: LONG position but funding flipped positive "
            "(%+.2f bps > +%.0f) — closing",
            rate_bps, threshold_bps,
        )
        return True

    return False


# ── Status-line helper ───────────────────────────────────────────────────────

def get_funding_state(funding_info: dict | None) -> dict:
    """Return funding rate info for the status line display.

    Returns dict with keys: rate_bps, mark_price, bias.
    bias is "SHORT", "LONG", or "NEUTRAL" — the direction the funding
    rate is pushing toward (for display only, not the signal).
    """
    if funding_info is None:
        return {"rate_bps": None, "mark_price": None, "bias": None}

    rate_bps = float(funding_info.get("funding_rate_bps", 0.0))
    return {
        "rate_bps":   round(rate_bps, 2),
        "mark_price": funding_info.get("mark_price"),
        "bias": (
            "SHORT" if rate_bps > 5   # longs paying → contrarian SHORT
            else "LONG" if rate_bps < -5
            else "NEUTRAL"
        ),
    }
