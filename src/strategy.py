"""
strategy.py — Trading strategy module.

Signal generation uses a three-layer confirmation model:
  1. MACD crossover (primary signal — required)
  2. RSI filter      (blocks signal when price is at extremes)
  3. Order book imbalance confirmation (optional, from live book depth)

Signal fires when: MACD crossover AND at least one of (RSI ok OR book ok).
This reduces false signals while keeping the bot responsive.
"""

import logging
import pandas as pd
import pandas_ta as ta


# ── Primary signal: MACD crossover ────────────────────────────────────────────

def _get_macd_params(cfg: dict) -> tuple[int, int, int]:
    """Extract MACD parameters supporting both nested and flat config layouts."""
    strat = cfg.get("strategy", {})
    params = strat.get("parameters", strat)
    return (
        int(params.get("fast_ema", 3)),
        int(params.get("slow_ema", 15)),
        int(params.get("signal_smoothing", 3)),
    )


def _compute_macd_signal(df: pd.DataFrame, cfg: dict) -> str | None:
    """Return 'BUY_YES', 'BUY_NO', or None based on MACD line/signal crossover."""
    fast, slow, smooth = _get_macd_params(cfg)

    macd_result = ta.macd(df["close"], fast=fast, slow=slow, signal=smooth)

    if macd_result is None:
        return None
    
    macd_col   = f"MACD_{fast}_{slow}_{smooth}"
    signal_col = f"MACDs_{fast}_{slow}_{smooth}"

    if macd_col not in macd_result.columns or signal_col not in macd_result.columns:
        return None

    macd_line   = macd_result[macd_col]
    signal_line = macd_result[signal_col]

    if macd_line.isna().iloc[-1] or signal_line.isna().iloc[-1]:
        return None
    if macd_line.isna().iloc[-2] or signal_line.isna().iloc[-2]:
        return None

    prev_macd   = macd_line.iloc[-2]
    prev_signal = signal_line.iloc[-2]
    curr_macd   = macd_line.iloc[-1]
    curr_signal = signal_line.iloc[-1]

    if prev_macd < prev_signal and curr_macd > curr_signal:
        return "BUY_YES"
    if prev_macd > prev_signal and curr_macd < curr_signal:
        return "BUY_NO"
    return None


# ── Confirmation 1: RSI filter ─────────────────────────────────────────────────

def _compute_rsi_confirmation(df: pd.DataFrame, cfg: dict, signal: str) -> bool:
    """Return True if RSI does NOT block the signal direction.

    BUY_YES is blocked when RSI >= overbought (already stretched upward).
    BUY_NO  is blocked when RSI <= oversold  (already stretched downward).
    Returns True (pass) when not enough data to compute.
    """
    rsi_cfg = cfg.get("strategy", {}).get("rsi", {})
    period     = int(rsi_cfg.get("period", 14))
    overbought = float(rsi_cfg.get("overbought", 65))
    oversold   = float(rsi_cfg.get("oversold", 35))

    if len(df) < period + 1:
        return True  # insufficient data — do not block

    try:
        rsi_series = ta.rsi(df["close"], length=period)
        if rsi_series is None or rsi_series.isna().iloc[-1]:
            return True
        current_rsi = float(rsi_series.iloc[-1])
    except Exception:
        return True

    if signal == "BUY_YES":
        return current_rsi < overbought   # pass if not overbought
    else:
        return current_rsi > oversold     # pass if not oversold


# ── Confirmation 2: Order book imbalance ──────────────────────────────────────

def _compute_book_confirmation(book_data: dict | None, cfg: dict, signal: str) -> bool:
    """Return True if order book imbalance confirms the signal direction.

    book_imbalance = bid_volume / (bid_volume + ask_volume)
      > threshold  → more buying pressure → confirms BUY_YES
      < 1-threshold → more selling pressure → confirms BUY_NO

    Returns True (pass) when book_data is unavailable.
    """
    if book_data is None:
        return True  # no depth data — do not block

    ob_cfg    = cfg.get("strategy", {}).get("order_book", {})
    threshold = float(ob_cfg.get("imbalance_threshold", 0.60))
    imbalance = book_data.get("book_imbalance", 0.5)

    if signal == "BUY_YES":
        return imbalance >= threshold
    else:
        return imbalance <= (1.0 - threshold)


# ── Entry filters (optional last-layer gate) ──────────────────────────────────

def _apply_entry_filters(
    cfg: dict,
    signal: str,
    book_data: dict | None,
    candles: list[dict],
) -> bool:
    """Return True if the signal passes all configured entry_filters.

    Every filter parameter is optional — absent or None means the filter
    is skipped, so old configs without entry_filters work unchanged.
    """
    filters = cfg.get("strategy", {}).get("entry_filters", {})
    if not filters:
        return True

    # ── Trend direction filter (EMA-based) ─────────────────────────────
    trend_period = filters.get("trend_ema_period")
    if trend_period is not None:
        trend_period = int(trend_period)
        if len(candles) >= trend_period + 1:
            closes = pd.Series([float(c["close"]) for c in candles])
            ema = ta.ema(closes, length=trend_period)
            if ema is not None and pd.notna(ema.iloc[-1]):
                current_close = float(candles[-1]["close"])
                ema_now = float(ema.iloc[-1])
                if signal == "BUY_YES" and current_close < ema_now:
                    logging.info(
                        "entry_filter BLOCKED: BUY_YES against downtrend "
                        "(close %.2f < EMA%d %.2f)", current_close, trend_period, ema_now,
                    )
                    return False
                if signal == "BUY_NO" and current_close > ema_now:
                    logging.info(
                        "entry_filter BLOCKED: BUY_NO against uptrend "
                        "(close %.2f > EMA%d %.2f)", current_close, trend_period, ema_now,
                    )
                    return False

    # ── Volume spike filter (uses Binance candle data) ────────────────────
    require_spike = filters.get("require_volume_spike")
    if require_spike:
        period = int(filters.get("volume_spike_period", 10))
        multiplier = float(filters.get("volume_spike_multiplier", 1.5))

        if len(candles) >= period + 1:
            volumes = [float(c["volume"]) for c in candles[-(period + 1):]]
            last_volume = volumes[-1]
            avg_volume = sum(volumes[:-1]) / period
            if avg_volume > 0 and last_volume <= multiplier * avg_volume:
                logging.info(
                    "entry_filter BLOCKED: volume %.2f <= %.1f * avg %.2f (no spike)",
                    last_volume, multiplier, avg_volume,
                )
                return False

    return True


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_signal(
    candles: list[dict],
    cfg: dict,
    book_data: dict | None = None,
    is_last_candle_open: bool = False,
) -> str | None:
    """Generate a trading signal using MACD + RSI + order book confirmation.

    Requires:
      - MACD crossover (primary, mandatory)
      - At least one of: RSI not at extreme OR book imbalance confirms direction

    When is_last_candle_open=True (live WebSocket candles), the crossover must
    have occurred on the completed candles, and the open candle must simply
    confirm (not reverse) the trend. This prevents false signals from a "twitching" open candle.

    Args:
        candles:              List of OHLCV dicts (Binance 1m candles).
        cfg:                  Bot configuration dict.
        book_data:            Optional dict from order-book fetcher (for depth).
        is_last_candle_open:  True when the last candle is still forming (live WS feed).

    Returns 'BUY_YES' (→ LONG), 'BUY_NO' (→ SHORT), or None.
    The caller (main.py) maps BUY_YES→LONG, BUY_NO→SHORT before calling execution.
    """
    if len(candles) < 20:
        return None

    df = pd.DataFrame(candles)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    df["close"] = pd.to_numeric(df["close"])

    # === ИСПРАВЛЕННЫЙ БЛОК ЛОГИКИ СИГНАЛОВ ===
    if is_last_candle_open and len(candles) >= 21:
        # 1. Ищем полноценное пересечение только на закрытых свечах (защита от дергания)
        df_completed = df.iloc[:-1].copy()
        macd_signal = _compute_macd_signal(df_completed, cfg)
        
        if macd_signal is None:
            return None
            
        # 2. Проверяем, что текущая (открытая) свеча не сломала этот сигнал
        fast, slow, smooth = _get_macd_params(cfg)
        macd_result = ta.macd(df["close"], fast=fast, slow=slow, signal=smooth)
        macd_col = f"MACD_{fast}_{slow}_{smooth}"
        sig_col = f"MACDs_{fast}_{slow}_{smooth}"
        
        curr_macd = macd_result[macd_col].iloc[-1]
        curr_sig = macd_result[sig_col].iloc[-1]
        
        # Если линия MACD провалилась обратно за сигнальную — отменяем вход
        if macd_signal == "BUY_YES" and curr_macd <= curr_sig:
            logging.info("Signal BUY_YES reversed on open candle — skipping")
            return None
        if macd_signal == "BUY_NO" and curr_macd >= curr_sig:
            logging.info("Signal BUY_NO reversed on open candle — skipping")
            return None
    else:
        # Стандартная проверка (если фид не использует WS или свеча только что закрылась)
        macd_signal = _compute_macd_signal(df, cfg)
        if macd_signal is None:
            return None
    # ==========================================

    rsi_ok  = _compute_rsi_confirmation(df, cfg, macd_signal)
    book_available = book_data is not None  # BUG FIX: track whether book fetch succeeded
    
    if not book_available:
        logging.warning("Order book unavailable — falling back to RSI-only confirmation")  # BUG FIX: warn on silent degradation
        
    book_ok = _compute_book_confirmation(book_data, cfg, macd_signal)

    # MACD + at least one confirmation (RSI or book imbalance).
    # Requiring both kills too many valid signals in thin or one-sided books.
    if book_available:
        if not (rsi_ok or book_ok):
            return None
    else:
        if not rsi_ok:
            return None

    # Entry filters — optional last-layer gate
    if not _apply_entry_filters(cfg, macd_signal, book_data, candles):
        return None

    return macd_signal


def get_macd_state(candles: list[dict], cfg: dict) -> dict:
    """Return current MACD values for the status line display.

    Returns dict with keys: macd, signal, diff, source_len.
    All float values are None if calculation fails.
    """
    if len(candles) < 20:
        return {"macd": None, "signal": None, "diff": None, "source_len": len(candles)}

    fast, slow, smooth = _get_macd_params(cfg)

    try:
        df = pd.DataFrame(candles)
        df["close"] = pd.to_numeric(df["close"])
        result = ta.macd(df["close"], fast=fast, slow=slow, signal=smooth)

        if result is None:
            return {"macd": None, "signal": None, "diff": None, "source_len": len(candles)}
        
        m = result[f"MACD_{fast}_{slow}_{smooth}"].iloc[-1]
        s = result[f"MACDs_{fast}_{slow}_{smooth}"].iloc[-1]
        return {
            "macd":       round(float(m), 6) if pd.notna(m) else None,
            "signal":     round(float(s), 6) if pd.notna(s) else None,
            "diff":       round(float(m - s), 6) if pd.notna(m) and pd.notna(s) else None,
            "source_len": len(candles),
        }
    except Exception:
        return {"macd": None, "signal": None, "diff": None, "source_len": len(candles)}
