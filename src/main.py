"""
main.py — Crypto Futures Paper Trading Bot entry point (async).

Single-symbol loop (e.g. BTCUSDT on Binance Futures). No market discovery, no
expiry, no binary resolution — we trade a continuously-available perpetual.

Layout of each iteration:
  1. Load state, reset daily PnL, apply prop-firm halt guard
  2. Fetch candles + order book (WS preferred, REST fallback)
  3. Update trailing stop on any active position
  4. Time-stop / max-hold / reverse-close checks
  5. Hard + Soft SL/TP checks
  6. If no position: signal gen → position sizing → open position

LONG/SHORT mapping from the strategy:
  BUY_YES  → LONG
  BUY_NO   → SHORT
"""

import asyncio
import logging
import argparse
import sys
from datetime import datetime, timezone
import os

import yaml
import httpx

from fetcher import (
    fetch_binance_futures_klines_async,
    fetch_binance_futures_book_async,
    fetch_funding_rate_async,
    BinanceFuturesTradesFeed,
    BinanceFuturesBookFeed,
)
from strategy import generate_signal, get_macd_state
from risk import (
    check_sl_tp,
    should_open_trade,
    calculate_position_size,
    update_halt_if_needed,
    calculate_atr,
    normalize_atr,
    update_trailing_stop,
    compute_dynamic_sl_tp,
    get_exit_price,
    atr_below_minimum,
)
from execution import (
    load_state,
    save_state,
    reset_daily_pnl_if_needed,
    open_position,
    close_position,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")

# Check WebSocket library once at import time
try:
    import websockets as _websockets_probe  # noqa: F401
    _WS_AVAILABLE = True
except ImportError:
    _WS_AVAILABLE = False


# ── ANSI colours ──────────────────────────────────────────────────────────────
_G  = "\033[32m"
_R  = "\033[31m"
_Y  = "\033[33m"
_C  = "\033[36m"
_W  = "\033[37m"
_B  = "\033[1m"
_RS = "\033[0m"


def _bar(char: str = "─", width: int = 52) -> str:
    return char * width


# ── Display helpers ───────────────────────────────────────────────────────────

def _print_open(
    side: str, entry: float, size: float, balance: float,
    symbol: str, fill_pct: float, sl_pct: float, tp_pct: float,
    funding_bps: float | None = None,
) -> None:
    side_label = (
        f"{_G}▲  LONG{_RS}" if side == "LONG"
        else f"{_R}▼  SHORT{_RS}"
    )
    fill_str = f"  {_Y}fill {fill_pct*100:.0f}%{_RS}" if fill_pct < 0.999 else ""
    print(f"\n{_B}{_C}{_bar('═')}{_RS}")
    print(f"  {_B}POSITION OPENED{_RS}  {side_label}{fill_str}")
    print(f"{_C}{_bar()}{_RS}")
    print(f"  Symbol  {_W}{symbol}{_RS}")
    print(f"  Entry   {_B}${entry:,.2f}{_RS}   Size  {_B}${size:.2f}{_RS}  (notional)")
    print(f"  SL      {_R}{sl_pct*100:.2f}%{_RS}   TP  {_G}{tp_pct*100:.2f}%{_RS}  (dynamic)")
    if funding_bps is not None:
        funding_col = _Y if abs(funding_bps) >= 10 else _W
        print(f"  Funding {funding_col}{funding_bps:+.1f} bps{_RS}")
    print(f"  Balance {_B}${balance:.2f}{_RS}  after margin deduction")
    print(f"{_C}{_bar('═')}{_RS}\n")


def _print_close(
    trigger: str, entry: float, exit_price: float,
    pnl: float, balance: float, is_trailing: bool = False,
    label_suffix: str = "",
) -> None:
    is_win = pnl > 0
    colour = _G if is_win else _R
    icon   = "✔" if is_win else "✘"
    if trigger == "TP":
        label = "TAKE PROFIT"
    elif trigger == "SL":
        label = "STOP LOSS (trailing)" if is_trailing else "STOP LOSS"
    elif trigger == "TIME_STOP":
        label = "TIME STOP"
    elif trigger == "REVERSE_CLOSE":
        label = "REVERSE CLOSE"
    elif trigger == "MAX_HOLD":
        label = "MAX HOLD EXCEEDED"
    else:
        label = trigger
    if label_suffix:
        label = f"{label} {label_suffix}"
    pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"

    print(f"\n{_B}{colour}{_bar('═')}{_RS}")
    print(f"  {icon}  {_B}{colour}{label}{_RS}  {'WIN' if is_win else 'LOSS'}")
    print(f"{colour}{_bar()}{_RS}")
    print(f"  Entry   {_W}${entry:,.2f}{_RS}  →  Exit  {_B}${exit_price:,.2f}{_RS}")
    print(f"  PnL     {_B}{colour}{pnl_str}{_RS}")
    print(f"  Balance {_B}${balance:.2f}{_RS}")
    print(f"{colour}{_bar('═')}{_RS}\n")


def _print_status(
    symbol: str, ask: float, bid: float, balance: float, cycle: int,
    macd_state: dict | None = None, source: str = "?",
    can_trade: bool = True, book_imbalance: float | None = None,
    trailing_stop: float | None = None,
    ws_diag: dict | None = None,
) -> None:
    spread = ask - bid

    if macd_state and macd_state["diff"] is not None:
        diff = macd_state["diff"]
        arrow = f"{_G}▲{_RS}" if diff > 0 else f"{_R}▼{_RS}"
        macd_s = f"  macd {arrow}{abs(diff):.3f}"
    else:
        macd_s = f"  macd {_W}--{_RS}"

    trade_s = "" if can_trade else f"  {_Y}wait{_RS}"

    imb_s = ""
    if book_imbalance is not None:
        imb_col = _G if book_imbalance > 0.55 else (_R if book_imbalance < 0.45 else _W)
        imb_s = f"  imb {imb_col}{book_imbalance:.2f}{_RS}"

    trail_s = ""
    if trailing_stop is not None:
        trail_s = f"  trail {_Y}${trailing_stop:,.2f}{_RS}"

    ws_s = ""
    if ws_diag is not None:
        tc = ws_diag["trade_count"]
        cc = ws_diag["candle_closes"]
        if not ws_diag["ws_connected"] or ws_diag["stale_sec"] > 60:
            ws_s = f"  {_R}WS-STALE{_RS}({ws_diag['stale_sec']:.0f}s)"
        elif tc > 0:
            ws_s = f"  ws:{tc}t/{cc}c"

    line = (
        f"  [{cycle:>4}]  {_W}{symbol}{_RS}"
        f"  ask {_B}${ask:,.2f}{_RS}  bid {_B}${bid:,.2f}{_RS}"
        f"  spread ${spread:.2f}"
        f"  src {_W}{source}{_RS}"
        f"{macd_s}{imb_s}{trail_s}{trade_s}{ws_s}"
        f"  bal ${balance:.2f}"
    )
    print(f"\r{line}          ", end="", flush=True)


def _print_status_newline() -> None:
    """Break the live status line before printing an important event."""
    print()


# ── Config ────────────────────────────────────────────────────────────────────

def load_config(path=None) -> dict:
    resolved = os.path.abspath(
        path if path else os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    )
    with open(resolved, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Critical drawdown alert ───────────────────────────────────────────────────

def _notify_telegram_drawdown(cfg: dict, balance: float) -> None:
    """Send a kill-switch alert to Telegram when balance drops below 5% of initial."""
    token   = cfg.get("endpoints", {}).get("telegram_bot_token")
    chat_id = cfg.get("endpoints", {}).get("telegram_chat_id")
    strategy_name = cfg.get("strategy", {}).get("name", "Unknown Bot")
    if not (token and chat_id):
        return
    message = (
        f"🚨 KILL-SWITCH: Bot [{strategy_name}] lost more than 95% of deposit. "
        f"Remaining: ${balance:.2f}. Script halted."
    )
    try:
        with httpx.Client(timeout=10.0) as client:
            client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message},
            )
    except Exception as exc:
        log.warning("Telegram notification failed: %s", exc)


# ── Funding-rate guard ────────────────────────────────────────────────────────

def _funding_blocks_entry(funding_info: dict | None, side: str, cfg: dict) -> bool:
    """Return True if funding rate blocks opening a new position on `side`.

    LONGs pay shorts when funding > 0 (positive funding).
    A high positive funding rate means LONGs are penalized → skip LONG entries.
    A very negative funding rate means SHORTs are penalized → skip SHORT entries.

    Config: risk_management.max_funding_rate_bps (default 50 = 0.5% per 8h).
    """
    if funding_info is None:
        return False  # No data — fail open, do not block
    max_bps = float(cfg.get("risk_management", {}).get("max_funding_rate_bps", 0))
    if max_bps <= 0:
        return False  # Guard disabled
    bps = float(funding_info.get("funding_rate_bps", 0.0))
    if side == "LONG" and bps > max_bps:
        return True
    if side == "SHORT" and bps < -max_bps:
        return True
    return False


# ── Max-hold-time guard ───────────────────────────────────────────────────────

def _exceeds_max_hold(position: dict, cfg: dict) -> float | None:
    """Return age_sec if position age exceeds max_hold_time_minutes, else None."""
    max_minutes = float(cfg.get("risk_management", {}).get("max_hold_time_minutes", 0))
    if max_minutes <= 0:
        return None
    pos_ts = position.get("timestamp")
    if not pos_ts:
        return None
    pos_dt = datetime.fromisoformat(pos_ts)
    if pos_dt.tzinfo is None:
        pos_dt = pos_dt.replace(tzinfo=timezone.utc)
    age_sec = (datetime.now(timezone.utc) - pos_dt).total_seconds()
    return age_sec if age_sec > max_minutes * 60 else None


# ── Main async loop ───────────────────────────────────────────────────────────

async def run_loop(cfg: dict) -> None:
    poll_interval = cfg.get("market", {}).get("polling_interval_seconds", 10)
    symbol        = cfg.get("exchange", {}).get("symbol", "BTCUSDT").upper()

    rm_cfg = cfg.get("risk_management", {})
    sl_mult   = rm_cfg.get("atr_sl_multiplier", 0)
    tp_mult   = rm_cfg.get("atr_tp_multiplier", 0)
    max_loss  = rm_cfg.get("max_daily_loss_pct", 0) * 100
    risk_pct  = rm_cfg.get("risk_per_trade_pct", rm_cfg.get("position_size_pct", 0)) * 100
    leverage  = cfg.get("exchange", {}).get("leverage", 1)

    atr_mode   = "ATR-dynamic" if rm_cfg.get("use_atr_dynamic", True) else "fixed"
    trail_mode = f"trailing {'✓' if rm_cfg.get('trailing_stop_enabled', True) else '✗'}"

    print(f"\n{_B}{_C}{_bar('═')}{_RS}")
    print(f"  {_B}CRYPTO FUTURES PAPER TRADING BOT  [async]{_RS}")
    print(f"{_C}{_bar()}{_RS}")
    print(f"  Symbol  {_B}{symbol}{_RS}   Leverage  {_B}{leverage}x{_RS}")
    print(
        f"  SL ATRx{sl_mult}  TP ATRx{tp_mult}  "
        f"Max daily loss {max_loss:.0f}%  "
        f"Risk/trade {risk_pct:.1f}%"
    )
    print(f"  Risk: {_C}{atr_mode}{_RS}  {_C}{trail_mode}{_RS}")
    print(f"{_C}{_bar('═')}{_RS}\n")

    # WS feeds: book + tick-based candles
    use_ws = cfg.get("trading", {}).get("use_ws", True)
    if use_ws and not _WS_AVAILABLE:
        log.warning("websockets not installed — book feed will use HTTP fallback")
        use_ws = False
    book_feed: BinanceFuturesBookFeed | None = BinanceFuturesBookFeed() if use_ws else None
    if book_feed is not None:
        await book_feed.start(cfg)
        print(f"  {_C}◉  Depth WS{_RS}  {symbol}@depth20@100ms")

    use_binance_ws = cfg.get("trading", {}).get("use_binance_ws", False)
    if use_binance_ws and not _WS_AVAILABLE:
        log.warning("websockets not installed — trades WS disabled")
        use_binance_ws = False
    trades_feed: BinanceFuturesTradesFeed | None = None
    if use_binance_ws:
        trades_feed = BinanceFuturesTradesFeed()
        await trades_feed.start(cfg)
        print(f"  {_C}◉  Trades WS{_RS}  {symbol}@aggTrade candle builder started\n")

    cycle = 0
    while True:
        cycle += 1
        try:
            await _iteration(
                cfg, cycle,
                book_feed=book_feed,
                trades_feed=trades_feed,
                use_ws=use_ws,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            _print_status_newline()
            log.warning("Iteration %d failed: %s", cycle, exc)

        await asyncio.sleep(poll_interval)


# ── Single iteration ──────────────────────────────────────────────────────────

async def _iteration(
    cfg: dict, cycle: int,
    book_feed: BinanceFuturesBookFeed | None = None,
    trades_feed: BinanceFuturesTradesFeed | None = None,
    use_ws: bool = False,
) -> None:
    symbol = cfg.get("exchange", {}).get("symbol", "BTCUSDT").upper()

    # ── 1. STATE + kill-switch ───────────────────────────────────────────────
    state = load_state(cfg)

    initial = float(cfg.get("risk_management", {}).get("initial_balance_usd", 1000.0))
    current = state["virtual_portfolio"].get("balance_usd", 0.0)
    if current < initial * 0.05:
        _print_status_newline()
        log.error(
            "\033[1m\033[31mCRITICAL: More than 95%% of deposit lost "
            "(remaining $%.2f). Halting.\033[0m",
            current,
        )
        _notify_telegram_drawdown(cfg, current)
        sys.exit(0)

    state = reset_daily_pnl_if_needed(state)
    portfolio = state["virtual_portfolio"]
    portfolio = update_halt_if_needed(portfolio, cfg)
    state["virtual_portfolio"] = portfolio

    pos = portfolio.get("active_position")

    # ── 2. DATA FETCH (WS → REST fallback) ───────────────────────────────────
    ws_extremums = None
    use_live_candles = False
    try:
        # Candles: prefer live WS feed, but fall back to REST if WS is stale
        if trades_feed is not None and trades_feed.is_ready():
            if trades_feed.is_stale():
                diag = trades_feed.get_diagnostics()
                if cycle % 6 == 0:  # log every ~60s, not every tick
                    _print_status_newline()
                    log.warning(
                        "TradesFeed stale (no trades for %.0fs, ws_connected=%s, "
                        "total_trades=%d, candle_closes=%d) — refreshing from REST",
                        diag["stale_sec"], diag["ws_connected"],
                        diag["trade_count"], diag["candle_closes"],
                    )
                await trades_feed.refresh_from_rest()
            candles = trades_feed.get_candles()
            use_live_candles = True
            candle_source = "bnb-ws"
        else:
            candles = await fetch_binance_futures_klines_async(cfg)
            candle_source = "bnb-rest"

        # Book: prefer WS snapshot
        if use_ws and book_feed is not None:
            ws_book = book_feed.get_latest()
            if ws_book is not None:
                ws_extremums = (
                    book_feed.get_and_reset_extremums() if pos is not None else None
                )
                book = ws_book
                book_source = "ws"
            else:
                book = await fetch_binance_futures_book_async(cfg)
                book_source = "rest-fallback"
        else:
            book = await fetch_binance_futures_book_async(cfg)
            book_source = "rest"
    except httpx.HTTPStatusError as exc:
        _print_status_newline()
        log.warning("REST fetch failed: HTTP %s %s", exc.response.status_code, exc)
        return
    except Exception as exc:
        _print_status_newline()
        log.warning("Data fetch failed: %s", exc)
        return

    best_ask = book["best_ask"]
    best_bid = book["best_bid"]
    book_imbalance = book.get("book_imbalance")

    # ── 3. ATR (raw USD for futures; risk.py uses atr_pct = atr_raw/price) ──
    atr_period = cfg.get("risk_management", {}).get("atr_period", 14)
    atr_raw    = calculate_atr(candles, period=atr_period)
    last_close = float(candles[-1]["close"]) if candles else None
    atr_normalized = normalize_atr(atr_raw, last_close, cfg) if atr_raw else None

    # ── 4. STATUS LINE ───────────────────────────────────────────────────────
    macd_state = get_macd_state(candles, cfg)
    can_trade  = should_open_trade(portfolio, cfg=cfg)
    trailing   = pos.get("trailing_stop_price") if pos else None
    ws_diag = trades_feed.get_diagnostics() if trades_feed is not None else None
    _print_status(
        symbol, best_ask, best_bid,
        portfolio["balance_usd"], cycle,
        macd_state=macd_state,
        source=f"{candle_source}{len(candles)}+{book_source}",
        can_trade=can_trade,
        book_imbalance=book_imbalance,
        trailing_stop=trailing,
        ws_diag=ws_diag,
    )

    if portfolio.get("trading_halted_until"):
        _print_status_newline()
        log.warning("Trading halted until %s", portfolio["trading_halted_until"])

    # ── 5. TRAILING STOP UPDATE ──────────────────────────────────────────────
    if pos is not None:
        # NOTE: risk.update_trailing_stop expects raw ATR in price units (USD).
        # normalize_atr returns atr_raw/price (a fraction). Pass atr_raw instead.
        portfolio["active_position"] = update_trailing_stop(
            pos, best_bid, best_ask, atr_raw, cfg,
            ws_extremums=ws_extremums,
        )
        state["virtual_portfolio"] = portfolio
        pos = portfolio["active_position"]  # refresh after update

    # ── 6a. MAX HOLD TIME CHECK ──────────────────────────────────────────────
    risk_cfg_iter = cfg.get("risk_management", {})
    if pos is not None:
        age_over = _exceeds_max_hold(pos, cfg)
        if age_over is not None:
            exit_p = get_exit_price(pos, best_bid, best_ask)
            state = close_position(state, exit_p, "MAX_HOLD", cfg, book_data=book)
            trade = state["trade_history"][-1]
            _print_status_newline()
            _print_close(
                "MAX_HOLD",
                pos["entry_price"],
                trade["exit_price"],
                trade["pnl"],
                state["virtual_portfolio"]["balance_usd"],
                label_suffix=f"({int(age_over)}s held)",
            )
            save_state(state, cfg)
            return

    # ── 6b. TIME-STOP CHECK (dead-zone exit for stale positions) ─────────────
    if pos is not None and risk_cfg_iter.get("use_time_stop", False):
        time_stop_sec      = float(risk_cfg_iter.get("time_stop_seconds", 120))
        time_stop_max_loss = float(risk_cfg_iter.get("time_stop_max_loss_pct", 0.02))
        tp_pct_pos = float(pos.get("tp_pct", risk_cfg_iter.get("take_profit_pct", 0.10)))

        pos_ts = pos.get("timestamp")
        if pos_ts:
            pos_dt = datetime.fromisoformat(pos_ts)
            if pos_dt.tzinfo is None:
                pos_dt = pos_dt.replace(tzinfo=timezone.utc)
            age_sec = (datetime.now(timezone.utc) - pos_dt).total_seconds()

            if age_sec > time_stop_sec:
                exit_p  = get_exit_price(pos, best_bid, best_ask)
                entry_p = pos["entry_price"]
                # Direction-aware PnL% for futures
                if pos["side"] == "LONG":
                    pnl_pct = (exit_p - entry_p) / entry_p if entry_p > 0 else 0.0
                else:
                    pnl_pct = (entry_p - exit_p) / entry_p if entry_p > 0 else 0.0

                if -time_stop_max_loss <= pnl_pct < tp_pct_pos:
                    state = close_position(state, exit_p, "TIME_STOP", cfg, book_data=book)
                    trade = state["trade_history"][-1]
                    _print_status_newline()
                    _print_close(
                        "TIME_STOP",
                        pos["entry_price"],
                        trade["exit_price"],
                        trade["pnl"],
                        state["virtual_portfolio"]["balance_usd"],
                        label_suffix=f"({int(age_sec)}s held)",
                    )
                    save_state(state, cfg)
                    return

    # ── 6c. REVERSE-CLOSE CHECK ──────────────────────────────────────────────
    if pos is not None and risk_cfg_iter.get("use_reverse_close", False):
        reverse_signal = generate_signal(
            candles, cfg, book_data=book,
            is_last_candle_open=use_live_candles,
        )
        # BUY_YES → bullish (LONG direction); BUY_NO → bearish (SHORT direction)
        pos_side = pos.get("side", "")
        is_reverse = (
            (pos_side == "LONG"  and reverse_signal == "BUY_NO")
            or (pos_side == "SHORT" and reverse_signal == "BUY_YES")
        )
        if is_reverse:
            exit_p = get_exit_price(pos, best_bid, best_ask)
            state  = close_position(state, exit_p, "REVERSE_CLOSE", cfg, book_data=book)
            trade  = state["trade_history"][-1]
            _print_status_newline()
            _print_close(
                "REVERSE_CLOSE",
                pos["entry_price"],
                trade["exit_price"],
                trade["pnl"],
                state["virtual_portfolio"]["balance_usd"],
                label_suffix=f"(signal → {reverse_signal})",
            )
            save_state(state, cfg)
            return

    # ── 7. SL / TP CHECK ─────────────────────────────────────────────────────
    use_sl_tp = cfg.get("risk_management", {}).get("use_sl_tp", True)
    if use_sl_tp and portfolio.get("active_position") is not None:
        pos = portfolio["active_position"]
        side = pos["side"]  # "LONG" or "SHORT"

        # Hard TP: WS extremums may have briefly touched our TP price between polls
        use_hard_tp = risk_cfg_iter.get("use_hard_tp", True)
        if use_hard_tp and ws_extremums is not None:
            tp_pct = pos.get("tp_pct", 0.045)
            if side == "LONG":
                target_tp_price = pos["entry_price"] * (1 + tp_pct)
                hard_tp_hit = ws_extremums["highest_bid"] >= target_tp_price
            else:  # SHORT
                target_tp_price = pos["entry_price"] * (1 - tp_pct)
                hard_tp_hit = (
                    ws_extremums["lowest_ask"] > 0
                    and ws_extremums["lowest_ask"] <= target_tp_price
                )
            if hard_tp_hit:
                state = close_position(
                    state, target_tp_price, "TP", cfg,
                    book_data=book, skip_slippage=True,
                )
                trade = state["trade_history"][-1]
                _print_status_newline()
                _print_close(
                    "TP",
                    pos["entry_price"],
                    trade["exit_price"],
                    trade["pnl"],
                    state["virtual_portfolio"]["balance_usd"],
                    label_suffix="(Hard TP)",
                )
                save_state(state, cfg)
                return

        # SL with optional confirmation delay
        sl_confirm_sec = float(risk_cfg_iter.get("sl_confirm_seconds", 0))

        # Hard SL: WS extremums may have briefly touched our SL price between polls
        hard_sl_hit = False
        target_sl_price = None
        use_hard_sl = risk_cfg_iter.get("use_hard_sl", True)
        if use_hard_sl and ws_extremums is not None:
            sl_pct = pos.get("sl_pct", 0.03)
            if side == "LONG":
                target_sl_price = pos["entry_price"] * (1 - sl_pct)
                hard_sl_hit = (
                    ws_extremums["lowest_bid"] > 0
                    and ws_extremums["lowest_bid"] <= target_sl_price
                )
            else:  # SHORT
                target_sl_price = pos["entry_price"] * (1 + sl_pct)
                hard_sl_hit = ws_extremums["highest_ask"] >= target_sl_price

        # Soft SL/TP from check_sl_tp (direction-aware in risk.py)
        trigger = check_sl_tp(portfolio, best_bid, best_ask, cfg)

        # Soft TP fires immediately — no confirmation delay
        if trigger == "TP":
            pos = portfolio["active_position"]
            exit_p = get_exit_price(pos, best_bid, best_ask)
            state  = close_position(state, exit_p, "TP", cfg, book_data=book)
            trade  = state["trade_history"][-1]
            _print_status_newline()
            _print_close(
                "TP",
                pos["entry_price"],
                trade["exit_price"],
                trade["pnl"],
                state["virtual_portfolio"]["balance_usd"],
            )
            save_state(state, cfg)
            return

        soft_sl_hit = (trigger == "SL")
        sl_hit = hard_sl_hit or soft_sl_hit

        # Confirmation delay — avoid SL triggers from a single wick
        if sl_hit and sl_confirm_sec > 0:
            now = datetime.now(timezone.utc)
            breach_since = pos.get("sl_breach_since")
            if not breach_since:
                pos["sl_breach_since"] = now.isoformat()
                state["virtual_portfolio"]["active_position"] = pos
                _print_status_newline()
                log.info("SL breach detected — confirming for %ds...", int(sl_confirm_sec))
                save_state(state, cfg)
                return
            breach_dt = datetime.fromisoformat(breach_since)
            if breach_dt.tzinfo is None:
                breach_dt = breach_dt.replace(tzinfo=timezone.utc)
            elapsed = (now - breach_dt).total_seconds()
            if elapsed < sl_confirm_sec:
                log.info("SL confirming... %.0fs / %ds", elapsed, int(sl_confirm_sec))
                save_state(state, cfg)
                return
            log.info("SL breach confirmed after %.0fs", elapsed)

        if sl_hit:
            if hard_sl_hit:
                state = close_position(
                    state, target_sl_price, "SL", cfg,
                    book_data=book, skip_slippage=True,
                )
                trade = state["trade_history"][-1]
                _print_status_newline()
                _print_close(
                    "SL",
                    pos["entry_price"],
                    trade["exit_price"],
                    trade["pnl"],
                    state["virtual_portfolio"]["balance_usd"],
                    label_suffix="(Hard SL)",
                )
            else:
                pos = portfolio["active_position"]
                exit_p = get_exit_price(pos, best_bid, best_ask)
                state  = close_position(state, exit_p, "SL", cfg, book_data=book)
                trade  = state["trade_history"][-1]
                # Was it the trailing stop?
                is_trailing = False
                trail_price = pos.get("trailing_stop_price")
                if trail_price is not None:
                    if pos["side"] == "LONG":
                        is_trailing = exit_p <= trail_price
                    else:
                        is_trailing = exit_p >= trail_price
                _print_status_newline()
                _print_close(
                    "SL",
                    pos["entry_price"],
                    trade["exit_price"],
                    trade["pnl"],
                    state["virtual_portfolio"]["balance_usd"],
                    is_trailing=is_trailing,
                )
            save_state(state, cfg)
            return

        # No SL breach — clear confirmation timer if it was set
        if pos.get("sl_breach_since"):
            log.info("SL breach cleared — price recovered")
            pos.pop("sl_breach_since", None)
            state["virtual_portfolio"]["active_position"] = pos

    # ── 8. ENTRY LOGIC ───────────────────────────────────────────────────────
    if not should_open_trade(portfolio, cfg=cfg):
        save_state(state, cfg)
        return

    # Фикс A: skip trading when ATR is below noise threshold
    if atr_below_minimum(atr_normalized, cfg):
        if cycle % 30 == 0:
            _print_status_newline()
            log.info(
                "DIAG: ATR below min (atr=%.6f min=%.4f) -- no signal check",
                atr_normalized or 0,
                float(cfg.get("risk_management", {}).get("min_atr_pct", 0)),
            )
        save_state(state, cfg)
        return

    signal = generate_signal(
        candles, cfg, book_data=book,
        is_last_candle_open=use_live_candles,
    )

    if cycle % 30 == 0:
        _print_status_newline()
        log.info(
            "DIAG cycle %d: signal=%s atr=%.6f macd=%.3f candles=%d live=%s",
            cycle, signal, atr_normalized or 0,
            macd_state.get("diff", 0) or 0, len(candles), use_live_candles,
        )

    if signal is None:
        save_state(state, cfg)
        return

    # Map strategy signal → futures side
    side = "LONG" if signal == "BUY_YES" else "SHORT"

    # Futures entry price: LONG buys at ask, SHORT sells at bid
    entry_price = best_ask if side == "LONG" else best_bid

    # Compute dynamic SL/TP from ATR before sizing (sl_pct is needed for risk-based sizing)
    sl_pct, tp_pct = compute_dynamic_sl_tp(atr_normalized, cfg)

    # Spread viability check
    spread = best_ask - best_bid
    if entry_price > 0 and spread > 0:
        spread_cost_pct = spread / entry_price
        if spread_cost_pct >= sl_pct * 0.75:
            _print_status_newline()
            log.info(
                "Spread too wide for SL: %.3f%% >= 75%% of SL %.3f%% — skipping",
                spread_cost_pct * 100, sl_pct * 100,
            )
            save_state(state, cfg)
            return

    # Funding-rate guard — avoid opening positions into a punishing funding cycle
    funding_info = await fetch_funding_rate_async(cfg)
    if _funding_blocks_entry(funding_info, side, cfg):
        _print_status_newline()
        log.info(
            "Funding guard: %s blocked (rate %.2f bps exceeds limit)",
            side, funding_info["funding_rate_bps"],
        )
        save_state(state, cfg)
        return

    # Risk-based position sizing (uses sl_pct so loss == balance * risk_per_trade_pct)
    size_usd = calculate_position_size(
        portfolio, cfg, entry_price=entry_price, sl_pct=sl_pct,
    )

    if size_usd < 1.0:
        _print_status_newline()
        log.critical("Position size too low ($%.2f). Halting.", size_usd)
        raise SystemExit(1)

    # Depth-warning: size vs. visible liquidity
    relevant_vol_key = "ask_volume" if side == "LONG" else "bid_volume"
    relevant_volume = book.get(relevant_vol_key, 0)
    if relevant_volume > 0:
        available_usd = relevant_volume * entry_price
        if size_usd > available_usd * 0.5:
            _print_status_newline()
            log.warning(
                "Size $%.2f exceeds 50%% of visible %s liquidity ($%.2f). "
                "Real execution would suffer severe slippage.",
                size_usd, relevant_vol_key, available_usd,
            )

    state = open_position(
        state, side, entry_price, size_usd, symbol, cfg,
        book_data=book, sl_pct=sl_pct, tp_pct=tp_pct,
    )

    # Sanity check — if open_position didn't create a position for any reason, bail out
    new_pos = state["virtual_portfolio"].get("active_position")
    if new_pos is None:
        save_state(state, cfg)
        return

    # Reset WS extremums so the next iteration tracks price movement only from NOW
    if use_ws and book_feed is not None:
        book_feed.get_and_reset_extremums()
    save_state(state, cfg)

    _print_status_newline()
    _print_open(
        side,
        new_pos["entry_price"],
        new_pos["size_usd"],
        state["virtual_portfolio"]["balance_usd"],
        symbol,
        new_pos.get("fill_pct", 1.0),
        new_pos.get("sl_pct", sl_pct),
        new_pos.get("tp_pct", tp_pct),
        funding_bps=(funding_info["funding_rate_bps"] if funding_info else None),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Futures Paper Trading Bot")
    parser.add_argument("--config", "-c", default=None,
                        help="Path to config YAML (default: ../config.yaml)")
    args = parser.parse_args()
    cfg = load_config(args.config)
    asyncio.run(run_loop(cfg))


if __name__ == "__main__":
    main()
