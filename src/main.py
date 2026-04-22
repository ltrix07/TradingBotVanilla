"""
main.py — Polymarket Paper Trading Bot entry point (async).

Key improvements over v1:
  - Full asyncio event loop: Binance + Polymarket fetched in parallel via asyncio.gather().
  - BUG FIX: When a position is open, the bot locks onto that position's market for
    SL/TP checks and expiry monitoring. Market discovery is SKIPPED while a position
    is active — the bot no longer drifts to a new market and "lose" the open trade.
  - ATR-based dynamic SL/TP: thresholds adapt to BTC volatility each cycle.
  - Trailing stop: ratchets upward as the position gains value.
  - Order book imbalance and RSI confirmation shown in status line.
"""

import asyncio
import logging
import argparse
import random
import sys
from datetime import datetime, timezone
import yaml
import os

import httpx

from fetcher import (
    find_active_market_id_async,
    fetch_binance_klines_async,
    fetch_polymarket_book_async,
    fetch_last_trade_price_async,
    PolymarketBookFeed,
    BinanceTradesFeed,
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

# WS: check websockets library availability once at import time
try:
    import websockets as _websockets_probe  # noqa: F401
    _WS_AVAILABLE = True
except ImportError:
    _WS_AVAILABLE = False

# WS: tracks which token the feed is currently subscribed to
_ws_active_token: str | None = None


class _MarketUnavailableError(Exception):
    """Raised when the CLOB returns 404 for a discovered market token.
    Signals run_loop to discard the current market_info and re-run discovery.
    """


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
    expires_in: float, market_slug: str,
    fill_pct: float, sl_pct: float, tp_pct: float,
    market_open_price: float | None = None,
    current_btc: float | None = None,
) -> None:
    side_label = f"{_G}▲  LONG  YES{_RS}" if side == "YES" else f"{_R}▼  SHORT  NO{_RS}"
    fill_str   = f"  {_Y}fill {fill_pct*100:.0f}%{_RS}" if fill_pct < 0.999 else ""
    print(f"\n{_B}{_C}{_bar('═')}{_RS}")
    print(f"  {_B}POSITION OPENED{_RS}  {side_label}{fill_str}")
    print(f"{_C}{_bar()}{_RS}")
    print(f"  Market  {_W}{market_slug}{_RS}")
    print(f"  Entry   {_B}{entry:.4f}{_RS}   Size  {_B}${size:.2f}{_RS}")
    if market_open_price is not None and current_btc is not None:
        delta = current_btc - market_open_price
        delta_col = _G if delta > 0 else _R
        print(
            f"  BTC     {_B}${current_btc:,.2f}{_RS}  vs open  "
            f"{_B}${market_open_price:,.2f}{_RS}  ({delta_col}{delta:+.2f}{_RS})"
        )
    print(f"  SL      {_R}{sl_pct*100:.1f}%{_RS}   TP  {_G}{tp_pct*100:.1f}%{_RS}  (dynamic)")
    print(f"  Balance {_B}${balance:.2f}{_RS}  after deduction")
    print(f"  Expires in {_B}{expires_in:.0f}s{_RS}")
    print(f"{_C}{_bar('═')}{_RS}\n")


def _print_close(
    trigger: str, entry: float, exit_price: float,
    pnl: float, balance: float, is_trailing: bool = False,
    label_suffix: str = "",
) -> None:
    is_win  = trigger == "TP" or pnl > 0
    colour  = _G if is_win else _R
    icon    = "✔" if is_win else "✘"
    if trigger == "TP":
        label = "TAKE PROFIT"
    elif trigger == "SL":
        label = "STOP LOSS (trailing)" if is_trailing else "STOP LOSS"
    elif trigger == "TIME_STOP":
        label = "TIME STOP"
    elif trigger == "REVERSE_CLOSE":
        label = "REVERSE CLOSE"
    else:
        label = "EXPIRED"
    if label_suffix:
        label = f"{label} {label_suffix}"
    pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"

    print(f"\n{_B}{colour}{_bar('═')}{_RS}")
    print(f"  {icon}  {_B}{colour}{label}{_RS}  {'WIN' if is_win else 'LOSS'}")
    print(f"{colour}{_bar()}{_RS}")
    print(f"  Entry   {_W}{entry:.4f}{_RS}  →  Exit  {_B}{exit_price:.4f}{_RS}")
    print(f"  PnL     {_B}{colour}{pnl_str}{_RS}")
    print(f"  Balance {_B}${balance:.2f}{_RS}")
    print(f"{colour}{_bar('═')}{_RS}\n")


def _print_status(
    token_short: str, ask: float, bid: float,
    expires_in: float, balance: float, cycle: int,
    macd_state: dict | None = None, source: str = "?",
    can_trade: bool = True, book_imbalance: float | None = None,
    trailing_stop: float | None = None,
) -> None:
    spread = ask - bid

    if macd_state and macd_state["diff"] is not None:
        diff   = macd_state["diff"]
        arrow  = f"{_G}▲{_RS}" if diff > 0 else f"{_R}▼{_RS}"
        macd_s = f"  macd {arrow}{abs(diff):.5f}"
    else:
        macd_s = f"  macd {_W}--{_RS}"

    trade_s = "" if can_trade else f"  {_Y}wait{_RS}"

    imb_s = ""
    if book_imbalance is not None:
        imb_col = _G if book_imbalance > 0.55 else (_R if book_imbalance < 0.45 else _W)
        imb_s   = f"  imb {imb_col}{book_imbalance:.2f}{_RS}"

    trail_s = ""
    if trailing_stop is not None:
        trail_s = f"  trail {_Y}{trailing_stop:.4f}{_RS}"

    line = (
        f"  [{cycle:>4}]  {_W}{token_short}…{_RS}"
        f"  ask {_B}{ask:.4f}{_RS}  bid {_B}{bid:.4f}{_RS}"
        f"  spread {spread:.4f}"
        f"  exp {expires_in:.0f}s"
        f"  src {_W}{source}{_RS}"
        f"{macd_s}{imb_s}{trail_s}{trade_s}"
        f"  bal ${balance:.2f}"
    )
    print(f"\r{line}          ", end="", flush=True)


def _print_status_newline() -> None:
    """Зафиксировать статус-строку перед печатью важного события."""
    print()


# ── Config & utilities ────────────────────────────────────────────────────────

def load_config(path=None) -> dict:
    resolved = os.path.abspath(
        path if path else os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    )
    with open(resolved, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _seconds_until_expiry(end_date_iso: str) -> float:
    if not end_date_iso:
        return 0.0
    end_dt = datetime.fromisoformat(end_date_iso)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    return max(0.0, (end_dt - datetime.now(timezone.utc)).total_seconds())


def _get_market_open_btc_price(end_date_iso: str, candles: list[dict]) -> float | None:
    """Calculate BTC price at the moment a 5-minute Polymarket market opened.

    The market open time = end_date_iso - 300 seconds.  We find the 1-minute
    Binance candle that covers that moment and return its close price (best
    approximation of BTC price at market open).

    Returns None when data is insufficient.
    """
    if not end_date_iso or not candles:
        return None
    try:
        end_dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    market_open_ts_ms = int((end_dt.timestamp() - 300) * 1000)

    # Find the most recent candle whose open timestamp <= market_open_ts_ms
    best = None
    for c in candles:
        if c["timestamp"] <= market_open_ts_ms:
            best = c
        else:
            break

    return float(best["close"]) if best is not None else None


async def _refresh_market_async(
    cfg: dict,
    current_token_id: str | None,
    skip_token_ids: set | None = None,
) -> dict | None:
    try:
        market_info = await find_active_market_id_async(cfg, skip_token_ids=skip_token_ids)
        if market_info["token_id"] != current_token_id:
            slug = market_info.get("slug", market_info["token_id"][:20])
            print(
                f"\n  {_C}◉  Market{_RS}  {_B}{slug}{_RS}"
                f"  expires {market_info['end_date_iso']}"
            )
        return market_info
    except Exception as exc:
        log.warning("Discovery failed: %s", exc)
        return None


# ── Main async loop ───────────────────────────────────────────────────────────

async def _reconcile_stale_position_on_startup(cfg: dict) -> None:
    """Закрыть зависшие позиции, чьи рынки уже истекли.

    При перезапуске бота проверяет state.json:
      - Если active_position есть и market_end_date_iso в прошлом → рынок резолвнулся
      - Запрашиваем last_trade_price с Polymarket CLOB
      - На основе цены и side определяем WIN/LOSS и вызываем close_position()

    Вызывается ровно один раз в начале run_loop перед основным циклом.
    Никогда не бросает исключений — все ошибки логируются и позиция остаётся.
    """
    try:
        state = load_state(cfg)
        pos = state["virtual_portfolio"].get("active_position")
        if pos is None:
            return  # Нет зависшей позиции — нечего делать

        market_end_iso = pos.get("market_end_date_iso", "")
        if not market_end_iso:
            log.warning(
                "RECONCILE: active position has no market_end_date_iso — "
                "cannot determine if expired. Leaving as-is."
            )
            return

        # Проверяем что рынок действительно истёк
        try:
            end_dt = datetime.fromisoformat(market_end_iso)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as exc:
            log.warning("RECONCILE: cannot parse market_end_date_iso=%r: %s",
                        market_end_iso, exc)
            return

        now = datetime.now(timezone.utc)
        if end_dt > now:
            # Позиция в ещё-живом рынке — пусть обычный цикл её мониторит
            _print_status_newline()
            log.info(
                "RECONCILE: active position is in still-live market "
                "(%.0fs until expiry). Skipping reconciliation.",
                (end_dt - now).total_seconds(),
            )
            return

        # Рынок истёк — фетчим резолюцию
        market_id = pos.get("market_id", "")
        if not market_id:
            log.error("RECONCILE: expired position has no market_id — cannot resolve.")
            return

        age_min = (now - end_dt).total_seconds() / 60
        _print_status_newline()
        log.info(
            "RECONCILE: found stale position (market expired %.1f min ago) — "
            "fetching resolution...",
            age_min,
        )

        last_price = await fetch_last_trade_price_async(cfg, market_id)
        if last_price is None:
            log.warning(
                "RECONCILE: last_trade_price returned None for market %s. "
                "Position remains open — will retry on next restart.",
                market_id[:16] + "...",
            )
            return

        # Определяем результат (0 = NO won, 1 = YES won; 0.5 = undetermined)
        side = pos.get("side", "YES").upper()

        # Предупреждение если цена в неопределённой зоне
        if 0.2 < last_price < 0.8:
            log.warning(
                "RECONCILE: last_price=%.3f is in ambiguous zone [0.2, 0.8] — "
                "market may not be fully resolved yet. "
                "Using 0.5 threshold anyway.",
                last_price,
            )

        yes_won = last_price >= 0.5

        if side == "YES":
            result = "WIN" if yes_won else "LOSS"
        else:  # NO position
            result = "WIN" if not yes_won else "LOSS"

        exit_price = 1.0 if result == "WIN" else 0.0

        log.info(
            "RECONCILE: side=%s last_price=%.3f → %s (exit_price=%.1f, qty=%.2f)",
            side, last_price, result, exit_price, pos.get("qty", 0),
        )

        # close_position сам обновит state, balance, trade_history и CSV
        state = close_position(
            state, exit_price=exit_price, result=result, cfg=cfg,
        )
        save_state(state, cfg)

        trade = state["trade_history"][-1]
        pnl = trade["pnl"]
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        log.info(
            "RECONCILE: closed. PnL=%s balance=$%.2f",
            pnl_str, state["virtual_portfolio"]["balance_usd"],
        )
        _print_status_newline()

    except Exception as exc:
        # НИКОГДА не прерываем запуск бота из-за reconciliation
        log.exception("RECONCILE: unexpected error (non-fatal): %s", exc)


async def run_loop(cfg: dict) -> None:
    poll_interval   = cfg["market"]["polling_interval_seconds"]
    min_expiry_sec  = cfg["risk_management"]["min_time_before_expiry_sec"]

    atr_mode   = "ATR-dynamic" if cfg["risk_management"].get("use_atr_dynamic", True) else "fixed"
    trail_mode = f"trailing {'✓' if cfg['risk_management'].get('trailing_stop_enabled', True) else '✗'}"

    rm_cfg = cfg.get('risk_management', {})
    
    # Безопасно получаем новые значения (с фолбэком на 0, если вдруг ключа нет)
    sl_mult = rm_cfg.get('atr_sl_multiplier', 0)
    tp_mult = rm_cfg.get('atr_tp_multiplier', 0)
    max_loss = rm_cfg.get('max_daily_loss_pct', 0) * 100
    pos_size = rm_cfg.get('position_size_pct', 0) * 100

    print(f"\n{_B}{_C}{_bar('═')}{_RS}")
    print(f"  {_B}POLYMARKET PAPER TRADING BOT  [async]{_RS}")
    print(f"{_C}{_bar()}{_RS}")
    print(
        f"  SL ATRx{sl_mult}  "      # Теперь показываем множитель ATR вместо жестких %
        f"TP ATRx{tp_mult}  "      # Теперь показываем множитель ATR вместо жестких %
        f"Max daily loss {max_loss:.0f}%  "
        f"Size {pos_size:.0f}%"
    )
    print(f"  Risk: {_C}{atr_mode}{_RS}  {_C}{trail_mode}{_RS}")
    print(f"{_C}{_bar('═')}{_RS}\n")

    _unavailable_tokens: set[str] = set()

    # WS: determine whether this strategy should use the WebSocket book feed
    global _ws_active_token
    use_ws = cfg.get("trading", {}).get("use_ws", True)
    if use_ws and not _WS_AVAILABLE:
        log.warning("websockets not installed — book feed will use HTTP fallback")
        use_ws = False
    # WS: create feed instance (not started yet — token_id unknown until discovery)
    book_feed: PolymarketBookFeed | None = PolymarketBookFeed() if use_ws else None

    # Binance WS: real-time aggTrade candle builder (opt-in via config)
    use_binance_ws = cfg.get("trading", {}).get("use_binance_ws", False)
    if use_binance_ws and not _WS_AVAILABLE:
        log.warning("websockets not installed — Binance WS feed disabled")
        use_binance_ws = False
    binance_feed: BinanceTradesFeed | None = None
    if use_binance_ws:
        binance_feed = BinanceTradesFeed()
        await binance_feed.start(cfg)
        print(f"  {_C}◉  Binance aggTrade WS{_RS}  candle builder started")

    market_info = await _refresh_market_async(cfg, current_token_id=None)
    if market_info is None:
        log.warning("No active market at startup — will retry each cycle.")

    # ── Reconcile any stale position from a prior crashed/restarted session ──
    # Before entering the main loop, check if the saved state contains a
    # position whose market has already expired. If so, fetch the final
    # resolution price and settle the position.
    await _reconcile_stale_position_on_startup(cfg)

    cycle = 0
    while True:
        cycle += 1
        try:
            # Peek at state to decide market routing for this cycle
            state = load_state(cfg)
            pos   = state["virtual_portfolio"].get("active_position")

            if pos is not None:
                # ── BUG FIX ──────────────────────────────────────────────────
                # Active position: always monitor the position's own market.
                # Do NOT run discovery — switching markets mid-position causes
                # SL/TP checks to use prices from the wrong order book, and
                # the trade will never be settled in the emulator.
                # WS: restart feed if the active position's market changed
                if use_ws and book_feed is not None:
                    if pos["market_id"] != _ws_active_token:
                        await book_feed.stop()
                        await book_feed.start(pos["market_id"], cfg)
                        _ws_active_token = pos["market_id"]
                await _iteration(
                    cfg,
                    pos["market_id"],
                    pos.get("market_end_date_iso", ""),
                    cycle,
                    book_feed=book_feed,
                    use_ws=use_ws,
                    binance_feed=binance_feed,
                )
            else:
                # No position: refresh market discovery when current is stale/expired
                if (
                    market_info is None
                    or _seconds_until_expiry(market_info["end_date_iso"]) < min_expiry_sec
                ):
                    market_info = await _refresh_market_async(
                        cfg,
                        market_info["token_id"] if market_info else None,
                        skip_token_ids=_unavailable_tokens,
                    )

                if market_info is not None:
                    # Fresh valid market — clear the blacklist
                    _unavailable_tokens.clear()
                    # WS: restart feed if discovered market token changed
                    if use_ws and book_feed is not None:
                        if market_info["token_id"] != _ws_active_token:
                            await book_feed.stop()
                            await book_feed.start(market_info["token_id"], cfg)
                            _ws_active_token = market_info["token_id"]
                    await _iteration(
                        cfg,
                        market_info["token_id"],
                        market_info["end_date_iso"],
                        cycle,
                        book_feed=book_feed,
                        use_ws=use_ws,
                        binance_feed=binance_feed,
                    )
                else:
                    _print_status_newline()
                    log.warning("No active market — skipping cycle %d.", cycle)

        except _MarketUnavailableError as exc:
            # CLOB returned 404 — the discovered market token isn't tradeable yet.
            # Blacklist this token so re-discovery skips it, then wait for next round.
            bad_token = str(exc)
            if bad_token:
                _unavailable_tokens.add(bad_token)
            market_info = None
            _print_status_newline()
            log.warning(
                "Market unavailable on CLOB — blacklisting token, waiting 30 s for next round."
            )
            await asyncio.sleep(30)
            continue
        except KeyboardInterrupt:
            print(f"\n{_Y}  Bot stopped.{_RS}\n")
            # WS: stop feeds on shutdown
            if use_ws and book_feed is not None:
                await book_feed.stop()
            if use_binance_ws and binance_feed is not None:
                await binance_feed.stop()
            break
        except Exception as exc:
            _print_status_newline()
            log.error("Iteration error: %s", exc)

        # Tick-driven: wait for next aggTrade tick (poll_interval as fallback/heartbeat)
        if use_binance_ws and binance_feed is not None:
            await binance_feed.wait_for_tick(timeout=poll_interval)
        else:
            await asyncio.sleep(poll_interval)


# ── Single iteration ──────────────────────────────────────────────────────────

async def _iteration(
    cfg: dict, market_id: str, end_date_iso: str, cycle: int,
    book_feed: "PolymarketBookFeed | None" = None, use_ws: bool = False,
    binance_feed: "BinanceTradesFeed | None" = None,
) -> None:
    # ── 1. STATE ──────────────────────────────────────────────────────────────
    state     = load_state(cfg)

    initial = float(cfg.get("risk_management", {}).get("initial_balance_usd", 1000.0))
    current = state["virtual_portfolio"].get("balance_usd", 0.0)
    if current < initial * 0.05:
        _print_status_newline()
        log.error(
            "\033[1m\033[31mCRITICAL: Слито более 95%% депозита "
            "(остаток $%.2f). Остановка бота.\033[0m",
            current,
        )
        token         = cfg.get("endpoints", {}).get("telegram_bot_token")
        chat_id       = cfg.get("endpoints", {}).get("telegram_chat_id")
        proxy         = cfg.get("endpoints", {}).get("proxy")
        strategy_name = cfg.get("strategy", {}).get("name", "Unknown Bot")
        if token and chat_id:
            message = (
                f"🚨 СТОП-КРАН: Бот [{strategy_name}] слил более 95% депозита. "
                f"Остаток: ${current:.2f}. Скрипт остановлен."
            )
            try:
                client_kwargs = {"timeout": 10.0}
                if proxy:
                    client_kwargs["proxy"] = proxy
                with httpx.Client(**client_kwargs) as client:
                    client.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": message},
                    )
            except Exception as tg_exc:
                _print_status_newline()
                log.warning("Telegram notification failed: %s", tg_exc)
        sys.exit(0)

    state     = reset_daily_pnl_if_needed(state)
    portfolio = state["virtual_portfolio"]
    portfolio = update_halt_if_needed(portfolio, cfg)
    state["virtual_portfolio"] = portfolio

    pos          = portfolio.get("active_position")
    seconds_left = _seconds_until_expiry(end_date_iso)

    # ── 1.5. EXPIRY SETTLEMENT ──────────────────────────────────────────────────
    if pos is not None and seconds_left < 90:
        exit_p = get_exit_price(pos, best_bid, best_ask)
        entry_p = pos["entry_price"]
        pnl_pct = (exit_p - entry_p) / entry_p if entry_p > 0 else 0.0

        if pnl_pct < -0.01:
            state = close_position(state, exit_p, "TIME_STOP", cfg, book_data=book)

        side = pos["side"]

        # Вычисляем, сколько секунд прошло с момента экспирации
        end_dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        time_since_expiry = (datetime.now(timezone.utc) - end_dt).total_seconds()

        try:
            last_price = await fetch_last_trade_price_async(cfg, market_id)
        except Exception:
            last_price = 0.5

        if last_price is None:
            last_price = 0.5

        if last_price > 0.9:
            yes_won = True
            result = "WIN" if side == "YES" else "LOSS"
        elif last_price < 0.1:
            yes_won = False
            result = "WIN" if side == "NO" else "LOSS"
        else:
            if time_since_expiry > 1800:  # 30 минут таймаут
                _print_status_newline()
                print(f"  [{cycle:>4}]  {market_id[:12]}…  Oracle timeout (30m). Force closing (Refund).")
                last_price = pos["entry_price"]  # Закрываем по цене входа (PnL = 0)
                result = "DRAW"
            else:
                _print_status_newline()
                print(f"  [{cycle:>4}]  {market_id[:12]}…  Waiting for Oracle... ({int(time_since_expiry)}s)")
                save_state(state, cfg)
                return

        state  = close_position(state, last_price, result, cfg, skip_slippage=(result == "DRAW"))
        trade  = state["trade_history"][-1]
        _print_status_newline()
        _print_close(
            f"EXPIRED {result}",
            pos["entry_price"],
            trade["exit_price"],
            trade["pnl"],
            state["virtual_portfolio"]["balance_usd"],
        )
        save_state(state, cfg)
        return

    # ── 2. PARALLEL FETCH ─────────────────────────────────────────────────────
    # market_id here is already the correct market: either position's own market
    # (routed by run_loop) or the currently discovered market (no position).
    ws_extremums = None
    use_live_candles = False
    try:
        # Binance candles: prefer live WS feed when ready, fall back to REST
        if binance_feed is not None and binance_feed.is_ready():
            candles = binance_feed.get_candles()
            use_live_candles = True
            candle_source = "bnb-ws"
        else:
            candles = None  # will be fetched below
            candle_source = "bnb"

        # WS: use WebSocket snapshot when available; fall back to HTTP otherwise
        if use_ws and book_feed is not None:
            ws_book = book_feed.get_latest()
            if ws_book is not None:
                ws_extremums = book_feed.get_and_reset_extremums() if pos is not None else None
                if candles is None:
                    candles = await fetch_binance_klines_async(cfg)
                book = ws_book
                book_source = "ws"
            else:
                # WebSocket not ready yet — fall back to HTTP
                if candles is None:
                    candles, book = await asyncio.gather(
                        fetch_binance_klines_async(cfg),
                        fetch_polymarket_book_async(cfg, market_id),
                    )
                else:
                    book = await fetch_polymarket_book_async(cfg, market_id)
                book_source = "http-fallback"
        else:
            if candles is None:
                candles, book = await asyncio.gather(
                    fetch_binance_klines_async(cfg),
                    fetch_polymarket_book_async(cfg, market_id),
                )
            else:
                book = await fetch_polymarket_book_async(cfg, market_id)
            book_source = "http"
    except httpx.HTTPStatusError as exc:
        _print_status_newline()
        if exc.response.status_code == 404:
            log.warning("CLOB 404 for token %s — market not yet tradeable.", market_id)
            raise _MarketUnavailableError(market_id) from exc
        log.warning("API fetch failed: %s", exc)
        return
    except Exception as exc:
        _print_status_newline()
        log.warning("API fetch failed: %s", exc)
        return

    best_ask        = book["best_ask"]
    best_bid        = book["best_bid"]
    book_imbalance  = book.get("book_imbalance")

    # ── 3. ATR ────────────────────────────────────────────────────────────────
    atr_period     = cfg.get("risk_management", {}).get("atr_period", 14)
    atr_raw        = calculate_atr(candles, period=atr_period)
    last_close     = float(candles[-1]["close"]) if candles else None
    atr_normalized = normalize_atr(atr_raw, last_close, cfg) if atr_raw else None  # BUG FIX: pass cfg for configurable BTC price fallback

    # ── 4. STATUS LINE ────────────────────────────────────────────────────────
    macd_state  = get_macd_state(candles, cfg)
    can_trade   = should_open_trade(portfolio, seconds_left, cfg)
    trailing    = pos.get("trailing_stop_price") if pos else None
    _print_status(
        market_id[:12], best_ask, best_bid, seconds_left,
        portfolio["balance_usd"], cycle,
        macd_state=macd_state, source=f"{candle_source}{len(candles)}+{book_source}",
        can_trade=can_trade, book_imbalance=book_imbalance,
        trailing_stop=trailing,
    )

    if portfolio.get("trading_halted_until"):
        _print_status_newline()
        log.warning("Trading halted until %s", portfolio["trading_halted_until"])

    # ── 6. TRAILING STOP UPDATE ───────────────────────────────────────────────
    if pos is not None:
        portfolio["active_position"] = update_trailing_stop(
            pos, best_bid, best_ask, atr_normalized, cfg,
            ws_extremums=ws_extremums,
        )
        state["virtual_portfolio"] = portfolio

    # ── 6a. TIME-STOP CHECK ──────────────────────────────────────────────────
    risk_cfg_iter = cfg.get("risk_management", {})
    if (
        pos is not None
        and risk_cfg_iter.get("use_time_stop", False)
    ):
        time_stop_sec = float(risk_cfg_iter.get("time_stop_seconds", 120))
        time_stop_max_loss = float(risk_cfg_iter.get("time_stop_max_loss_pct", 0.02))
        tp_pct_pos = float(pos.get("tp_pct", risk_cfg_iter.get("take_profit_pct", 0.10)))

        pos_ts = pos.get("timestamp")
        if pos_ts:
            pos_dt = datetime.fromisoformat(pos_ts)
            if pos_dt.tzinfo is None:
                pos_dt = pos_dt.replace(tzinfo=timezone.utc)
            age_sec = (datetime.now(timezone.utc) - pos_dt).total_seconds()

            if age_sec > time_stop_sec:
                exit_p = get_exit_price(pos, best_bid, best_ask)
                entry_p = pos["entry_price"]
                if entry_p > 0:
                    pnl_pct = (exit_p - entry_p) / entry_p
                else:
                    pnl_pct = 0.0

                # Close only if PnL is in the "dead zone": small loss or unrealised gain < TP
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

    # ── 6b. REVERSE-CLOSE CHECK ──────────────────────────────────────────────
    if (
        pos is not None
        and risk_cfg_iter.get("use_reverse_close", False)
    ):
        # Calculate BTC price at market open for signal context
        market_open_price = _get_market_open_btc_price(end_date_iso, candles)

        reverse_signal = generate_signal(
            candles, cfg, book_data=book,
            is_last_candle_open=use_live_candles,
            market_open_price=market_open_price,
        )
        # If MACD gives opposite signal to our open position, close immediately
        pos_side = pos.get("side", "")
        is_reverse = (
            (pos_side == "YES" and reverse_signal == "BUY_NO")
            or (pos_side == "NO" and reverse_signal == "BUY_YES")
        )
        if is_reverse:
            exit_p = get_exit_price(pos, best_bid, best_ask)
            state = close_position(state, exit_p, "REVERSE_CLOSE", cfg, book_data=book)
            trade = state["trade_history"][-1]
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

    # ── 7. SL / TP CHECK ──────────────────────────────────────────────────────
    use_sl_tp = cfg.get("risk_management", {}).get("use_sl_tp", True)
    if use_sl_tp and portfolio.get("active_position") is not None:
        pos = portfolio["active_position"]

        # Hard TP: WS extremums may have touched our limit price between poll cycles
        use_hard_tp = cfg.get("risk_management", {}).get("use_hard_tp", True)
        if use_hard_tp and ws_extremums is not None:
            target_tp_price = pos["entry_price"] * (1 + pos.get("tp_pct", 0.10))
            ws_tp_exit = get_exit_price(pos, ws_extremums["highest_bid"], ws_extremums["lowest_ask"])
            hard_tp_hit = ws_tp_exit >= target_tp_price
            if hard_tp_hit:
                exit_p = target_tp_price
                state  = close_position(state, exit_p, "TP", cfg, book_data=book, skip_slippage=True)
                trade  = state["trade_history"][-1]
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

        # ── SL detection with confirmation delay ─────────────────────────
        sl_confirm_sec = float(cfg.get("risk_management", {}).get("sl_confirm_seconds", 0))

        # Hard SL: WS extremums may have touched our stop-loss price between poll cycles
        hard_sl_hit = False
        target_sl_price = None
        use_hard_sl = cfg.get("risk_management", {}).get("use_hard_sl", True)
        if use_hard_sl and ws_extremums is not None:
            target_sl_price = pos["entry_price"] * (1 - pos.get("sl_pct", 0.07))
            ws_sl_exit = get_exit_price(pos, ws_extremums["lowest_bid"], ws_extremums["highest_ask"])
            hard_sl_hit = ws_sl_exit <= target_sl_price

        # Soft SL/TP: check against current best prices
        trigger = check_sl_tp(portfolio, best_bid, best_ask, cfg)

        # Soft TP fires immediately — no confirmation delay
        if trigger == "TP":
            pos    = portfolio["active_position"]
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

        # SL breach detected? (Hard or Soft)
        soft_sl_hit = (trigger == "SL")
        sl_hit = hard_sl_hit or soft_sl_hit

        # Apply confirmation delay: wait sl_confirm_seconds before executing SL
        if sl_hit and sl_confirm_sec > 0:
            now = datetime.now(timezone.utc)
            breach_since = pos.get("sl_breach_since")
            if not breach_since:
                pos["sl_breach_since"] = now.isoformat()
                state["virtual_portfolio"]["active_position"] = pos
                _print_status_newline()
                log.info(
                    "SL breach detected — confirming for %ds...",
                    int(sl_confirm_sec),
                )
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
                exit_p = target_sl_price
                state  = close_position(state, exit_p, "SL", cfg, book_data=book, skip_slippage=True)
                trade  = state["trade_history"][-1]
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
                pos    = portfolio["active_position"]
                exit_p = get_exit_price(pos, best_bid, best_ask)
                state  = close_position(state, exit_p, "SL", cfg, book_data=book)
                trade  = state["trade_history"][-1]
                is_trailing = (
                    pos.get("trailing_stop_price") is not None
                    and exit_p <= pos["trailing_stop_price"]
                )
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

    # ── 8. OPEN TRADE ─────────────────────────────────────────────────────────
    if not should_open_trade(portfolio, seconds_left, cfg):
        save_state(state, cfg)
        return

    # Calculate BTC price at market open — key reference for 5-min market prediction
    market_open_price = _get_market_open_btc_price(end_date_iso, candles)

    signal = generate_signal(
        candles, cfg, book_data=book,
        is_last_candle_open=use_live_candles,
        market_open_price=market_open_price,
    )
    if signal is None:
        save_state(state, cfg)
        return

    side        = "YES" if signal == "BUY_YES" else "NO"
    size_usd    = calculate_position_size(portfolio, cfg)
    entry_price = best_ask if side == "YES" else (1.0 - best_bid)

    if size_usd < 1.0:
        _print_status_newline()
        log.critical("Balance too low ($%.2f). Stopping.", portfolio["balance_usd"])
        raise SystemExit(1)

    # Depth warning: alert if order size exceeds visible book liquidity
    relevant_vol_key = "ask_volume" if side == "YES" else "bid_volume"
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

    # Compute dynamic SL/TP from current ATR before opening
    sl_pct, tp_pct = compute_dynamic_sl_tp(atr_normalized, cfg)

    # Spread viability check: don't open if spread eats too much of the SL room
    spread = best_ask - best_bid
    if entry_price > 0 and spread > 0:
        spread_cost_pct = spread / entry_price
        if spread_cost_pct >= sl_pct * 0.75:
            _print_status_newline()
            log.info(
                "Spread too wide for SL: spread %.1f%% >= 75%% of SL %.1f%% — skipping",
                spread_cost_pct * 100, sl_pct * 100,
            )
            save_state(state, cfg)
            return

    # Time-to-expiry quality check: warn if TP is unlikely within remaining time
    if seconds_left < 120 and tp_pct > 0.10:
        _print_status_newline()
        log.warning(
            "Only %.0fs to expiry with TP %.1f%% — TP may not be reachable.",
            seconds_left, tp_pct * 100,
        )

    cfg["_current_market_end_date_iso"] = end_date_iso

    # ── Async Latency: simulate Polygon block inclusion delay ─────────────
    # After signal fires, real tx waits 1.5-4s to be included in a block.
    # During this time MMs reprice the book. We re-fetch fresh book data
    # so the fill executes against the post-impulse order book, not the
    # stale snapshot that generated the signal.
    sim_cfg = cfg.get("simulation", {})
    latency_min = float(sim_cfg.get("latency_min_sec", 0.0))
    latency_max = float(sim_cfg.get("latency_max_sec", 0.0))
    if latency_max > 0:
        delay = random.uniform(latency_min, latency_max)
        _print_status_newline()
        log.info("Simulating Polygon tx latency: %.1fs...", delay)
        await asyncio.sleep(delay)

        # Re-fetch fresh order book after the delay
        try:
            if use_ws and book_feed is not None:
                fresh_book = book_feed.get_latest()
                if fresh_book is not None:
                    book = fresh_book
                    log.info("Book refreshed from WS after latency delay")
                else:
                    book = await fetch_polymarket_book_async(cfg, market_id)
                    log.info("Book refreshed from HTTP after latency delay")
            else:
                book = await fetch_polymarket_book_async(cfg, market_id)
                log.info("Book refreshed from HTTP after latency delay")

            # Recalculate entry price from the fresh book
            best_ask = book["best_ask"]
            best_bid = book["best_bid"]
            entry_price = best_ask if side == "YES" else (1.0 - best_bid)
        except Exception as exc:
            log.warning("Book re-fetch after latency failed: %s — using stale book", exc)

    state = open_position(
        state, side, entry_price, size_usd, market_id, cfg,
        book_data=book, sl_pct=sl_pct, tp_pct=tp_pct,
    )

    # TX drop: open_position returns state unchanged if tx was dropped
    new_pos = state["virtual_portfolio"].get("active_position")
    if new_pos is None:
        save_state(state, cfg)
        return

    # Reset WS extremums NOW so the next iteration tracks spikes only from this moment
    if use_ws and book_feed is not None:
        book_feed.get_and_reset_extremums()
    save_state(state, cfg)

    _print_status_newline()
    _print_open(
        side,
        new_pos["entry_price"],
        new_pos["size_usd"],
        state["virtual_portfolio"]["balance_usd"],
        seconds_left,
        market_id[:24],
        new_pos.get("fill_pct", 1.0),
        new_pos.get("sl_pct", sl_pct),
        new_pos.get("tp_pct", tp_pct),
        market_open_price=market_open_price,
        current_btc=float(candles[-1]["close"]) if candles else None,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket Paper Trading Bot")
    parser.add_argument("--config", "-c", default=None,
                        help="Path to config YAML (default: ../config.yaml)")
    args = parser.parse_args()
    cfg  = load_config(args.config)
    asyncio.run(run_loop(cfg))


if __name__ == "__main__":
    main()