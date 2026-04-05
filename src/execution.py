"""
execution.py — Mock Broker (Execution Layer).

[REFACTORED] Simulates crypto futures (Long/Short) trading mechanics.
Replaced Polymarket binary token logic with classical futures PnL math.

LONG:  profit when price rises → PnL = (exit_price - entry_price) * qty
SHORT: profit when price falls → PnL = (entry_price - exit_price) * qty
Fee:   charged on exit notional → fee_pct * exit_price * qty

Prop Firm Daily Drawdown Guard:
  After each close, if daily_pnl <= -(initial_balance_usd * max_daily_loss_pct),
  new entries are blocked until 00:00 UTC of the next calendar day.
  Threshold is computed from initial_balance_usd (fixed), not current balance —
  this matches prop firm standard (daily loss limit resets at market open).

Key changes vs. Polymarket version:
  - side: "YES"/"NO" → "LONG"/"SHORT"
  - close_position: removed WIN/LOSS binary resolution, added futures PnL formula
  - open_position:  is_buy derived from side (LONG=True, SHORT=False) for slippage
  - close_position: is_buy inverted for exit (LONG sells=False, SHORT buys back=True)
  - _simulate_fill_price: removed 0–0.99 price clamp (not valid for BTC prices)
  - Daily halt: now+24h → next day 00:00 UTC (prop firm trading day boundary)
  - position dict: added "breakeven_activated" field (managed by risk.py)
"""

import csv
import json
import os
import random
import uuid
from datetime import datetime, timezone, timedelta

STATE_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "state.json")


def _state_path(cfg: dict) -> str:
    filename = cfg.get("simulation", {}).get("state_file", "state.json")
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", filename)
    )


def _default_state(cfg: dict) -> dict:
    balance = float(cfg.get("risk_management", {}).get("initial_balance_usd", 1000.0))
    return {
        "virtual_portfolio": {
            "balance_usd": balance,
            "active_position": None,
            "daily_pnl": 0.0,
            "last_update": None,
            "trading_halted_until": None,
        },
        "trade_history": [],
    }


def load_state(cfg: dict) -> dict:
    path = _state_path(cfg)
    if not os.path.isfile(path):
        state = _default_state(cfg)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        return state
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict, cfg: dict) -> None:
    path = _state_path(cfg)
    state["virtual_portfolio"]["last_update"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def reset_daily_pnl_if_needed(state: dict) -> dict:
    """Reset daily_pnl to 0.0 if the UTC calendar date has changed."""
    portfolio       = state["virtual_portfolio"]
    last_update_str = portfolio.get("last_update")
    if last_update_str is None:
        return state

    last_update = datetime.fromisoformat(last_update_str)
    if last_update.tzinfo is None:
        last_update = last_update.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    if now.date() > last_update.date():
        portfolio["daily_pnl"] = 0.0
        portfolio["last_sl_timestamp"] = None
        halted_until = portfolio.get("trading_halted_until")
        if halted_until:
            halted_dt = datetime.fromisoformat(halted_until)
            if halted_dt.tzinfo is None:
                halted_dt = halted_dt.replace(tzinfo=timezone.utc)
            if now >= halted_dt:
                portfolio["trading_halted_until"] = None

    return state


# ── Market impact & partial fill helpers ──────────────────────────────────────

def _simulate_fill_price(
    base_price: float,
    filled_size_usd: float,
    book_data: dict | None,
    cfg: dict,
    is_buy: bool = True,
) -> float:
    """Compute realistic fill price including base slippage + market impact.

    Market impact formula:
        total_slippage = base_slippage + impact_factor * (size_usd / liquidity)

    For entry BUY (LONG open / SHORT close):
        fill_price = base_price * (1 + total_slippage)  ← paying more
    For entry SELL (SHORT open / LONG close):
        fill_price = base_price * (1 - total_slippage)  ← receiving less

    [CHANGED] Price is NOT clamped. Polymarket 0–0.99 clamp was removed since
    futures prices (e.g. BTC at 80,000 USD) are unbounded.
    """
    sim_cfg       = cfg.get("simulation", {})
    base_slippage = float(sim_cfg.get("slippage_simulation_pct", 0.001))
    impact_factor = float(sim_cfg.get("market_impact_factor", 0.002))

    adverse_penalty     = float(sim_cfg.get("adverse_selection_penalty_pct", 0.0))
    volume_spike_active = cfg.get("strategy", {}).get("entry_filters", {}).get(
        "require_volume_spike", False
    )

    if book_data is not None:
        spread = book_data.get("best_ask", base_price) - book_data.get("best_bid", base_price)
        if base_price > 0:
            spread_slippage_pct = max(spread / 2.0, 0.0) / base_price
        else:
            spread_slippage_pct = 0.0
        base_slippage = max(base_slippage, spread_slippage_pct)

        volume_key = "ask_volume" if is_buy else "bid_volume"
        raw_volume = book_data.get(volume_key, 0.0)

        if volume_spike_active and adverse_penalty > 0:
            raw_volume *= (1.0 - adverse_penalty)

        liquidity = max(raw_volume * base_price, 10.0)
    else:
        fallback_liquidity = float(sim_cfg.get("liquidity_fallback_usd", 1000.0))
        liquidity = fallback_liquidity

    market_impact  = impact_factor * (filled_size_usd / liquidity)
    total_slippage = base_slippage + market_impact

    if is_buy:
        return base_price * (1.0 + total_slippage)
    else:
        return base_price * (1.0 - total_slippage)


def _simulate_partial_fill(
    requested_size_usd: float,
    cfg: dict,
    book_data: dict | None = None,
    entry_price: float = 0.0,
    is_buy: bool = True,
) -> float:
    """Liquidity-capped partial fill: never fill more than the book offers.

    Two-branch logic:
      1. requested > available  → hard cap at available_usd.
      2. requested <= available → random fill [partial_fill_min_pct, 1.0].

    Falls back to random fill when book_data is absent.
    """
    sim_cfg      = cfg.get("simulation", {})
    min_fill_pct = float(sim_cfg.get("partial_fill_min_pct", 0.85))

    if book_data is not None and entry_price > 0:
        volume_key = "ask_volume" if is_buy else "bid_volume"
        raw_volume = book_data.get(volume_key, 0.0)

        adverse_penalty     = float(sim_cfg.get("adverse_selection_penalty_pct", 0.0))
        volume_spike_active = cfg.get("strategy", {}).get("entry_filters", {}).get(
            "require_volume_spike", False
        )
        if volume_spike_active and adverse_penalty > 0:
            raw_volume *= (1.0 - adverse_penalty)

        max_available_usd = raw_volume * entry_price

        if max_available_usd > 0:
            if requested_size_usd > max_available_usd:
                return max_available_usd
            fill_pct = random.uniform(min_fill_pct, 1.0)
            return requested_size_usd * fill_pct

    fallback_usd = float(sim_cfg.get("liquidity_fallback_usd", 1000.0))
    if requested_size_usd > fallback_usd:
        return fallback_usd
    fill_pct = random.uniform(min_fill_pct, 1.0)
    return requested_size_usd * fill_pct


# ── Transaction drop simulation ───────────────────────────────────────────────

def _tx_dropped(cfg: dict) -> bool:
    """Simulate network tx failure. Returns True if the transaction is dropped."""
    drop_rate = float(cfg.get("simulation", {}).get("tx_drop_rate_pct", 0.0))
    if drop_rate <= 0:
        return False
    return random.random() < drop_rate


# ── Core execution ────────────────────────────────────────────────────────────

def open_position(
    state: dict,
    side: str,            # [CHANGED] "LONG" or "SHORT" (was "YES" / "NO")
    entry_price: float,
    size_usd: float,      # Notional size = qty * entry_price; deducted from balance as margin
    market_id: str,
    cfg: dict,
    book_data: dict | None = None,
    sl_pct: float | None = None,
    tp_pct: float | None = None,
) -> dict:
    """Open a simulated futures position with market-impact slippage and partial fill.

    [CHANGED] Side-aware slippage direction:
      LONG  open → buying  → is_buy=True  (fill_price pushed UP)
      SHORT open → selling → is_buy=False (fill_price pushed DOWN)

    qty = filled_size_usd / fill_price  (e.g. 0.004 BTC when size=320 USD, price=80,000)
    Balance is reduced by filled_size_usd; it is returned on close +/- net PnL.

    [NEW] Position dict includes "breakeven_activated": False field,
    managed by risk.update_trailing_stop().
    """
    from risk import compute_dynamic_sl_tp  # late import avoids circular dep

    if _tx_dropped(cfg):
        import logging
        logging.getLogger(__name__).warning(
            "TX_DROP: open_position tx rejected. Position NOT opened."
        )
        return state

    # [CHANGED] LONG = buying (is_buy=True), SHORT = selling short (is_buy=False)
    is_buy = (side == "LONG")

    filled_size_usd = _simulate_partial_fill(
        size_usd, cfg, book_data=book_data, entry_price=entry_price, is_buy=is_buy,
    )
    fill_pct   = filled_size_usd / size_usd
    fill_price = _simulate_fill_price(
        entry_price, filled_size_usd, book_data, cfg, is_buy=is_buy
    )

    # qty = number of contracts (e.g., BTC); drives all PnL calculations
    qty = filled_size_usd / fill_price

    if sl_pct is None or tp_pct is None:
        sl_pct, tp_pct = compute_dynamic_sl_tp(None, cfg)

    portfolio = state["virtual_portfolio"]
    portfolio["balance_usd"] -= filled_size_usd
    portfolio["active_position"] = {
        "id":                    str(uuid.uuid4()),
        "side":                  side,            # "LONG" or "SHORT"
        "entry_price":           fill_price,
        "qty":                   qty,
        "size_usd":              filled_size_usd,
        "requested_size_usd":    size_usd,
        "fill_pct":              round(fill_pct, 4),
        "market_id":             market_id,
        "market_end_date_iso":   cfg.get("_current_market_end_date_iso", ""),
        "sl_pct":                round(sl_pct, 4),
        "tp_pct":                round(tp_pct, 4),
        "trailing_stop_price":   None,
        "breakeven_activated":   False,           # [NEW] managed by risk.update_trailing_stop
        "timestamp":             datetime.now(timezone.utc).isoformat(),
    }
    return state


def close_position(
    state: dict,
    exit_price: float,
    result: str,
    cfg: dict,
    book_data: dict | None = None,
    skip_slippage: bool = False,
) -> dict:
    """Close the active futures position and settle PnL.

    [CHANGED] Result types (removed WIN/LOSS/DRAW binary outcomes):
      "SL"           : Stop-Loss hit
      "TP"           : Take-Profit hit (pass skip_slippage=True for limit TP)
      "TIME_STOP"    : Position timed out / market expired
      "REVERSE_CLOSE": Opposite signal detected — close before reversing

    [CHANGED] Futures PnL math (replaces binary token resolution):
      LONG:  PnL_gross = (exit_price - entry_price) * qty
      SHORT: PnL_gross = (entry_price - exit_price) * qty
      Fee   = fee_pct * actual_exit_price * qty  (on exit notional)
      Net PnL = PnL_gross - Fee

    Balance on close: balance += size_usd + net_pnl
    (size_usd deducted on open is returned; net_pnl may be negative on loss)

    [CHANGED] Exit slippage direction:
      LONG  close → selling  → is_buy=False (price pushed DOWN)
      SHORT close → buyback  → is_buy=True  (price pushed UP)

    [NEW] Prop Firm Daily Drawdown Guard:
      After updating daily_pnl, if daily_pnl <= -(initial_balance_usd * max_daily_loss_pct),
      trading_halted_until is set to 00:00 UTC of the next calendar day.
      Uses initial_balance_usd (not current balance) — prop firm standard.
    """
    portfolio = state["virtual_portfolio"]
    position  = portfolio.get("active_position")
    if position is None:
        return state

    if result in ("SL", "TP", "TIME_STOP", "REVERSE_CLOSE") and _tx_dropped(cfg):
        import logging
        logging.getLogger(__name__).warning(
            "TX_DROP: close_position (%s) tx rejected. Position stays open.", result
        )
        return state

    fee_pct     = float(cfg.get("simulation", {}).get("fee_simulation_pct", 0.0))
    entry_price = position["entry_price"]
    size_usd    = position["size_usd"]
    qty         = position.get("qty", size_usd / entry_price)
    side        = position.get("side", "LONG")

    # [CHANGED] Exit direction: LONG closes by selling (is_buy=False),
    # SHORT closes by buying back (is_buy=True)
    is_buy_close = (side == "SHORT")
    if skip_slippage:
        actual_exit_price = exit_price
    else:
        actual_exit_price = _simulate_fill_price(
            exit_price, size_usd, book_data, cfg, is_buy=is_buy_close
        )

    # [CHANGED] Futures PnL: direction-aware (replaces binary token gross_return)
    if side == "LONG":
        pnl_gross = (actual_exit_price - entry_price) * qty
    else:  # SHORT
        pnl_gross = (entry_price - actual_exit_price) * qty

    # Fee on exit notional volume (not on gross return as in token model)
    fee     = fee_pct * actual_exit_price * qty
    net_pnl = pnl_gross - fee

    # Return the deducted margin + net PnL (can be negative on a losing trade)
    portfolio["balance_usd"] += size_usd + net_pnl
    portfolio["daily_pnl"]   += net_pnl

    # ── [NEW] Prop Firm Daily Drawdown Guard ──────────────────────────────────
    # Threshold is anchored to initial_balance_usd, not current equity.
    # If daily_pnl breaches the limit, halt until 00:00 UTC next day.
    risk_cfg           = cfg.get("risk_management", {})
    initial_balance    = float(risk_cfg.get("initial_balance_usd", 1000.0))
    max_daily_loss_pct = float(risk_cfg.get("max_daily_loss_pct", 0.04))
    max_daily_loss_usd = initial_balance * max_daily_loss_pct

    if portfolio["daily_pnl"] <= -max_daily_loss_usd:
        now           = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        portfolio["trading_halted_until"] = next_midnight.isoformat()
    # ─────────────────────────────────────────────────────────────────────────

    state["trade_history"].append({
        "id":                  position["id"],
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "market_id":           position["market_id"],
        "side":                side,
        "entry_price":         entry_price,
        "qty":                 round(qty, 6),
        "exit_price":          round(actual_exit_price, 4),
        "size_usd":            size_usd,
        "fill_pct":            position.get("fill_pct", 1.0),
        "sl_pct":              position.get("sl_pct"),
        "tp_pct":              position.get("tp_pct"),
        "trailing_stop_price": position.get("trailing_stop_price"),
        "breakeven_activated": position.get("breakeven_activated", False),  # [NEW]
        "pnl":                 round(net_pnl, 4),
        "result":              result,
    })

    if result == "SL":
        portfolio["last_sl_timestamp"] = datetime.now(timezone.utc).isoformat()

    _append_trade_csv(state["trade_history"][-1], cfg)

    portfolio["active_position"] = None
    return state


def _append_trade_csv(trade: dict, cfg: dict) -> None:
    """Append a completed trade row to the CSV log file."""
    log_file = cfg.get("simulation", {}).get("log_file")
    if not log_file:
        return
    csv_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", log_file)
    )
    fields = [
        "timestamp", "side", "result", "entry_price", "exit_price",
        "size_usd", "qty", "fill_pct", "sl_pct", "tp_pct",
        "pnl", "trailing_stop_price", "breakeven_activated",
    ]
    file_exists = os.path.isfile(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(trade)
