# Technical Specification: Crypto Futures Paper Trading Bot

## 1. Обзор системы

Автономный бот для имитации торговли BTCUSDT perpetual на Binance Futures USDT-M.
Paper-режим: локальный JSON-файл как источник правды для баланса и позиций.
Live-режим (будущий): прямая интеграция через signed REST/WS API Binance.

## 2. Data Structures (`state.json`)

### `virtual_portfolio`
| Поле | Тип | Описание |
|------|-----|----------|
| `balance_usd` | float | Начальный: 1000.0 |
| `active_position` | object \| null | см. ниже |
| `daily_pnl` | float | Сбрасывается в 00:00 UTC |
| `last_update` | str (ISO) | Время последнего обновления |
| `trading_halted_until` | str (ISO) \| null | Prop-firm halt до даты |
| `last_sl_timestamp` | str (ISO) \| null | Время последнего SL для cooldown |

### `active_position` (когда позиция открыта)
| Поле | Тип |
|------|-----|
| `id` | uuid |
| `side` | "LONG" \| "SHORT" |
| `symbol` | "BTCUSDT" |
| `entry_price` | float |
| `qty` | float (BTC) |
| `size_usd` | float (notional) |
| `sl_pct`, `tp_pct` | float (dynamic, ATR-based) |
| `trailing_stop_price` | float \| null |
| `breakeven_activated` | bool |
| `timestamp` | str (ISO) |

### `trade_history[]`
| Поле | Тип |
|------|-----|
| `id` | uuid |
| `timestamp` | str (ISO) |
| `symbol` | "BTCUSDT" |
| `side` | "LONG" \| "SHORT" |
| `entry_price`, `exit_price` | float |
| `qty`, `size_usd`, `fill_pct` | float |
| `sl_pct`, `tp_pct` | float |
| `pnl` | float (USD, net of fees) |
| `result` | "SL" \| "TP" \| "TIME_STOP" \| "MAX_HOLD" \| "REVERSE_CLOSE" |

## 3. Risk Management Logic

| Параметр | Правило |
|----------|---------|
| Risk-based sizing | `size_usd = balance * risk_per_trade_pct / sl_pct` |
| Stop-Loss (SL) | `atr_sl_multiplier * atr_pct` (default 3x ATR) |
| Take-Profit (TP) | `atr_tp_multiplier * atr_pct` (default 4.5x ATR -> R:R 1.5) |
| Trailing Stop | Ratchets to `price - (1.5 * atr_raw)` for LONG, inverse for SHORT |
| Breakeven | Snaps SL to entry_price after `1.0 * atr_raw` profit |
| Max Daily Loss | Prop firm: 4% of initial_balance_usd; halt until 00:00 UTC next day |
| Time-stop | Close after 15 min if PnL in dead-zone [-0.5%, +TP) |
| Max-hold-time | Hard close after 120 min regardless of PnL (safety) |
| Reverse-close | Close on opposite MACD signal |
| Funding guard | Block entry if abs(funding_rate_bps) > 50 |
| Cooldown after SL | 60 sec before next entry |

## 4. API Endpoints

### Binance Futures (public, no auth needed for paper mode)
```
REST:   GET  https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=300
REST:   GET  https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=20
REST:   GET  https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT

WS:     wss://fstream.binance.com/ws/btcusdt@aggTrade       (tick -> candle builder)
WS:     wss://fstream.binance.com/ws/btcusdt@depth20@100ms  (top-20 snapshots)
```

### Binance Futures (signed, for future live mode)
```
POST   /fapi/v1/order                       (place order)
DELETE /fapi/v1/order                       (cancel order)
GET    /fapi/v2/account                     (balances, positions)
POST   /fapi/v1/leverage                    (set leverage)
POST   /fapi/v1/marginType                  (isolated/cross)
POST   /fapi/v1/listenKey                   (user data stream)
```
All signed requests: `X-MBX-APIKEY` header + HMAC-SHA256 signature of params.

## 5. Main Loop Algorithm

1. **State sync** - load state.json, reset daily_pnl if UTC-date changed, apply halt guard
2. **Fetch** - candles (WS preferred, REST fallback) + order book (WS depth20 -> REST)
3. **ATR** - compute raw ATR (USD units) and normalized ATR (fraction of price)
4. **Trailing stop update** - ratchet price floor/ceiling based on best_bid/ask + ws_extremums
5. **Position exit checks** (in this order):
   - Max-hold-time exceeded -> force close
   - Time-stop triggered (stale dead-zone position) -> close
   - Reverse-close: MACD flipped against position -> close
   - Hard TP (WS extremums crossed TP price between polls) -> close
   - Soft TP (current bid/ask at TP) -> close
   - SL breach (Hard or Soft) + confirm_seconds wait -> close
6. **Entry** (only if no position):
   - `should_open_trade` gate (balance ok, cooldown elapsed, not halted)
   - `generate_signal` returns BUY_YES/BUY_NO/None
   - Map BUY_YES -> LONG, BUY_NO -> SHORT
   - Spread viability check (spread/entry < 75% of SL%)
   - Funding-rate guard
   - `calculate_position_size` with risk-based formula
   - `open_position` -> update state.json

## 6. Edge Cases

- **WS disconnect**: automatic reconnect with 3s backoff; REST fallback during outage
- **REST timeout**: 5s strict, iteration skipped, no stale data used
- **Insufficient balance**: kill-switch fires when balance < 5% of initial, Telegram alert, sys.exit
- **Orphan position on crash**: on restart, bot re-loads state.json and continues monitoring the existing position
- **Trailing stop set but current price already below**: `check_sl_tp` triggers SL immediately on resume

## 7. Orchestrator (`orchestrator.py`)

Supervisor that polls BTCUSDT ADX every 15 min and switches which strategy
config the inner bot runs:

| ADX(15m) | Mode | Base config |
|----------|------|-------------|
| < 20 | sniper | `configs/config_sniper.yaml` |
| 20 - 35 | volume | `configs/config_volume.yaml` |
| >= 35 | trend | `configs/config_trend.yaml` |

All three modes share the same `state_hybrid.json` and `trades_hybrid.csv`
so balance and history are continuous across regime switches.
