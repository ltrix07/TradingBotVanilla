# Technical Specification: Polymarket Paper Trading Bot

## 1. Обзор системы

Автономный бот для имитации торговли (Paper Trading) на 5-минутных рынках Polymarket.
Система использует REST API для получения данных и локальный JSON-файл для хранения состояния.

## 2. Data Structures (`state.json`)

### `virtual_portfolio`
| Поле | Тип | Описание |
|------|-----|----------|
| `balance_usd` | float | Начальный: 1000.0 |
| `active_position` | jsonb | `{side, entry_price, size_usd, timestamp}` |
| `daily_pnl` | float | Сбрасывается в 00:00 UTC |
| `last_update` | timestamptz | Время последнего обновления |

### `trade_history`
| Поле | Тип |
|------|-----|
| `id` | uuid |
| `timestamp` | timestamptz |
| `market_id` | text |
| `side` | text (`YES`/`NO`) |
| `price` | float |
| `result` | text (`WIN`/`LOSS`/`PENDING`) |

## 3. Risk Management Logic

| Параметр | Правило |
|----------|---------|
| Stop-Loss (SL) | Закрытие позиции при падении цены на **5%** от входа |
| Take-Profit (TP) | Фиксация прибыли при росте цены на **15%** от входа |
| Max Daily Loss | При `daily_pnl <= -10%` баланса — остановка торговли на **24 часа** |
| Position Sizing | Размер сделки — строго **5%** от `balance_usd` |
| Expiration Safety | Если до конца раунда < 30 секунд — новые сделки не открываются |

## 4. API Endpoints (REST Only)

```
Binance:    GET /api/v3/klines?symbol=BTCUSDT&interval=1m&limit=50
Polymarket: GET /book?token_id={id}
Polymarket: GET /prices-history?market={id}&interval=1m
Gamma API: GET https://gamma-api.polymarket.com/markets с параметрами active=true, closed=false, query=Bitcoin Price.
```

## 5. Main Loop Algorithm

0. Discovery — если текущий token_id истек или не задан, найти актуальный рынок через Gamma AP
1. **Sync** — запрос цен BTC и стакана Polymarket
2. **Risk Check** — проверка `daily_pnl` и лимита Max Daily Loss
3. **Position Management** — проверка условий SL/TP для открытой позиции
4. **Signal Generation** — расчёт MACD (3, 15, 3):
   - Пересечение MACD снизу вверх → сигнал `BUY YES`
   - Пересечение MACD сверху вниз → сигнал `BUY NO`
5. **Execution** — если сигнал есть и позиция пуста → запись в `state.json` по цене Best Ask

## 6. Edge Cases

- **API Timeout**: данные не получены за 5 секунд → итерация пропускается, старые данные не используются
- **Insufficient Balance**: баланс ниже $1 → критическая запись в лог, остановка бота
