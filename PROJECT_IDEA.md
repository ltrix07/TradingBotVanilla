# Project Idea: Crypto Futures Paper Trading Bot

**Problem**: Эмоциональный трейдинг на 1-5 минутках, высокие комиссии фьючерсов,
невозможность 24/7 мониторить BTC вручную. Рыночные ордера съедают edge через
проскальзывание и taker-fees.

**Solution**: Автономный Python-бот для BTCUSDT perpetual на Binance Futures.
MACD + RSI + order-book imbalance + volume spike + trend EMA на 1-минутных свечах.
Стартует в paper-режиме (локальная симуляция через state.json без реальных ордеров).
Когда paper-статистика выходит стабильно в плюс — переключается на live через
`binance_client.py` (готовый скелет с signed-request helpers).

**Architecture**:
- **Data Layer** — Binance Futures REST (`/fapi/v1/klines`, `/fapi/v1/depth`,
  `/fapi/v1/premiumIndex`) + WebSocket (`wss://fstream.binance.com`) для свечей
  и стакана в реал-тайме.
- **Strategy Layer** — MACD crossover (основной сигнал) + RSI-фильтр + book
  imbalance + trend EMA + volume spike. Маппинг `BUY_YES -> LONG`, `BUY_NO -> SHORT`.
- **Risk Engine** — ATR-based dynamic SL/TP, trailing stop с breakeven-snap,
  funding-rate guard, prop-firm daily-drawdown guard, time-stop и max-hold-time.
- **Execution Layer** — paper (state.json) или live (binance_client.py) через
  один флаг `cfg.execution.mode`.
- **Orchestrator** — `orchestrator.py` опрашивает ADX на 5m/15m и переключает
  конфиг бота: <20 sniper, 20-35 volume, >=35 trend.

**Stack**: Python 3.10+, pandas, pandas_ta, httpx (async REST), websockets, PyYAML.

**Target audience**: Алготрейдеры, которые хотят оттестировать MACD-стратегию
на крипто-фьючерсах без риска реальных денег, прежде чем идти на mainnet.

## Риски

| Риск | Вероятность | Митигация |
|------|-------------|-----------|
| Переобучение под исторические свечи | Высокая | Paper-фаза 2+ недели, EV >=0 на живых данных до live |
| Fees + slippage съедают edge | Средняя | Paper-симулятор учитывает 0.05% round-trip + ATR-based slippage |
| Ликвидация при высоком плече | Низкая при 5x | SL 3xATR (~3%) намного ближе чем ликвидация (~18%) |
| Funding-rate убивает SHORT | Средняя | Funding guard блокирует вход если |rate| > 50 bps |
| Бот застревает с открытой позицией | Низкая | max_hold_time_minutes + time_stop как safety net |
| Биржа возвращает ошибку на ордер | Низкая в paper | В live binance_client делает идемпотентные клиентские ID |
