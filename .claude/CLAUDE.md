# Polymarket Paper Trading Bot (Spec-First v2.0)

## Технологический стек
- Python 3.10+, Pandas, Pandas_TA, HTTPX, websockets
- Storage: `data/state_<profile>.json` (Portfolio & History)
- API: Polymarket CLOB (REST + WebSocket), Binance (REST)

## Основные команды
- Установка: `pip install pandas pandas-ta httpx pyyaml websockets`
- Запуск бота: `python src/main.py --config configs/config_<profile>.yaml`
- Сброс статистики: `python scripts/reset_state.py`

## Правила разработки
- Никаких TODO в коде.
- Risk Management — абсолютный приоритет.
- Весь PnL считается локально в `state_<profile>.json`.
- Используй Context7 MCP для актуальных данных по API.

---

## Архитектура модулей

```
src/
  main.py       # Async event loop, routing, display, Hard SL/TP checks
  strategy.py   # MACD + RSI + Book imbalance + entry_filters signal generation
  risk.py       # ATR, trailing stop, SL/TP checks, trade gate
  execution.py  # Mock broker: slippage, partial fill, position open/close
  fetcher.py    # Binance REST, Polymarket CLOB REST + WebSocket book feed

scripts/
  reporter.py   # Trade history analysis (WIN / LOSS / DRAW)
  reset_state.py

data/
  state_<profile>.json   # по одному файлу на каждый конфиг-профиль

configs/
  config_aggress.yaml
  config_safe.yaml
  config_fast_safe.yaml
  config_sniper.yaml
  config_uncertainty.yaml  # фильтр неопределённости + volume spike
```

## Команда субагентов

| Агент | Модель | Роль | Инструменты |
|-------|--------|------|-------------|
| `architect` | Opus | Проектирование связей между API и Risk Engine | Read, Write, Bash, Context7 |
| `trader-engineer` | Sonnet | Реализация стратегии MACD и логики симулятора | Read, Write, Bash |
| `data-fetcher` | Sonnet | Написание модулей запросов к REST API | Read, Write, Bash, Context7 |
| `qa-reviewer` | Sonnet | Тестирование логики Risk Management на краевых кейсах | **Только** Read, Bash, Grep |

## Соглашения по коду
- Все расчёты цен выхода — только через `get_exit_price()` из `risk.py`
- Hard SL/TP блоки в `main.py` должны быть идентичны по структуре для YES и NO
- Любой новый `result`-тип в `close_position()` обязательно добавляется в `reporter.py`
- `liquidity_fallback_usd` всегда берётся из конфига, не хардкодится

---

## Паттерн расширения: entry_filters

`entry_filters` — необязательная надстройка над сигнальной логикой в `strategy.py`.
Все параметры опциональные: если секция или параметр отсутствуют в конфиге — фильтр
не применяется, поведение идентично предыдущим версиям. Старые конфиги работают без изменений.

### Доступные фильтры

```yaml
strategy:
  entry_filters:
    max_token_price: 0.70         # не входить если YES ask выше порога
    min_token_price: 0.30         # не входить если YES ask ниже порога
    market_uncertainty_band: 0.15 # входить только если цена в диапазоне [0.5-band, 0.5+band]
                                  # например 0.15 → только при цене 0.35–0.65
    trend_ema_period: 50          # торговать только по тренду: close > EMA → YES, close < EMA → NO
    require_volume_spike: true    # требовать всплеск объёма BTC на Binance
    volume_spike_multiplier: 1.5  # объём последней свечи > multiplier * среднее
    volume_spike_period: 10       # период для расчёта среднего объёма (свечей)
```

### Market open price filter (автоматический, без конфига)

5-минутные маркеты Polymarket резолвятся по условию: YES выигрывает если BTC при экспирации > BTC при открытии маркета. Фильтр вычисляет цену BTC в момент открытия маркета (`end_date_iso - 300s`) из свечей Binance и блокирует сигналы, противоречащие текущему состоянию:
- BUY_YES блокируется если текущая цена BTC < цена при открытии маркета (NO выигрывает)
- BUY_NO блокируется если текущая цена BTC > цена при открытии маркета (YES выигрывает)

Вычисление — в `main.py::_get_market_open_btc_price()`, фильтр — в `strategy.py::_apply_entry_filters()`.

### Правила реализации
- Фильтры применяются в `generate_signal()` после MACD + RSI + book — последним слоем
- Каждый параметр читается через `.get()` с дефолтом `None` — если `None` фильтр пропускается
- `trend_ema_period` — вычисляет EMA(N) по close. Если close > EMA → аптренд → только BUY_YES. Если close < EMA → даунтренд → только BUY_NO
- `max_token_price` / `min_token_price` используют `book_data["best_ask"]` для YES, `book_data["best_bid"]` для NO
- `market_uncertainty_band` — симметричен вокруг 0.5, имеет приоритет над `max/min_token_price` если оба заданы
- `volume_spike` считается по полю `volume` из candles которые уже передаются в функцию
- Market open price filter — всегда активен, не требует конфига, `market_open_price` передаётся из `main.py`
- При отсутствии `book_data` ценовые фильтры пропускаются (не блокируют сигнал)