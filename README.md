# Crypto Futures Paper Trading Bot

Paper-trading бот для BTCUSDT perpetual на Binance Futures USDT-M.
Опционально — ежедневный пост-трейд анализ через Claude Opus 4.7.

## Стек

- Python 3.10+
- pandas, pandas_ta (индикаторы)
- httpx (async REST), websockets (стакан и свечи в реал-тайме)
- anthropic SDK (для daily review)
- requests (Telegram long-polling)

## Установка

```bash
pip install -r requirements.txt
```

## Быстрый старт (paper mode)

### Вариант 1: один бот напрямую

```bash
python src/main.py --config config.yaml
```

Будет тикать с настройками из `config.yaml` (сбалансированная стратегия).

### Вариант 2: orchestrator (переключение стратегий по ADX)

```bash
python orchestrator.py
```

Каждые 15 минут orchestrator опрашивает ADX(15m) на BTCUSDT и переключает дочерний бот:

| ADX(15m) | Режим | Конфиг |
|---|---|---|
| `< 20` | sniper | `configs/config_sniper.yaml` |
| `20 – 35` | volume | `configs/config_volume.yaml` |
| `≥ 35` | trend | `configs/config_trend.yaml` |

Все три режима пишут в общий `data/state_hybrid.json` и `data/trades_hybrid.csv` — баланс сохраняется между переключениями.

## Файловая структура

```
bot/
├── config.yaml                       # default config для прямого запуска main.py
├── orchestrator.py                   # переключает конфиги по ADX
├── configs/
│   ├── config_sniper.yaml            # ADX<20
│   ├── config_volume.yaml            # 20–35
│   ├── config_trend.yaml             # >=35
│   ├── config_balanced.yaml
│   ├── config_scalper.yaml
│   └── backups/                      # бэкапы при apply_review
├── src/
│   ├── main.py                       # async entry point
│   ├── strategy.py                   # MACD + RSI + book imbalance
│   ├── fetcher.py                    # Binance Futures REST + WS
│   ├── risk.py                       # SL/TP, trailing, halt
│   ├── execution.py                  # paper/live dispatch
│   └── binance_client.py             # скелет для live (NotImplementedError)
├── scripts/
│   ├── daily_review.py               # cron: Claude post-trade анализ
│   ├── apply_review.py               # CLI для применения diff
│   ├── review_bot.py                 # Telegram callback handler
│   ├── reporter.py                   # Telegram daily summary
│   ├── reset_state.py                # сброс state/trades
│   ├── regime_logger.py              # пишет market_regime.csv
│   └── analyze_trades.py
├── data/                             # state.json, trades_*.csv, reviews/
├── requirements.txt
├── PROJECT_IDEA.md
├── SPECIFICATION.md
└── README.md
```

## Daily review (Claude Opus 4.7)

Раз в сутки читает `data/trades_*.csv`, считает статистику, зовёт Claude Opus 4.7 для анализа и предложения config diffs. Ничего не применяется автоматически — для этого есть `apply_review.py` и `review_bot.py`.

### Настройка

1. Получи API ключ на https://console.anthropic.com
2. Экспортируй в окружение (**рекомендуется**):
   ```bash
   export ANTHROPIC_API_KEY="sk-ant-..."
   ```
   Или, если нужна привязка к конфигу — пропиши в `config.yaml` (любом):
   ```yaml
   endpoints:
     anthropic_api_key: "sk-ant-..."
   ```
3. Настрой Telegram (если ещё не сделал):
   ```yaml
   endpoints:
     telegram_bot_token: "123456:ABC..."
     telegram_chat_id: "1234567890"
   ```

### Ручной запуск

```bash
# Dry-run: считает статистику, строит промпт, не вызывает API
python scripts/daily_review.py --dry-run

# Реальный запуск
python scripts/daily_review.py
```

Если за последние 24ч было меньше 5 сделок — скрипт пропустит вызов Claude (не тратит деньги на шум). Аномалии (drawdown > 5%, зависшая позиция, серия из 5 убытков) алертятся в Telegram **независимо** от review.

Каждый review сохраняется в `data/reviews/YYYY-MM-DD.json` с промптом, ответом и реальным расходом токенов.

### Автоматический запуск (cron)

```cron
# Каждый день в 00:05 UTC
5 0 * * * cd /path/to/bot && /usr/bin/python3 scripts/daily_review.py >> logs/daily_review.log 2>&1
```

Не забудь `mkdir logs` перед первым запуском.

### Применение diff (ручной CLI)

После того как review пришёл в Telegram:

```bash
# Dry-run — показывает что будет изменено
python scripts/apply_review.py 2026-04-21

# Применить
python scripts/apply_review.py 2026-04-21 --confirm

# Отклонить (с пометкой в changelog)
python scripts/apply_review.py 2026-04-21 --reject "Not enough sample size"
```

### Применение через Telegram кнопки

Запусти `review_bot.py` как долгоживущий процесс:

```bash
python scripts/review_bot.py
```

Или как systemd service (пример файла ниже).

Когда daily_review отправит сводку с inline кнопками **✅ Apply / ❌ Reject / 📄 Show diff**, review_bot поймает нажатие и вызовет apply_review в subprocess.

### Защита (зашита в код)

| Защита | Где |
|---|---|
| Claude никогда не пишет в конфиги напрямую | архитектура (3 файла) |
| Sanity-лимиты на значения полей | `daily_review.py:SANITY_LIMITS` + `apply_review.py:SANITY_LIMITS` (дублирование — defence in depth) |
| apply_review отказывается писать вне `configs/` | `apply_review.py::apply_review` |
| Бэкап перед каждой записью | `configs/backups/config_X.YYYYMMDDTHHMMSSZ.yaml` |
| Changelog всех изменений (append-only) | `data/reviews/changelog.jsonl` |
| Минимум 5 сделок перед вызовом Claude | `daily_review.py:MIN_TRADES_FOR_REVIEW` |
| Твои **отклонения** НЕ передаются в промпт (anti-sycophancy-drift) | by design |
| В промпт передаются только **факты** предыдущих изменений и их PnL-эффект | `daily_review.py::_load_changelog` |

### Стоимость

Claude Opus 4.7: $5/M input, $25/M output.
Типичный daily review: ~8k input + ~1.5k output = **~$0.08 за вызов**, **~$2.5 в месяц** при ежедневном запуске.

## Systemd-сервисы (Linux)

**Пример: `/etc/systemd/system/crypto-bot.service`**

```ini
[Unit]
Description=Crypto Futures Paper Trading Bot
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/bot
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/python3 orchestrator.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Пример: `/etc/systemd/system/crypto-review-bot.service`**

```ini
[Unit]
Description=Telegram callback handler for daily reviews
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/bot
Environment=PYTHONUNBUFFERED=1
Environment=ANTHROPIC_API_KEY=sk-ant-...
ExecStart=/usr/bin/python3 scripts/review_bot.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Включить:

```bash
sudo systemctl enable --now crypto-bot crypto-review-bot
sudo systemctl status crypto-bot
```

## Переход на live-торговлю

**Пока НЕ делай** — сначала нужны 2+ недели стабильного положительного paper-PnL.

Когда придёт время:

1. Установи флаг в конфиге:
   ```yaml
   execution:
     mode: "live"
   exchange:
     testnet: true   # на первый раз используй testnet.binancefuture.com
     # api_key / api_secret — лучше через env
   ```
2. Заполни все методы в `src/binance_client.py` (сейчас они `raise NotImplementedError`). Скелет уже содержит helper-функции для подписи (`_sign`, `_signed_params`) и документацию по endpoint-ам.
3. Обязательно: на testnet сначала. Потом mainnet с маленьким капиталом.

## Известные ограничения

- Один символ за раз (`BTCUSDT`). Мультиактив — следующая большая итерация.
- Paper-симулятор не учитывает funding payments за держание позиции (только блокирует вход при плохом funding rate). Для коротких позиций это приемлемо; для holds > 8h фиксируй вручную.
- Live Binance клиент пока только скелет.
