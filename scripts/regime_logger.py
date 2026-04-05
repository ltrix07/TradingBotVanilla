"""
scripts/regime_logger.py — Скрипт-шпион для сбора данных о фазах рынка (Market Regime).
Собирает 15-минутные свечи BTC, рассчитывает ADX (сила тренда) и ATR (волатильность),
и сохраняет их в CSV для последующего слияния с логами торгов.
"""

import os
import csv
import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone

# Путь к файлу с историей фаз рынка
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CSV_FILE = os.path.join(DATA_DIR, "market_regime.csv")

def fetch_15m_candles(symbol="BTCUSDT", limit=50):
    """Получает последние 15-минутные свечи с Binance."""
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=15m&limit={limit}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    try:
        data = fetch_15m_candles()
    except Exception as e:
        print(f"[ERROR] Не удалось получить данные с Binance: {e}")
        return

    # Загружаем в DataFrame
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "tbbav", "tbqav", "ignore"
    ])

    # Конвертируем в числа
    for col in ["high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    # Считаем индикаторы (ADX для силы тренда, ATR для волатильности)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    if adx_df is not None:
        df = pd.concat([df, adx_df], axis=1)
    
    df["ATR_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)

    # Берем последнюю ЗАКРЫТУЮ свечу (df.iloc[-2]), так как текущая еще формируется
    last_closed = df.iloc[-2]
    current_price = df.iloc[-1]["close"]
    now_iso = datetime.now(timezone.utc).isoformat()

    # Формируем строку данных
    row = {
        "timestamp": now_iso,
        "btc_price": current_price,
        "adx_15m": round(last_closed.get("ADX_14", 0.0), 2),
        "dmp_15m": round(last_closed.get("DMP_14", 0.0), 2),  # Позитивное движение
        "dmn_15m": round(last_closed.get("DMN_14", 0.0), 2),  # Негативное движение
        "atr_15m": round(last_closed.get("ATR_14", 0.0), 2),
        "volume_15m": round(last_closed["volume"], 2)
    }

    # Дописываем в CSV
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"[OK] {now_iso} | Price: {row['btc_price']} | ADX: {row['adx_15m']} | ATR: {row['atr_15m']}")

if __name__ == "__main__":
    main()