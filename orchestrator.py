import time
import requests
import pandas as pd
import pandas_ta as ta
import subprocess
import sys
import os
import json
import yaml
from datetime import datetime

# Настройки рынка
SYMBOL = "BTCUSDT"
TIMEFRAME = "5m"
ADX_PERIOD = 14
CHECK_INTERVAL_SEC = 60 * 15
POLL_INTERVAL_SEC = 10  # Sub-interval for crash detection

# Файлы
HYBRID_STATE_FILE = "data/state_hybrid.json"
HYBRID_CONFIG_FILE = "configs/config_hybrid.yaml"
BASE_CONFIGS = {
    "sniper": "configs/config_sniper.yaml",
    "trend": "configs/config_trend.yaml",
    "volume": "configs/config_volume.yaml"
}

# Папка для логов
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
HYBRID_LOG_FILE = os.path.join(LOGS_DIR, "hybrid_bot.log")

active_process = None
active_log_file = None  # Global handle — closed explicitly on terminate
current_mode = "pause"  # 'sniper', 'trend', 'pause'

def get_time():
    return datetime.now().strftime('%H:%M:%S')

def get_market_data():
    """Получает данные с Binance и рассчитывает ADX"""
    url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={TIMEFRAME}&limit=150"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        for col in ['high', 'low', 'close']:
            df[col] = df[col].astype(float)

        adx_df = ta.adx(df['high'], df['low'], df['close'], length=ADX_PERIOD)
        return adx_df[f'ADX_{ADX_PERIOD}'].iloc[-1]
    except Exception as e:
        print(f"[{get_time()}] ❌ Ошибка получения данных: {e}")
        return None

def has_active_position():
    """Проверяет файл состояния гибрида на наличие открытых позиций"""
    if not os.path.exists(HYBRID_STATE_FILE):
        return False

    try:
        with open(HYBRID_STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
            portfolio = state.get("virtual_portfolio", {})
            return portfolio.get("active_position") is not None
    except json.JSONDecodeError:
        # File is being written — safe block: assume position is open
        return True
    except Exception as e:
        print(f"[{get_time()}] ⚠️ Ошибка чтения стейта: {e}")
        return True

def prepare_hybrid_config(base_mode):
    """Создает config_hybrid.yaml на базе нужной стратегии"""
    base_config_path = BASE_CONFIGS[base_mode]
    try:
        with open(base_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if 'simulation' not in config:
            config['simulation'] = {}
        config['simulation']['state_file'] = "state_hybrid.json"

        config['simulation']['log_file'] = "trades_hybrid.csv"

        if 'storage' not in config:
            config['storage'] = {}
        config['storage']['state_file'] = "state_hybrid.json"

        config['storage']['log_file'] = "trades_hybrid.csv"

        with open(HYBRID_CONFIG_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False)

    except Exception as e:
        print(f"[{get_time()}] ❌ Ошибка создания гибридного конфига: {e}")

def switch_mode(target_mode):
    """Переключает текущего бота на новый режим"""
    global active_process, active_log_file, current_mode

    if current_mode == target_mode:
        return

    if has_active_position():
        print(f"[{get_time()}] ⏳ Тренд изменился на {target_mode.upper()}, но есть АКТИВНАЯ ПОЗИЦИЯ. Ждем закрытия сделки режимом {current_mode.upper()}...")
        return

    if active_process is not None:
        print(f"[{get_time()}] 🛑 Останавливаем логику: {current_mode.upper()}")
        active_process.terminate()
        active_process.wait()
        active_process = None
        if active_log_file is not None:
            active_log_file.close()
            active_log_file = None

    current_mode = target_mode

    if target_mode == "pause":
        print(f"[{get_time()}] ⏸ Бот переведен в режим ПАУЗЫ (рынок неопределен).")
    else:
        prepare_hybrid_config(target_mode)
        print(f"[{get_time()}] 🚀 Запускаем логику: {target_mode.upper()} (Стейт: Гибрид)")
        print(f"[{get_time()}] 📝 Логи сделок пишутся в файл: {HYBRID_LOG_FILE}")

        active_log_file = open(HYBRID_LOG_FILE, "a", encoding="utf-8")
        cmd = [sys.executable, "src/main.py", "--config", HYBRID_CONFIG_FILE]
        active_process = subprocess.Popen(
            cmd,
            stdout=active_log_file,
            stderr=subprocess.STDOUT,
            text=True
        )

def orchestrate():
    adx = get_market_data()
    if adx is None:
        return

    print(f"\n[{get_time()}] 📊 Текущий ADX: {adx:.2f}")

    if adx < 25:
        target_mode = "sniper"
    elif adx <= 40:
        target_mode = "pause"
    else:
        target_mode = "trend"

    switch_mode(target_mode)

if __name__ == "__main__":
    with open(HYBRID_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"[{get_time()}] --- НОВЫЙ ЗАПУСК ДИРИЖЕРА ---\n")

    print(f"[{get_time()}] 🧠 Гибридный Мозг (Дирижер) запущен!")
    print(f"[{get_time()}] Баланс и позиции синхронизируются через {HYBRID_STATE_FILE}")

    if not os.path.exists(HYBRID_STATE_FILE):
        os.makedirs("data", exist_ok=True)
        init_state = {
            "virtual_portfolio": {
                "balance_usd": 1000.0,
                "active_position": None,
                "daily_pnl": 0.0
            },
            "trade_history": []
        }
        with open(HYBRID_STATE_FILE, 'w') as f:
            json.dump(init_state, f)
        print(f"[{get_time()}] 💰 Создан новый портфель на $1000")

    try:
        orchestrate()
        while True:
            print(f"[{get_time()}] 💤 Ожидание 15 минут до следующей проверки...\n" + "-" * 40)
            elapsed = 0
            while elapsed < CHECK_INTERVAL_SEC:
                time.sleep(POLL_INTERVAL_SEC)
                elapsed += POLL_INTERVAL_SEC
                if active_process is not None and active_process.poll() is not None:
                    print(f"[{get_time()}] ⚠️ Дочерний процесс упал (код: {active_process.returncode}). Принудительный перезапуск...")
                    active_process = None
                    if active_log_file is not None:
                        active_log_file.close()
                        active_log_file = None
                    orchestrate()
                    break
            else:
                orchestrate()
    except KeyboardInterrupt:
        if active_process:
            active_process.terminate()
        if active_log_file:
            active_log_file.close()
        print(f"\n[{get_time()}] Дирижер остановлен.")
        sys.exit(0)
