"""
scripts/reporter.py — Read-only analytics & Telegram reporter for all trading bots.

Scans configs/ for *.yaml files, reads corresponding state files from data/,
builds a summary report and sends it to Telegram.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
CONFIGS_DIR = ROOT / "configs"
DATA_DIR = ROOT / "data"

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_configs() -> list[dict]:
    """Return list of parsed configs from configs/*.yaml (sorted by filename)."""
    if not CONFIGS_DIR.exists():
        print(f"[WARN] configs/ directory not found at {CONFIGS_DIR}. ")
        return []

    # 🟢 БЕЛЫЙ СПИСОК: Репортер будет читать ТОЛЬКО эти файлы
    ALLOWED_LIST = [
        "config_hybrid.yaml", 
        "config_scalper.yaml", 
        "config_balanced.yaml"
    ]

    configs = []
    for path in sorted(CONFIGS_DIR.glob("*.yaml")):
        # Если файла нет в белом списке - жестко игнорируем его
        if path.name not in ALLOWED_LIST:
            continue

        try:
            with open(path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            cfg["_source"] = str(path)
            configs.append(cfg)
        except Exception as exc:
            print(f"[WARN] Failed to parse {path.name}: {exc}")

    return configs


def load_state(state_filepath: str) -> dict | None:
    """Load state JSON for a bot. Returns None if file is missing or invalid."""
    # Извлекаем только имя файла, чтобы избежать дублирования путей (например, data/data/state.json)
    filename = Path(state_filepath).name
    path = DATA_DIR / filename
    
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"[WARN] Failed to read {path.name}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Per-bot analytics
# ---------------------------------------------------------------------------

def analyse_bot(cfg: dict) -> dict:
    """Extract all metrics for one bot config. Returns a metrics dict."""
    strategy_name = cfg.get("strategy", {}).get("name", "Unknown")
    
    # 🌟 МАГИЯ ДЛЯ ДИРИЖЕРА: Если это гибридный конфиг, переименовываем его для отчета
    if "hybrid" in cfg.get("_source", "").lower():
        strategy_name = "🧠 Дирижер (Hybrid)"

    initial_balance = cfg.get("risk_management", {}).get("initial_balance_usd", 1000.0)
    
    # Ищем путь к стейт-файлу в разных блоках (на случай разной структуры конфигов)
    state_file = cfg.get("storage", {}).get("state_file") or cfg.get("simulation", {}).get("state_file", "state.json")

    state = load_state(state_file)

    if state is None:
        return {
            "name": strategy_name,
            "initial_balance": initial_balance,
            "state_file": state_file,
            "available": False,
        }

    portfolio = state.get("virtual_portfolio", {})
    balance = portfolio.get("balance_usd", initial_balance)
    pnl = balance - initial_balance
    pnl_pct = (pnl / initial_balance * 100) if initial_balance else 0.0

    history = state.get("trade_history", [])
    wins = sum(1 for t in history if t.get("result") == "WIN")
    losses = sum(1 for t in history if t.get("result") == "LOSS")
    draws = sum(1 for t in history if t.get("result") == "DRAW")
    time_stops = sum(1 for t in history if t.get("result") == "TIME_STOP")
    rev_closes = sum(1 for t in history if t.get("result") == "REVERSE_CLOSE")

    tp_count = sum(1 for t in history if t.get("result") == "TP")
    sl_count = sum(1 for t in history if t.get("result") == "SL")

    tp_wins = sum(1 for t in history if t.get("result") == "TP" and t.get("pnl", 0) > 0)
    sl_wins = sum(1 for t in history if t.get("result") == "SL" and t.get("pnl", 0) > 0)
    total_decided = wins + losses + tp_count + sl_count
    total_wins = wins + tp_wins + sl_wins
    win_rate = (total_wins / total_decided * 100) if total_decided else 0.0

    # 🚨 HEALTH CHECK: Проверяем, не завис ли бот (если стейт не обновлялся больше 20 минут)
    is_dead = False
    last_update_str = portfolio.get("last_update")
    if last_update_str:
        try:
            last_update = datetime.fromisoformat(last_update_str)
            # Сравниваем с текущим временем UTC
            if (datetime.now(timezone.utc) - last_update).total_seconds() > 20 * 60:
                is_dead = True
        except ValueError:
            pass

    active = portfolio.get("active_position")
    
    if is_dead:
        status_str = "🔴 ОСТАНОВЛЕН (Нет связи / Пауза)"
    elif active:
        side = active.get("side", "?")
        status_str = f"📈 В сделке ({side})"
    else:
        status_str = "⏳ Ожидание"

    return {
        "name": strategy_name,
        "initial_balance": initial_balance,
        "balance": balance,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "time_stops": time_stops,
        "rev_closes": rev_closes,
        "win_rate": win_rate,
        "status": status_str,
        "available": True,
    }

# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def sign(value: float) -> str:
    return f"+${value:.2f}" if value >= 0 else f"-${abs(value):.2f}"

def build_report(bots: list[dict]) -> str:
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%-d %B, %H:%M UTC")

    available = [b for b in bots if b["available"]]
    unavailable = [b for b in bots if not b["available"]]

    available.sort(key=lambda b: b["pnl"], reverse=True)

    lines = [
        "📊 *Polymarket Алготрейдинг | Сводка*",
        f"⏱ {date_str}",
        "",
    ]

    if available:
        leader = available[0]
        loser = available[-1]
        lines.append(f"🏆 *ЛИДЕР:* {leader['name']} ({sign(leader['pnl'])})")
        lines.append(f"🤬 *АУТСАЙДЕР:* {loser['name']} ({sign(loser['pnl'])})")
        lines.append("")

    for i, bot in enumerate(available, start=1):
        pnl_sign = "+" if bot["pnl"] >= 0 else ""
        pnl_str = f"{pnl_sign}${bot['pnl']:.2f}"
        pnl_pct_str = f"{pnl_sign}{bot['pnl_pct']:.2f}%"
        win_icon = "🟢" if bot["wins"] > 0 else ""
        loss_icon = "🔴" if bot["losses"] > 0 else ""
        draw_icon = "⚪" if bot.get("draws", 0) > 0 else ""

        draw_str = f" | {bot.get('draws', 0)} {draw_icon}" if bot.get("draws", 0) > 0 else ""
        ts_str = f" | {bot.get('time_stops', 0)} ⏱" if bot.get("time_stops", 0) > 0 else ""
        rc_str = f" | {bot.get('rev_closes', 0)} 🔄" if bot.get("rev_closes", 0) > 0 else ""
        lines += [
            f"🤖 *{i}. {bot['name']}*",
            f"├ 💰 Баланс: ${bot['balance']:.2f} ({pnl_pct_str})",
            f"├ 🎯 Win Rate: {bot['win_rate']:.0f}% "
            f"({bot['wins']} {win_icon} | {bot['losses']} {loss_icon}{draw_str}{ts_str}{rc_str})",
            f"└ 🔄 Статус: {bot['status']}",
            "",
        ]

    for bot in unavailable:
        lines += [
            f"🤖 *{bot['name']}*",
            f"└ ⚠️ Нет данных ({bot['state_file']} не найден)",
            "",
        ]

    if available:
        total_balance = sum(b["balance"] for b in available)
        total_initial = sum(b["initial_balance"] for b in available)
        total_pnl = total_balance - total_initial
        total_sign = "+" if total_pnl >= 0 else ""

        lines += [
            "💼 *ОБЩИЙ ИТОГ СИСТЕМЫ:*",
            f"💵 Капитал: ${total_balance:.2f} (Старт: ${total_initial:.2f})",
            f"📈 Общий PnL: {total_sign}${total_pnl:.2f}",
        ]

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(text: str, token: str, chat_id: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }
    
    # ❌ ПРОКСИ УБРАНЫ: Запрос идет напрямую через чистый интернет сервера
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        print("[OK] Report sent to Telegram.")
    except requests.RequestException as exc:
        print(f"[ERROR] Telegram send failed: {exc}")
        sys.exit(1)

def extract_telegram_creds(configs: list[dict]) -> tuple[str, str]:
    """Pick Telegram token and chat_id from the first config that has them."""
    for cfg in configs:
        endpoints = cfg.get("endpoints", {})
        token = endpoints.get("telegram_bot_token", "")
        chat_id = str(endpoints.get("telegram_chat_id", ""))
        if token and chat_id:
            return token, chat_id
    raise ValueError("No Telegram credentials found in any config file.")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    configs = load_configs()
    if not configs:
        print("[ERROR] No configs loaded. Exiting.")
        sys.exit(1)

    bots = [analyse_bot(cfg) for cfg in configs]

    report = build_report(bots)
    print("\n" + report + "\n")

    token, chat_id = extract_telegram_creds(configs)
    send_telegram(report, token, chat_id)

if __name__ == "__main__":
    main()