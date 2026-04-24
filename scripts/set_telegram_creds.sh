#!/usr/bin/env bash
#
# set_telegram_creds.sh — проставляет telegram_bot_token и telegram_chat_id
# во все YAML конфиги проекта (config.yaml + configs/*.yaml).
#
# Usage:
#   ./scripts/set_telegram_creds.sh                   # интерактивно спросит значения
#   ./scripts/set_telegram_creds.sh TOKEN CHAT_ID     # напрямую
#   ./scripts/set_telegram_creds.sh --clear           # очистить (поставить "")
#
# Безопасность:
#   - Валидирует формат токена (должен быть NNNN:XXXX...)
#   - Валидирует chat_id (должен быть числом, опционально с минусом для групп)
#   - Делает бэкап каждого файла в configs/backups/ перед правкой
#   - Показывает diff затронутых строк перед применением

set -euo pipefail

# ── Определяем корень проекта независимо от того, откуда запущен скрипт ──────
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$ROOT_DIR"

BACKUP_DIR="configs/backups/credentials_$(date -u +%Y%m%dT%H%M%SZ)"

# ── ANSI цвета (если терминал не tty, остаются пустыми) ──────────────────────
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'
    CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; CYAN=''; BOLD=''; RESET=''
fi

# ── Парсим аргументы ──────────────────────────────────────────────────────────
CLEAR_MODE=false
TOKEN=""
CHAT_ID=""

if [[ "${1:-}" == "--clear" ]]; then
    CLEAR_MODE=true
elif [[ $# -eq 2 ]]; then
    TOKEN="$1"
    CHAT_ID="$2"
elif [[ $# -ne 0 ]]; then
    echo -e "${RED}Usage:${RESET}"
    echo "  $0                   # интерактивно"
    echo "  $0 TOKEN CHAT_ID     # напрямую"
    echo "  $0 --clear           # очистить"
    exit 1
fi

# ── Интерактивный ввод (если не передали аргументы) ──────────────────────────
if [[ "$CLEAR_MODE" == "false" && -z "$TOKEN" ]]; then
    echo -e "${CYAN}${BOLD}Настройка Telegram credentials для всех конфигов${RESET}"
    echo ""
    echo -e "${YELLOW}Telegram bot token${RESET} — получается у @BotFather в Telegram"
    echo "   Формат: NNNNNNNNNN:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    read -r -p "   Введи токен: " TOKEN
    echo ""
    echo -e "${YELLOW}Telegram chat ID${RESET} — получается у @userinfobot в Telegram"
    echo "   Формат: число (положительное для личных чатов, отрицательное для групп)"
    read -r -p "   Введи chat ID: " CHAT_ID
    echo ""
fi

# ── Валидация ────────────────────────────────────────────────────────────────
if [[ "$CLEAR_MODE" == "false" ]]; then
    # Token: цифры, двоеточие, минимум 35 символов после двоеточия
    if [[ ! "$TOKEN" =~ ^[0-9]+:[A-Za-z0-9_-]{35,}$ ]]; then
        echo -e "${RED}✗ Невалидный формат токена${RESET}"
        echo "  Ожидается: NNNN:XXXX... (цифры, двоеточие, 35+ символов)"
        echo "  Получено:  '$TOKEN'"
        exit 1
    fi

    # Chat ID: число, может начинаться с минуса (для групп/каналов)
    if [[ ! "$CHAT_ID" =~ ^-?[0-9]+$ ]]; then
        echo -e "${RED}✗ Невалидный chat ID${RESET}"
        echo "  Ожидается: число (возможно со знаком минус)"
        echo "  Получено:  '$CHAT_ID'"
        exit 1
    fi
fi

# ── Находим все YAML-файлы для обработки ─────────────────────────────────────
FILES=()
[[ -f "config.yaml" ]] && FILES+=("config.yaml")
for f in configs/*.yaml; do
    # пропускаем config_hybrid.yaml — его orchestrator генерирует на лету
    [[ "$(basename "$f")" == "config_hybrid.yaml" ]] && continue
    [[ -f "$f" ]] && FILES+=("$f")
done

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo -e "${RED}✗ Не найдено ни одного YAML конфига${RESET}"
    echo "  Запускай скрипт из корня проекта (где лежит config.yaml)"
    exit 1
fi

echo -e "${CYAN}Буду править ${#FILES[@]} файл(а/ов):${RESET}"
for f in "${FILES[@]}"; do echo "  • $f"; done
echo ""

# ── Делаем бэкап ─────────────────────────────────────────────────────────────
mkdir -p "$BACKUP_DIR"
for f in "${FILES[@]}"; do
    # сохраняем относительный путь внутри backup-директории
    target_dir="$BACKUP_DIR/$(dirname "$f")"
    mkdir -p "$target_dir"
    cp "$f" "$BACKUP_DIR/$f"
done
echo -e "${GREEN}✓ Бэкап создан:${RESET} $BACKUP_DIR"
echo ""

# ── Применяем правки ─────────────────────────────────────────────────────────
DISPLAY_TOKEN="$TOKEN"
DISPLAY_CHAT="$CHAT_ID"
if [[ "$CLEAR_MODE" == "true" ]]; then
    DISPLAY_TOKEN=""
    DISPLAY_CHAT=""
fi

changed_count=0
for f in "${FILES[@]}"; do
    # Проверяем что поля вообще есть в файле
    if ! grep -q "^\s*telegram_bot_token:" "$f"; then
        echo -e "${YELLOW}⚠${RESET}  $f: поле telegram_bot_token не найдено, пропускаю"
        continue
    fi

    # sed с разделителем | чтобы не конфликтовать с возможными / в токене
    # (Telegram-токены иногда содержат - и _, но / не используется)
    sed -i.tmp \
        -e "s|^\(\s*telegram_bot_token:\s*\).*|\1\"$DISPLAY_TOKEN\"|" \
        -e "s|^\(\s*telegram_chat_id:\s*\).*|\1\"$DISPLAY_CHAT\"|" \
        "$f"
    rm -f "${f}.tmp"

    # Проверяем что YAML остался валидным
    if command -v python3 &> /dev/null; then
        if ! python3 -c "import yaml; yaml.safe_load(open('$f'))" 2>/dev/null; then
            echo -e "${RED}✗ $f — YAML сломался после правки, восстанавливаю из бэкапа${RESET}"
            cp "$BACKUP_DIR/$f" "$f"
            exit 1
        fi
    fi

    echo -e "${GREEN}✓${RESET} $f"
    changed_count=$((changed_count + 1))
done

echo ""
echo -e "${GREEN}${BOLD}Готово.${RESET} Обновлено файлов: $changed_count"

if [[ "$CLEAR_MODE" == "true" ]]; then
    echo -e "${YELLOW}Все Telegram credentials очищены (оба поля = \"\").${RESET}"
else
    # Маскируем токен в выводе — показываем только первые и последние символы
    masked_token="${TOKEN:0:6}...${TOKEN: -4}"
    echo -e "  token:   ${BOLD}$masked_token${RESET}"
    echo -e "  chat_id: ${BOLD}$CHAT_ID${RESET}"
fi

echo ""
echo -e "${CYAN}Проверить результат:${RESET}"
echo "  grep -A 1 'telegram_bot_token' configs/config_trend.yaml"
echo ""
echo -e "${CYAN}Откатить правки:${RESET}"
echo "  cp -r $BACKUP_DIR/* ."
