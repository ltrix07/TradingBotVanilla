"""
scripts/review_bot.py — long-polling Telegram callback handler.

Keeps the process alive, polls getUpdates every few seconds, and responds to
Apply / Reject button clicks on daily-review messages.

Run alongside the main bot:
    python scripts/review_bot.py &

Or as a systemd service.

Wire protocol
-------------
daily_review.py sends a message with inline buttons carrying callback_data:
    apply:2026-04-21
    reject:2026-04-21

This script listens for those callbacks and dispatches:
    apply → runs apply_review.py <date> --confirm
    reject → runs apply_review.py <date> --reject "from telegram"

Rules
-----
- Only responds to messages from the chat_id configured in any configs/*.yaml.
- Never applies anything without an explicit button click.
- Sends confirmation back into the same chat after each action.
- Stores getUpdates offset in data/reviews/.telegram_offset so restarts
  don't replay old callbacks.
"""

from __future__ import annotations

import json
import logging
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import requests
import yaml

ROOT        = Path(__file__).resolve().parent.parent
CONFIGS_DIR = ROOT / "configs"
REVIEWS_DIR = ROOT / "data" / "reviews"
OFFSET_FILE = REVIEWS_DIR / ".telegram_offset"
APPLY_SCRIPT = ROOT / "scripts" / "apply_review.py"

REVIEWS_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("review_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

# callback_data formats we accept (review_id = YYYY-MM-DD)
_CALLBACK_RE = re.compile(r"^(apply|reject):(\d{4}-\d{2}-\d{2})$")

POLL_TIMEOUT = 25    # long-poll seconds
LOOP_SLEEP   = 1     # fallback sleep on errors


# --- Credentials & chat whitelist -------------------------------------------

def _read_telegram_creds() -> tuple[str, str]:
    """First (token, chat_id) pair found in configs/*.yaml."""
    for cfg_path in sorted(CONFIGS_DIR.glob("*.yaml")):
        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            ep = cfg.get("endpoints", {})
            token = ep.get("telegram_bot_token", "")
            chat  = str(ep.get("telegram_chat_id", ""))
            if token and chat:
                return token, chat
        except Exception:
            continue
    raise RuntimeError(
        "No telegram_bot_token / telegram_chat_id found in any configs/*.yaml"
    )


# --- Offset persistence -----------------------------------------------------

def _read_offset() -> int:
    try:
        return int(OFFSET_FILE.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return 0


def _write_offset(offset: int) -> None:
    OFFSET_FILE.write_text(str(offset), encoding="utf-8")


# --- Telegram API helpers ---------------------------------------------------

def _api(token: str, method: str, **params) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    resp = requests.post(url, json=params, timeout=POLL_TIMEOUT + 5)
    resp.raise_for_status()
    return resp.json()


def _answer_callback(token: str, callback_id: str, text: str = "") -> None:
    """ACK the callback so the button spinner stops."""
    try:
        _api(token, "answerCallbackQuery",
             callback_query_id=callback_id, text=text[:200])
    except Exception as exc:
        log.warning("answerCallbackQuery failed: %s", exc)


def _send_message(token: str, chat_id: str, text: str) -> None:
    try:
        _api(token, "sendMessage",
             chat_id=chat_id, text=text, parse_mode="Markdown")
    except Exception as exc:
        log.warning("sendMessage failed: %s", exc)


# --- Action dispatch --------------------------------------------------------

def _run_apply(review_id: str, reject_note: Optional[str] = None) -> tuple[int, str]:
    """Invoke scripts/apply_review.py as a subprocess. Returns (rc, combined_output)."""
    if reject_note is not None:
        cmd = [sys.executable, str(APPLY_SCRIPT), review_id, "--reject", reject_note]
    else:
        cmd = [sys.executable, str(APPLY_SCRIPT), review_id, "--confirm"]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, cwd=str(ROOT),
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, output.strip()
    except subprocess.TimeoutExpired:
        return 1, "apply_review.py timed out after 60s"
    except Exception as exc:
        return 1, f"apply_review.py invocation failed: {exc}"


def _handle_callback(token: str, allowed_chat: str, cb: dict) -> None:
    cb_id    = cb.get("id")
    data     = cb.get("data", "")
    from_chat = str(cb.get("message", {}).get("chat", {}).get("id", ""))
    user     = cb.get("from", {}).get("username", "?")

    # Whitelist: only our chat_id
    if from_chat != allowed_chat:
        log.warning("Callback from unauthorised chat %s (user=%s) — ignored", from_chat, user)
        _answer_callback(token, cb_id, "Unauthorised.")
        return

    m = _CALLBACK_RE.match(data)
    if not m:
        log.info("Ignoring unrecognised callback data: %r", data)
        _answer_callback(token, cb_id, "Unknown action.")
        return

    action, review_id = m.group(1), m.group(2)
    log.info("Callback %s for review %s from user=%s", action, review_id, user)

    if action == "apply":
        _answer_callback(token, cb_id, "Applying…")
        rc, out = _run_apply(review_id)
        short = out[-1500:] if len(out) > 1500 else out
        prefix = "✅ *Applied*" if rc == 0 else "⚠️ *Apply exited with errors*"
        _send_message(token, allowed_chat,
                      f"{prefix} `{review_id}`\n```\n{short}\n```")

    elif action == "reject":
        _answer_callback(token, cb_id, "Rejected.")
        rc, out = _run_apply(review_id, reject_note="user-rejected via telegram")
        prefix = "❌ *Rejected*" if rc == 0 else "⚠️ *Reject logging failed*"
        _send_message(token, allowed_chat,
                      f"{prefix} `{review_id}`")


# --- Main loop --------------------------------------------------------------

_running = True


def _shutdown(*_):
    global _running
    log.info("Shutdown signal received.")
    _running = False


def main() -> None:
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    token, allowed_chat = _read_telegram_creds()
    offset = _read_offset()

    log.info("review_bot started (chat=%s, offset=%d)", allowed_chat, offset)

    global _running
    while _running:
        try:
            result = _api(
                token, "getUpdates",
                offset=offset,
                timeout=POLL_TIMEOUT,
                allowed_updates=["callback_query"],
            )
            for upd in result.get("result", []):
                upd_id = upd.get("update_id", 0)
                offset = max(offset, upd_id + 1)
                cb = upd.get("callback_query")
                if cb:
                    try:
                        _handle_callback(token, allowed_chat, cb)
                    except Exception as exc:
                        log.exception("Callback handler crashed: %s", exc)
                _write_offset(offset)
        except requests.exceptions.Timeout:
            # Long-poll just timed out — nothing happened. Loop again.
            continue
        except Exception as exc:
            log.warning("Polling error: %s — sleeping %ds", exc, LOOP_SLEEP)
            time.sleep(LOOP_SLEEP)

    log.info("review_bot exited cleanly.")


if __name__ == "__main__":
    main()
