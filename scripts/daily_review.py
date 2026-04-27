"""
scripts/daily_review.py — LLM-powered daily post-trade review.

Run from cron at 00:05 UTC:
    5 0 * * * cd /path/to/bot && /usr/bin/python3 scripts/daily_review.py

What it does
------------
1. Loads the last 24h of trades from data/trades_hybrid.csv (fallback: all
   data/trades_*.csv files), joined with data/market_regime.csv.
2. Detects which configs actually ran in that window (via timestamps of
   individual CSVs). Only those go into the prompt.
3. Computes local statistics (PnL, win-rate by pnl sign, breakdown by ADX
   bucket, distribution of exit codes, average hold time, funding impact).
4. Detects anomalies that warrant an IMMEDIATE Telegram alert regardless of
   review outcome (drawdown, stuck position, funding spike).
5. If there were fewer than MIN_TRADES_FOR_REVIEW, skips the Claude call
   entirely and sends "not enough data" to Telegram.
6. Otherwise builds a structured prompt for Claude Opus 4.7, reading the
   current config values and the 30-day apply-changelog (facts only —
   no preferences fed back to the model).
7. Saves the full review (stats + prompt + response + usage + proposed
   diffs) to data/reviews/YYYY-MM-DD.json.
8. Sends a short summary to Telegram with inline Apply/Reject buttons.

Non-goals
---------
- This script NEVER writes to configs. Applying a diff is the job of
  scripts/apply_review.py (manual CLI) or scripts/review_bot.py (Telegram).
- Proposed diffs that would breach sanity limits (see SANITY_LIMITS) are
  still saved in the review JSON but clearly flagged; apply_review.py will
  refuse to apply them.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml

# --- Paths --------------------------------------------------------------------
ROOT        = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "config.yaml"
DATA_DIR    = ROOT / "data"
REVIEWS_DIR = DATA_DIR / "reviews"
CHANGELOG   = REVIEWS_DIR / "history.jsonl"

REVIEWS_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("daily_review")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


# --- Model / pricing ---------------------------------------------------------
MODEL_ID         = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 4096
INPUT_PRICE_PER_MTOK  = 5.0   # USD per 1M input tokens
OUTPUT_PRICE_PER_MTOK = 25.0  # USD per 1M output tokens

# Minimum trades below which Claude is not called (statistically insignificant)
MIN_TRADES_FOR_REVIEW = 5


# --- Sanity limits — hard bounds applied in apply_review.py ------------------
# The same table is embedded into the prompt so Claude knows the envelope.
SANITY_LIMITS: dict[str, tuple[float, float]] = {
    # Risk management
    "risk_management.risk_per_trade_pct":          (0.003, 0.03),
    "risk_management.position_size_pct":           (0.01,  0.15),
    "risk_management.max_daily_loss_pct":          (0.02,  0.15),
    "risk_management.atr_sl_multiplier":           (1.0,   10.0),
    "risk_management.atr_tp_multiplier":           (1.0,   20.0),
    "risk_management.trailing_stop_atr_multiplier": (0.5,  5.0),
    "risk_management.breakeven_atr_multiplier":    (0.0,   5.0),
    "risk_management.max_hold_time_minutes":       (60,    6000),
    "risk_management.max_funding_rate_bps":        (10,    200),
    "risk_management.sl_confirm_seconds":          (0,     60),
    "risk_management.cooldown_after_sl_sec":       (0,     600),
    "risk_management.time_stop_seconds":           (60,    14400),
    "risk_management.time_stop_max_loss_pct":      (0.0,   0.02),
    "exchange.leverage":                           (1,     10),
    # VolBreakout strategy parameters
    "strategy.vol_breakout.bb_period":             (10,    50),
    "strategy.vol_breakout.squeeze_bw_pct":        (0.5,   5.0),
}


# --- Anomaly thresholds ------------------------------------------------------
DRAWDOWN_ALERT_PCT    = 0.05   # balance down > 5% from start of day
STUCK_POSITION_HOURS  = 4.0    # position open > 4h without close
FUNDING_SPIKE_BPS     = 75.0   # |funding| > 75 bps


# =============================================================================
# Data loading
# =============================================================================

def _load_trades(window_hours: float = 24.0) -> "pd.DataFrame":
    """Load trades from the last `window_hours` from trades.csv (simulation log).

    The VolBreakout bot writes all trades to ROOT/trades.csv (configured in
    config.yaml → simulation.log_file).
    """
    import pandas as pd

    # Primary: ROOT/trades.csv (from config simulation.log_file)
    csv_path = ROOT / "trades.csv"
    if not csv_path.exists():
        log.info("trades.csv not found — no trades yet.")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        log.warning("Failed to read trades.csv: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["strategy"] = "VolBreakout"

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    return df[df["timestamp"] >= cutoff].copy()


def _load_regime() -> Optional["pd.DataFrame"]:
    """Load market_regime.csv if present. Returns None if the file is missing."""
    import pandas as pd

    path = DATA_DIR / "market_regime.csv"
    if not path.exists():
        log.info("No market_regime.csv — regime-aware analysis disabled.")
        return None
    try:
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df.dropna(subset=["timestamp"]).sort_values("timestamp")
    except Exception as exc:
        log.warning("Failed to read market_regime.csv: %s", exc)
        return None


def _load_state() -> Optional[dict]:
    """Load state.json from project root (VolBreakout single-strategy bot)."""
    path = ROOT / "state.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log.warning("Failed to read state.json: %s", exc)
        return None


def _load_active_configs() -> list[dict]:
    """Return the single VolBreakout config from ROOT/config.yaml."""
    if not CONFIG_FILE.exists():
        log.warning("config.yaml not found at %s", CONFIG_FILE)
        return []
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        cfg["_filename"] = "config.yaml"
        return [cfg]
    except Exception as exc:
        log.warning("Failed to parse config.yaml: %s", exc)
        return []


def _load_changelog(days: int = 30) -> list[dict]:
    """Return the last `days` worth of config-change entries (facts only)."""
    if not CHANGELOG.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    try:
        with open(CHANGELOG, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = entry.get("timestamp")
                if ts:
                    try:
                        dt = datetime.fromisoformat(ts)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt < cutoff:
                            continue
                    except ValueError:
                        pass
                out.append(entry)
    except Exception as exc:
        log.warning("Failed to read changelog: %s", exc)
    return out


# =============================================================================
# Local statistics (before any LLM call)
# =============================================================================

def _compute_stats(trades: "pd.DataFrame", regime: Optional["pd.DataFrame"]) -> dict:
    """Compute deterministic stats from the trade DataFrame."""
    import pandas as pd

    if trades.empty:
        return {"total_trades": 0}

    total = len(trades)
    winning = int((trades["pnl"] > 0).sum())
    losing  = int((trades["pnl"] < 0).sum())
    flat    = total - winning - losing

    pnl_total = float(trades["pnl"].sum())
    pnl_mean  = float(trades["pnl"].mean())
    pnl_std   = float(trades["pnl"].std()) if total > 1 else 0.0

    # By side (LONG/SHORT)
    by_side = {}
    for side in trades["side"].unique():
        sub = trades[trades["side"] == side]
        by_side[str(side)] = {
            "count":    int(len(sub)),
            "total_pnl": round(float(sub["pnl"].sum()), 2),
            "avg_pnl":   round(float(sub["pnl"].mean()), 2),
            "win_rate":  round(float((sub["pnl"] > 0).mean()) * 100, 1),
        }

    # By result code
    by_result = {}
    for res in trades["result"].unique():
        sub = trades[trades["result"] == res]
        by_result[str(res)] = {
            "count":    int(len(sub)),
            "total_pnl": round(float(sub["pnl"].sum()), 2),
            "avg_pnl":   round(float(sub["pnl"].mean()), 2),
        }

    # By strategy (only the ones that traded)
    by_strategy = {}
    for strat in trades["strategy"].unique():
        sub = trades[trades["strategy"] == strat]
        by_strategy[str(strat)] = {
            "count":    int(len(sub)),
            "total_pnl": round(float(sub["pnl"].sum()), 2),
            "avg_pnl":   round(float(sub["pnl"].mean()), 2),
            "win_rate":  round(float((sub["pnl"] > 0).mean()) * 100, 1),
        }

    # By ADX regime (if market_regime.csv is available)
    by_adx = None
    if regime is not None and not regime.empty:
        try:
            joined = pd.merge_asof(
                trades.sort_values("timestamp"),
                regime[["timestamp", "adx_15m", "atr_15m"]].sort_values("timestamp"),
                on="timestamp",
                direction="backward",
                tolerance=pd.Timedelta(minutes=30),
            )
            joined["adx_bucket"] = pd.cut(
                joined["adx_15m"],
                bins=[0, 20, 35, 100],
                labels=["weak_<20", "medium_20-35", "strong_>=35"],
            )
            by_adx = {}
            for bucket, sub in joined.groupby("adx_bucket", observed=True):
                if len(sub) == 0:
                    continue
                by_adx[str(bucket)] = {
                    "count":    int(len(sub)),
                    "total_pnl": round(float(sub["pnl"].sum()), 2),
                    "avg_pnl":   round(float(sub["pnl"].mean()), 2),
                    "win_rate":  round(float((sub["pnl"] > 0).mean()) * 100, 1),
                }
        except Exception as exc:
            log.warning("ADX join failed: %s", exc)
            by_adx = None

    return {
        "total_trades":  total,
        "winning":       winning,
        "losing":        losing,
        "flat":          flat,
        "win_rate_pct":  round(winning / total * 100, 1) if total else 0.0,
        "total_pnl_usd": round(pnl_total, 2),
        "avg_pnl_usd":   round(pnl_mean, 4),
        "pnl_std_usd":   round(pnl_std, 4),
        "by_side":       by_side,
        "by_result":     by_result,
        "by_strategy":   by_strategy,
        "by_adx_bucket": by_adx,
    }


# =============================================================================
# Anomaly detection
# =============================================================================

def _detect_anomalies(state: Optional[dict], trades: "pd.DataFrame") -> list[dict]:
    """Return a list of anomaly dicts for immediate Telegram alerts."""
    import pandas as pd

    anomalies: list[dict] = []

    # 1. Drawdown: daily_pnl breached DRAWDOWN_ALERT_PCT of initial balance
    if state is not None:
        pf = state.get("virtual_portfolio", {})
        initial = float(pf.get("initial_balance_usd", 1000.0))
        # virtual_portfolio doesn't always carry initial; prefer state.json's ctor value
        daily_pnl = float(pf.get("daily_pnl", 0.0))
        if daily_pnl < -DRAWDOWN_ALERT_PCT * initial:
            anomalies.append({
                "type":     "drawdown",
                "severity": "high",
                "detail":   f"daily_pnl ${daily_pnl:+.2f} "
                            f"breached {DRAWDOWN_ALERT_PCT*100:.0f}% of ${initial:.0f}",
            })

        # 2. Stuck position: active position open > STUCK_POSITION_HOURS
        pos = pf.get("active_position")
        if pos and pos.get("timestamp"):
            try:
                pos_dt = datetime.fromisoformat(pos["timestamp"])
                if pos_dt.tzinfo is None:
                    pos_dt = pos_dt.replace(tzinfo=timezone.utc)
                age_h = (datetime.now(timezone.utc) - pos_dt).total_seconds() / 3600
                if age_h > STUCK_POSITION_HOURS:
                    anomalies.append({
                        "type":     "stuck_position",
                        "severity": "medium",
                        "detail":   f"{pos.get('side','?')} on {pos.get('symbol','?')} "
                                    f"open for {age_h:.1f}h",
                    })
            except ValueError:
                pass

    # 3. Funding spike is detected in main.py loop, not here — but we can flag
    # if recent trades had fee/slippage patterns that suggest funding pain.
    # This is a proxy. Real funding-rate logging would require adding it to CSVs.

    # 4. Many losses in a row (tail risk signal)
    if not trades.empty:
        trades_sorted = trades.sort_values("timestamp")
        streak = 0
        max_streak = 0
        for pnl in trades_sorted["pnl"]:
            if pnl < 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        if max_streak >= 5:
            anomalies.append({
                "type":     "loss_streak",
                "severity": "medium",
                "detail":   f"{max_streak} consecutive losses in last 24h",
            })

    return anomalies


# =============================================================================
# Prompt building
# =============================================================================

_SYSTEM_PROMPT = """You are a quantitative trading analyst reviewing a crypto-futures paper-trading bot's last 24 hours.

The bot runs a single strategy: **Volatility Breakout (Bollinger Squeeze)**.
- It detects low-volatility compression (BB bandwidth < squeeze threshold).
- When the squeeze ends and price breaks beyond the compressed range:
  - Close above upper BB → LONG
  - Close below lower BB → SHORT
- SL/TP are ATR-based (dynamic). Trailing stop is enabled.
- If price returns to BB mid while in a position, a "reverse close" triggers.
- The strategy trades 1h candles on Binance Futures (perpetual).

Your output MUST be a single valid JSON object with this exact schema:

{
  "summary": "<2–3 sentence plain-English verdict on the day>",
  "insights": [
    "<one concrete observation grounded in the numbers provided>",
    ...
  ],
  "proposed_diffs": [
    {
      "file": "config.yaml",
      "path": "risk_management.atr_sl_multiplier",
      "current": 1.75,
      "proposed": 2.0,
      "reason": "<one sentence citing the statistic that justifies the change>"
    },
    ...
  ],
  "confidence": "<low|medium|high — your confidence in the proposed diffs given the sample size>"
}

Rules:
- Never propose a value outside the SANITY_LIMITS range given below.
- Do not propose diffs if total_trades < 10: insights only.
- "path" uses dotted notation for nested YAML keys.
- Every diff must cite the stat from the numbers block — vague reasoning is rejected.
- If the day was statistically unremarkable (|total_pnl_usd| small, win-rate in [0.45, 0.55]), say so and propose no diffs.
- The strategy was backtested and optimized: bb_period=22, squeeze_bw_pct=2.0, sl_atr=1.75, tp_atr=4.0. Only propose changes to these if there is strong statistical evidence.
- Output JSON only. No markdown fences. No prose outside the JSON object."""


def _build_prompt(
    stats: dict,
    anomalies: list[dict],
    configs: list[dict],
    changelog: list[dict],
) -> str:
    """Build the user-turn prompt for Claude."""
    payload = {
        "window": "last 24 hours, UTC",
        "stats": stats,
        "anomalies_detected_locally": anomalies,
        "active_configs": [
            {
                "filename": c.get("_filename"),
                "strategy_name": c.get("strategy", {}).get("name"),
                "risk_management": c.get("risk_management", {}),
                "vol_breakout": c.get("strategy", {}).get("vol_breakout", {}),
                "timeframe": c.get("strategy", {}).get("timeframe"),
                "exchange_leverage": c.get("exchange", {}).get("leverage"),
                "exchange_symbol": c.get("exchange", {}).get("symbol"),
            }
            for c in configs
        ],
        "sanity_limits": {
            path: {"min": lo, "max": hi}
            for path, (lo, hi) in SANITY_LIMITS.items()
        },
        "recent_config_changelog_30d": changelog,
    }
    return (
        "Review this trading day and return your JSON analysis.\n\n"
        "Numbers and configs below:\n\n"
        + json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    )


# =============================================================================
# Claude API call
# =============================================================================

def _call_claude(system_prompt: str, user_prompt: str) -> tuple[dict, dict]:
    """Call Claude Opus 4.7 and return (parsed_json_response, usage_dict).

    Raises RuntimeError on failure.
    """
    try:
        from anthropic import Anthropic
    except ImportError as exc:
        raise RuntimeError(
            "anthropic package not installed — add 'anthropic>=0.40.0' to requirements.txt"
        ) from exc

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var is not set")

    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model=MODEL_ID,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    # Extract text from content blocks (usually a single text block here)
    text_chunks = [
        block.text for block in response.content
        if getattr(block, "type", None) == "text"
    ]
    raw_text = "\n".join(text_chunks).strip()

    # Strip code fences if present
    if raw_text.startswith("```"):
        lines = raw_text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        raw_text = "\n".join(lines).strip()

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Claude returned non-JSON response:\n---\n{raw_text[:2000]}\n---\n{exc}"
        ) from exc

    usage = {
        "input_tokens":  response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "cost_usd": round(
            response.usage.input_tokens  / 1_000_000 * INPUT_PRICE_PER_MTOK  +
            response.usage.output_tokens / 1_000_000 * OUTPUT_PRICE_PER_MTOK,
            4,
        ),
    }
    return parsed, usage


# =============================================================================
# Sanity check on proposed diffs
# =============================================================================

def _validate_diffs(diffs: list[dict]) -> list[dict]:
    """Tag each diff with validation_status: ok | out_of_bounds | unknown_path."""
    validated = []
    for diff in diffs:
        path = str(diff.get("path", ""))
        proposed = diff.get("proposed")
        tag = {
            "validation_status": "ok",
            "validation_reason": "",
        }
        if path not in SANITY_LIMITS:
            tag["validation_status"] = "unknown_path"
            tag["validation_reason"] = f"'{path}' is not in the approved sanity table"
        else:
            lo, hi = SANITY_LIMITS[path]
            try:
                val = float(proposed)
                if not (lo <= val <= hi):
                    tag["validation_status"] = "out_of_bounds"
                    tag["validation_reason"] = (
                        f"proposed {val} outside [{lo}, {hi}]"
                    )
            except (TypeError, ValueError):
                tag["validation_status"] = "non_numeric"
                tag["validation_reason"] = f"proposed value {proposed!r} is not numeric"
        validated.append({**diff, **tag})
    return validated


# =============================================================================
# Telegram
# =============================================================================

def _send_telegram(text: str, review_id: Optional[str] = None) -> None:
    """Send a Markdown-formatted message to Telegram.

    If review_id is provided, adds inline Apply/Reject buttons whose
    callback_data carries the review_id so review_bot.py can route the action.
    """
    import requests

    # Read creds from config.yaml
    token = ""
    chat_id = ""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            ep = cfg.get("endpoints", {})
            token = ep.get("telegram_bot_token", "")
            chat_id = str(ep.get("telegram_chat_id", ""))
        except Exception:
            pass

    if not token or not chat_id:
        log.warning("Telegram creds not found in any config — skipping send.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if review_id:
        payload["reply_markup"] = json.dumps({
            "inline_keyboard": [[
                {"text": "✅ Apply", "callback_data": f"apply:{review_id}"},
                {"text": "❌ Reject", "callback_data": f"reject:{review_id}"},
            ]]
        })

    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Telegram send failed: %s", exc)


def _format_summary(review: dict) -> str:
    """Render a short Telegram summary from a review dict."""
    stats = review["stats"]
    summary = review["llm_response"].get("summary", "—") if review.get("llm_response") else "—"
    confidence = review["llm_response"].get("confidence", "—") if review.get("llm_response") else "—"

    lines = [
        f"📊 *Daily Review — {review['date']}*",
        "",
        f"Trades: *{stats.get('total_trades', 0)}*  "
        f"| Win rate: *{stats.get('win_rate_pct', 0):.1f}%*",
        f"PnL: *${stats.get('total_pnl_usd', 0):+.2f}*  "
        f"| Avg/trade: *${stats.get('avg_pnl_usd', 0):+.3f}*",
        "",
        f"💡 {summary}",
    ]

    diffs = review.get("proposed_diffs", [])
    valid_diffs = [d for d in diffs if d.get("validation_status") == "ok"]
    flagged     = [d for d in diffs if d.get("validation_status") != "ok"]

    if diffs:
        lines.append("")
        lines.append(f"📝 *Proposed diffs:* {len(valid_diffs)} ok, {len(flagged)} flagged")
        lines.append(f"🎯 Confidence: *{confidence}*")
        for d in valid_diffs[:5]:
            lines.append(
                f"  • `{d['path']}`: {d['current']} → *{d['proposed']}*"
            )
        if len(valid_diffs) > 5:
            lines.append(f"  (and {len(valid_diffs)-5} more — see review JSON)")

    anomalies = review.get("anomalies", [])
    if anomalies:
        lines.append("")
        lines.append("🚨 *Anomalies:*")
        for a in anomalies:
            lines.append(f"  • _{a['type']}_ ({a['severity']}): {a['detail']}")

    usage = review.get("usage", {})
    if usage:
        lines.append("")
        lines.append(
            f"🔢 Tokens: {usage.get('input_tokens', 0):,} in, "
            f"{usage.get('output_tokens', 0):,} out | "
            f"Cost: *${usage.get('cost_usd', 0):.3f}*"
        )

    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Daily LLM post-trade review.")
    parser.add_argument("--window-hours", type=float, default=24.0,
                        help="Time window to analyze (default 24h)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute stats but skip Claude call and Telegram send")
    parser.add_argument("--no-telegram", action="store_true",
                        help="Skip Telegram send (still saves review JSON)")
    args = parser.parse_args()

    try:
        import pandas as pd  # noqa: F401 — ensure it's importable up front
    except ImportError:
        log.error("pandas is required — install with: pip install pandas")
        sys.exit(1)

    log.info("Loading trades for last %.1fh…", args.window_hours)
    trades = _load_trades(window_hours=args.window_hours)
    regime = _load_regime()

    # State for anomaly detection (stuck position, drawdown)
    state = _load_state()

    stats     = _compute_stats(trades, regime)
    anomalies = _detect_anomalies(state, trades)

    # Seed the review dict with everything we know deterministically
    review = {
        "date":       datetime.now(timezone.utc).date().isoformat(),
        "generated":  datetime.now(timezone.utc).isoformat(),
        "window_hours": args.window_hours,
        "stats":      stats,
        "anomalies":  anomalies,
        "configs_analyzed": [],
        "llm_response": None,
        "proposed_diffs": [],
        "usage":      {},
        "status":     "pending",
    }

    total_trades = stats.get("total_trades", 0)

    if total_trades < MIN_TRADES_FOR_REVIEW:
        log.info(
            "Only %d trades in window (min %d) — skipping LLM call.",
            total_trades, MIN_TRADES_FOR_REVIEW,
        )
        review["status"] = "skipped_low_volume"
        review["skip_reason"] = (
            f"Only {total_trades} trades in window — need >= {MIN_TRADES_FOR_REVIEW} "
            "for statistical significance."
        )
    else:
        configs   = _load_active_configs()
        changelog = _load_changelog(days=30)

        review["configs_analyzed"] = [c.get("_filename") for c in configs]

        if args.dry_run:
            log.info("--dry-run set — skipping Claude API call.")
            review["status"] = "dry_run"
            # Build the prompt anyway so we can inspect what would be sent
            review["prompt_preview"] = _build_prompt(stats, anomalies, configs, changelog)[:2000] + "…"
        else:
            try:
                user_prompt = _build_prompt(stats, anomalies, configs, changelog)
                parsed, usage = _call_claude(_SYSTEM_PROMPT, user_prompt)
                review["llm_response"]   = parsed
                review["proposed_diffs"] = _validate_diffs(parsed.get("proposed_diffs", []))
                review["usage"]          = usage
                review["status"]         = "ok"
                log.info(
                    "Claude call: %d in / %d out tokens, cost $%.4f",
                    usage["input_tokens"], usage["output_tokens"], usage["cost_usd"],
                )
            except Exception as exc:
                log.error("Claude call failed: %s", exc)
                review["status"] = "error"
                review["error"]  = str(exc)

    # Save the review
    review_path = REVIEWS_DIR / f"{review['date']}.json"
    with open(review_path, "w", encoding="utf-8") as f:
        json.dump(review, f, indent=2, ensure_ascii=False, default=str)
    log.info("Review saved to %s", review_path)

    # Telegram
    if not args.no_telegram:
        review_id = review["date"] if review.get("proposed_diffs") else None
        _send_telegram(_format_summary(review), review_id=review_id)

    # Exit 0 always — cron should not retry on analytical "no-ops"
    sys.exit(0)


if __name__ == "__main__":
    main()
