"""
scripts/apply_review.py — apply LLM-proposed config diffs with safety checks.

Used both from the command line (manual review) and from review_bot.py
(Telegram Apply button).

Usage:
    python scripts/apply_review.py 2026-04-21                 # dry-run, show what would change
    python scripts/apply_review.py 2026-04-21 --confirm       # actually write
    python scripts/apply_review.py 2026-04-21 --reject "too aggressive"  # mark as rejected

Safety
------
- Refuses to apply diffs flagged as out_of_bounds / unknown_path / non_numeric
  by daily_review.py's validator.
- Double-checks sanity limits here (defence in depth).
- Creates a timestamped backup of every file before modifying it.
- Appends a facts-only entry to data/reviews/history.jsonl: which path,
  old value, new value, file, reason quoted from the review, timestamp.
- Never runs automatically — must be invoked explicitly.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT        = Path(__file__).resolve().parent.parent
CONFIGS_DIR = ROOT / "configs"
BACKUPS_DIR = CONFIGS_DIR / "backups"
REVIEWS_DIR = ROOT / "data" / "reviews"
CHANGELOG   = REVIEWS_DIR / "history.jsonl"

BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
REVIEWS_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("apply_review")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)


# Same sanity table as daily_review.py — import would create circular deps,
# and duplicating here provides defence in depth.
SANITY_LIMITS: dict[str, tuple[float, float]] = {
    "risk_management.risk_per_trade_pct":          (0.003, 0.03),
    "risk_management.position_size_pct":           (0.01,  0.15),
    "risk_management.max_daily_loss_pct":          (0.02,  0.15),
    "risk_management.atr_sl_multiplier":           (1.0,   10.0),
    "risk_management.atr_tp_multiplier":           (1.0,   20.0),
    "risk_management.trailing_stop_atr_multiplier": (0.5,  5.0),
    "risk_management.breakeven_atr_multiplier":    (0.0,   5.0),
    "risk_management.max_hold_time_minutes":       (15,    480),
    "risk_management.max_funding_rate_bps":        (10,    200),
    "risk_management.sl_confirm_seconds":          (0,     60),
    "risk_management.cooldown_after_sl_sec":       (0,     600),
    "risk_management.time_stop_seconds":           (60,    3600),
    "risk_management.time_stop_max_loss_pct":      (0.0,   0.02),
    "exchange.leverage":                           (1,     10),
    "strategy.parameters.fast_ema":                (3,     30),
    "strategy.parameters.slow_ema":                (10,    100),
    "strategy.parameters.signal_smoothing":        (2,     20),
    "strategy.rsi.overbought":                     (60,    85),
    "strategy.rsi.oversold":                       (15,    40),
    "strategy.order_book.imbalance_threshold":     (0.50,  0.70),
    "strategy.entry_filters.trend_ema_period":     (20,    300),
    "strategy.entry_filters.volume_spike_multiplier": (1.0, 3.0),
    "strategy.entry_filters.volume_spike_period":  (5,     30),
}

INTEGER_PATHS = {
    "exchange.leverage",
    "risk_management.max_hold_time_minutes",
    "risk_management.max_funding_rate_bps",
    "risk_management.sl_confirm_seconds",
    "risk_management.cooldown_after_sl_sec",
    "risk_management.time_stop_seconds",
    "strategy.parameters.fast_ema",
    "strategy.parameters.slow_ema",
    "strategy.parameters.signal_smoothing",
    "strategy.rsi.overbought",
    "strategy.rsi.oversold",
    "strategy.entry_filters.trend_ema_period",
    "strategy.entry_filters.volume_spike_period",
}


# --- Helpers ----------------------------------------------------------------

def _load_review(review_id: str) -> dict:
    path = REVIEWS_DIR / f"{review_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Review file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _set_by_path(d: dict, dotted: str, value) -> tuple:
    """Set d[a][b][c]=value for dotted='a.b.c'. Returns (old_value, parent_dict, leaf_key)."""
    keys = dotted.split(".")
    node = d
    for k in keys[:-1]:
        if k not in node or not isinstance(node[k], dict):
            node[k] = {}
        node = node[k]
    leaf = keys[-1]
    old = node.get(leaf)
    node[leaf] = value
    return old, node, leaf


def _coerce_number(path: str, value):
    """Coerce to int if the path is in INTEGER_PATHS, else float."""
    if path in INTEGER_PATHS:
        return int(round(float(value)))
    return float(value)


def _validate_one(diff: dict) -> tuple[bool, str]:
    """Return (ok, reason). Defence-in-depth: re-check limits."""
    status = diff.get("validation_status")
    if status and status != "ok":
        return False, f"marked '{status}' by daily_review: {diff.get('validation_reason', '')}"

    path = diff.get("path")
    if not path or path not in SANITY_LIMITS:
        return False, f"path '{path}' not in sanity table"

    try:
        val = float(diff.get("proposed"))
    except (TypeError, ValueError):
        return False, f"proposed value {diff.get('proposed')!r} is not numeric"

    lo, hi = SANITY_LIMITS[path]
    if not (lo <= val <= hi):
        return False, f"{val} outside sanity limits [{lo}, {hi}]"
    return True, ""


def _backup_file(cfg_path: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUPS_DIR / f"{cfg_path.stem}.{ts}{cfg_path.suffix}"
    shutil.copy2(cfg_path, backup)
    return backup


def _append_changelog(entry: dict) -> None:
    with open(CHANGELOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# --- Main actions ------------------------------------------------------------

def apply_review(review_id: str, confirm: bool) -> int:
    review = _load_review(review_id)
    diffs = review.get("proposed_diffs", [])

    if not diffs:
        log.info("Review %s has no proposed diffs.", review_id)
        return 0

    # Group diffs by target file
    by_file: dict[str, list[dict]] = {}
    for d in diffs:
        fname = d.get("file")
        if not fname:
            log.warning("Diff missing 'file' field — skipped: %s", d)
            continue
        by_file.setdefault(fname, []).append(d)

    any_applied = False
    any_skipped = False

    for rel_file, file_diffs in by_file.items():
        # Safety: only touch files under configs/
        cfg_path = (ROOT / rel_file).resolve()
        try:
            cfg_path.relative_to(CONFIGS_DIR.resolve())
        except ValueError:
            log.error("Refusing to write outside configs/: %s", rel_file)
            any_skipped = True
            continue

        if not cfg_path.exists():
            log.error("Target config not found: %s", rel_file)
            any_skipped = True
            continue

        with open(cfg_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        print(f"\n=== {rel_file} ===")
        applied_in_this_file: list[dict] = []

        for d in file_diffs:
            ok, reason = _validate_one(d)
            path = d.get("path", "<no path>")
            proposed = d.get("proposed")
            current  = d.get("current")
            reason_txt = d.get("reason", "")

            if not ok:
                print(f"  ✗ SKIP  {path}: {reason}")
                any_skipped = True
                continue

            # Coerce to right numeric type based on SANITY table
            try:
                new_value = _coerce_number(path, proposed)
            except Exception as exc:
                print(f"  ✗ SKIP  {path}: coercion failed — {exc}")
                any_skipped = True
                continue

            # Show the pending change
            print(f"  ~ {path}: {current} → {new_value}")
            print(f"    reason: {reason_txt[:200]}")

            if confirm:
                old, _, _ = _set_by_path(cfg, path, new_value)
                applied_in_this_file.append({
                    "path":       path,
                    "old":        old,
                    "new":        new_value,
                    "reason":     reason_txt,
                })

        if confirm and applied_in_this_file:
            backup = _backup_file(cfg_path)
            print(f"  💾 backup → {backup.relative_to(ROOT)}")
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
            print(f"  ✓ wrote {len(applied_in_this_file)} changes to {rel_file}")

            entry = {
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "review_id":   review_id,
                "file":        rel_file,
                "backup":      str(backup.relative_to(ROOT)),
                "changes":     applied_in_this_file,
            }
            _append_changelog(entry)
            any_applied = True

    if not confirm:
        print("\nDry-run mode. Re-run with --confirm to apply.")
        return 0

    if any_applied:
        print("\n✓ Review applied. Changelog updated.")
    if any_skipped:
        print("⚠ Some diffs were skipped (see above). Review JSON retained.")
    return 0 if any_applied or not diffs else 1


def reject_review(review_id: str, note: str) -> int:
    """Log a rejection in the changelog without modifying any config."""
    entry = {
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "review_id":   review_id,
        "action":      "reject",
        "note":        note,
    }
    _append_changelog(entry)
    log.info("Rejection logged for %s: %s", review_id, note)
    return 0


# --- CLI ---------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Apply an LLM-generated review.")
    parser.add_argument("review_id", help="Review date, e.g. 2026-04-21")
    parser.add_argument("--confirm",  action="store_true", help="Actually write changes")
    parser.add_argument("--reject",   metavar="NOTE",
                        help="Mark the review as rejected with a short note")
    args = parser.parse_args()

    if args.reject is not None:
        sys.exit(reject_review(args.review_id, args.reject))

    sys.exit(apply_review(args.review_id, confirm=args.confirm))


if __name__ == "__main__":
    main()
