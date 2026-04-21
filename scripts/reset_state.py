"""Reset the paper trading state to initial values.

Usage examples:
  python3 scripts/reset_state.py                          # reset default state.json
  python3 scripts/reset_state.py --config configs/config_aggress.yaml
  python3 scripts/reset_state.py --config configs/config_safe.yaml
  python3 scripts/reset_state.py --all                    # reset all state_*.json in data/
"""

import argparse
import json
import os
import glob

import yaml


DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
DEFAULT_STATE_FILE = os.path.join(DATA_DIR, "state.json")
DEFAULT_BALANCE = 1000.0


def _build_initial_state(balance: float) -> dict:
    return {
        "virtual_portfolio": {
            "balance_usd": balance,
            "active_position": None,
            "daily_pnl": 0.0,
            "last_update": None,
            "trading_halted_until": None,
        },
        "trade_history": [],
    }


def _reset_file(path: str, balance: float, dry_run: bool = False) -> None:
    abs_path = os.path.abspath(path)
    if dry_run:
        print(f"  [dry-run] would reset: {abs_path}  (balance=${balance:.2f})")
        return
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "w", encoding="utf-8") as f:
        json.dump(_build_initial_state(balance), f, indent=2)
    print(f"  ✓ reset: {abs_path}  (balance=${balance:.2f})")


def _load_config(config_path: str) -> dict:
    abs_path = os.path.abspath(config_path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Config not found: {abs_path}")
    with open(abs_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset crypto futures paper trading state file(s).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config", "-c",
        metavar="PATH",
        help="Path to a bot config YAML. Resets the state_file declared inside it.",
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Reset ALL state_*.json files found in the data/ directory.",
    )
    parser.add_argument(
        "--balance",
        type=float,
        default=None,
        metavar="USD",
        help="Override starting balance (default: read from config, or 1000.0).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be reset without actually writing anything.",
    )
    args = parser.parse_args()

    if args.all and args.config:
        parser.error("--all and --config are mutually exclusive.")

    if args.dry_run:
        print("[dry-run mode — nothing will be written]\n")

    # ── --all: reset every state_*.json in data/ ──────────────────────────────
    if args.all:
        pattern = os.path.join(DATA_DIR, "state*.json")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No state files found in {DATA_DIR}")
            return
        print(f"Resetting {len(files)} state file(s):\n")
        for path in files:
            balance = args.balance if args.balance is not None else DEFAULT_BALANCE
            _reset_file(path, balance, dry_run=args.dry_run)
        return

    # ── --config: read state_file and balance from yaml ───────────────────────
    if args.config:
        cfg = _load_config(args.config)
        state_filename = cfg.get("simulation", {}).get("state_file", "state.json")
        state_path = os.path.join(DATA_DIR, state_filename)
        balance = args.balance if args.balance is not None else \
            float(cfg.get("risk_management", {}).get("initial_balance_usd", DEFAULT_BALANCE))
        print(f"Config:  {os.path.abspath(args.config)}")
        _reset_file(state_path, balance, dry_run=args.dry_run)
        return

    # ── default: reset state.json ─────────────────────────────────────────────
    balance = args.balance if args.balance is not None else DEFAULT_BALANCE
    _reset_file(DEFAULT_STATE_FILE, balance, dry_run=args.dry_run)


if __name__ == "__main__":
    main()