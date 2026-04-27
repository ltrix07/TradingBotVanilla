"""
funding_backtest.py — Backtest funding rate contrarian strategy.

Downloads historical funding rates + klines from Binance Futures,
simulates entries/exits, and prints detailed performance metrics.

Usage:
    python scripts/funding_backtest.py                  # default: BTCUSDT, 12 months
    python scripts/funding_backtest.py --symbol ETHUSDT  # test on ETH
    python scripts/funding_backtest.py --symbol SOLUSDT --months 6

Strategy:
    - When funding rate > threshold → SHORT (market is overleveraged long)
    - When funding rate < -threshold → LONG (market is overleveraged short)
    - Hold for N hours (configurable), then close
    - SL/TP checked during hold via price extremes
"""

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field


# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://fapi.binance.com"
LEVERAGE = 5
POSITION_PCT = 0.30        # 30% of balance per trade
INITIAL_BALANCE = 1000.0
FEE_PCT = 0.0005           # 0.05% round-trip (taker)
SLIPPAGE_PCT = 0.0003      # 0.03% estimated slippage

# Risk
SL_PCT = 0.008     # 0.8% stop loss
TP_PCT = 0.012     # 1.2% take profit (R:R = 1.5)


# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_json(url: str) -> list | dict:
    """Fetch JSON from Binance API with retries."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=15)
            return json.loads(resp.read())
        except Exception as e:
            if attempt == 2:
                raise
            print(f"  Retry {attempt+1}: {e}")
            time.sleep(2)


def fetch_funding_history(symbol: str, months: int = 12) -> list[dict]:
    """Fetch funding rate history. Binance returns max 1000 per call."""
    all_rates = []
    end_time = int(time.time() * 1000)
    start_time = end_time - (months * 30 * 24 * 3600 * 1000)

    print(f"Fetching {months} months of {symbol} funding rate history...")
    current_start = start_time

    while current_start < end_time:
        url = (
            f"{BASE_URL}/fapi/v1/fundingRate"
            f"?symbol={symbol}&startTime={current_start}&limit=1000"
        )
        data = fetch_json(url)
        if not data:
            break

        for d in data:
            all_rates.append({
                "time_ms": int(d["fundingTime"]),
                "rate": float(d["fundingRate"]),
                "time_str": datetime.fromtimestamp(
                    int(d["fundingTime"]) / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M"),
            })

        current_start = int(data[-1]["fundingTime"]) + 1
        time.sleep(0.3)  # rate limit

    print(f"  Got {len(all_rates)} funding records "
          f"({all_rates[0]['time_str']} → {all_rates[-1]['time_str']})" if all_rates else "  Got 0 records")
    return all_rates


def fetch_klines_around(symbol: str, timestamp_ms: int, hours_before: int = 1,
                        hours_after: int = 24) -> list[dict]:
    """Fetch 1h klines around a timestamp for price tracking."""
    start = timestamp_ms - (hours_before * 3600 * 1000)
    end = timestamp_ms + (hours_after * 3600 * 1000)
    url = (
        f"{BASE_URL}/fapi/v1/klines"
        f"?symbol={symbol}&interval=1h&startTime={start}&endTime={end}&limit=500"
    )
    data = fetch_json(url)
    return [
        {
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        }
        for k in data
    ]


def get_price_at_time(klines: list[dict], target_ms: int) -> float | None:
    """Find the close price of the candle containing target_ms."""
    for k in klines:
        if k["open_time"] <= target_ms < k["open_time"] + 3600_000:
            return k["close"]
    return klines[-1]["close"] if klines else None


def get_extreme_price(klines: list[dict], entry_ms: int, exit_ms: int,
                      side: str) -> float | None:
    """Get the worst price during hold period (for SL check)."""
    worst = None
    for k in klines:
        if k["open_time"] < entry_ms - 3600_000:
            continue
        if k["open_time"] > exit_ms:
            break
        if side == "SHORT":
            if worst is None or k["high"] > worst:
                worst = k["high"]
        else:
            if worst is None or k["low"] < worst:
                worst = k["low"]
    return worst


# ── Backtest engine ───────────────────────────────────────────────────────────

@dataclass
class Trade:
    entry_time: str
    exit_time: str
    side: str
    entry_price: float
    exit_price: float
    funding_rate_bps: float
    pnl_pct: float
    pnl_usd: float
    result: str  # "TP", "SL", "TIME", "HOLD"


@dataclass
class BacktestResult:
    threshold_bps: float
    hold_hours: int
    trades: list[Trade] = field(default_factory=list)
    final_balance: float = 0.0
    max_drawdown_pct: float = 0.0

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.pnl_usd > 0)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.trades if t.pnl_usd <= 0)

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades * 100 if self.total_trades > 0 else 0

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl_usd for t in self.trades)

    @property
    def avg_win(self) -> float:
        wins = [t.pnl_usd for t in self.trades if t.pnl_usd > 0]
        return sum(wins) / len(wins) if wins else 0

    @property
    def avg_loss(self) -> float:
        losses = [t.pnl_usd for t in self.trades if t.pnl_usd <= 0]
        return sum(losses) / len(losses) if losses else 0


def run_backtest(
    funding_rates: list[dict],
    threshold_bps: float,
    hold_hours: int,
    symbol: str = "BTCUSDT",
) -> BacktestResult:
    """Run a single backtest configuration."""
    result = BacktestResult(threshold_bps=threshold_bps, hold_hours=hold_hours)
    balance = INITIAL_BALANCE
    peak_balance = balance
    max_dd = 0.0

    # Filter signals by threshold
    signals = []
    for fr in funding_rates:
        rate_bps = fr["rate"] * 10_000
        if rate_bps > threshold_bps:
            signals.append({"side": "SHORT", **fr})
        elif rate_bps < -threshold_bps:
            signals.append({"side": "LONG", **fr})

    print(f"  Threshold {threshold_bps:5.1f} bps, hold {hold_hours:2d}h: "
          f"{len(signals)} signals", end="")

    if not signals:
        print(" — skipping (no trades)")
        result.final_balance = balance
        return result

    # Simulate trades
    last_exit_ms = 0
    processed = 0

    for sig in signals:
        entry_ms = sig["time_ms"]

        # Skip if we're still in previous trade
        if entry_ms < last_exit_ms:
            continue

        exit_ms = entry_ms + (hold_hours * 3600 * 1000)

        # Fetch price data for this trade
        try:
            klines = fetch_klines_around(symbol, entry_ms, hours_before=1,
                                         hours_after=hold_hours + 2)
        except Exception as e:
            print(f"    Skipping trade at {sig['time_str']}: {e}")
            continue

        if not klines:
            continue

        entry_price = get_price_at_time(klines, entry_ms)
        exit_price = get_price_at_time(klines, exit_ms)
        if entry_price is None or exit_price is None:
            continue

        # Check SL/TP during hold period
        extreme = get_extreme_price(klines, entry_ms, exit_ms, sig["side"])
        trade_result = "HOLD"
        actual_exit_price = exit_price

        if extreme is not None:
            if sig["side"] == "SHORT":
                sl_price = entry_price * (1 + SL_PCT)
                tp_price = entry_price * (1 - TP_PCT)
                if extreme >= sl_price:
                    actual_exit_price = sl_price
                    trade_result = "SL"
                low_extreme = None
                for k in klines:
                    if k["open_time"] >= entry_ms - 3600_000 and k["open_time"] <= exit_ms:
                        if low_extreme is None or k["low"] < low_extreme:
                            low_extreme = k["low"]
                if low_extreme is not None and low_extreme <= tp_price:
                    if trade_result != "SL":
                        actual_exit_price = tp_price
                        trade_result = "TP"
            else:  # LONG
                sl_price = entry_price * (1 - SL_PCT)
                tp_price = entry_price * (1 + TP_PCT)
                if extreme <= sl_price:
                    actual_exit_price = sl_price
                    trade_result = "SL"
                high_extreme = None
                for k in klines:
                    if k["open_time"] >= entry_ms - 3600_000 and k["open_time"] <= exit_ms:
                        if high_extreme is None or k["high"] > high_extreme:
                            high_extreme = k["high"]
                if high_extreme is not None and high_extreme >= tp_price:
                    if trade_result != "SL":
                        actual_exit_price = tp_price
                        trade_result = "TP"

        # Calculate PnL
        notional = balance * LEVERAGE * POSITION_PCT
        qty = notional / entry_price

        if sig["side"] == "SHORT":
            pnl_gross = (entry_price - actual_exit_price) * qty
        else:
            pnl_gross = (actual_exit_price - entry_price) * qty

        fee = FEE_PCT * notional
        slip = SLIPPAGE_PCT * notional
        pnl_net = pnl_gross - fee - slip

        # Funding payment collected (if held through funding time)
        funding_collected = abs(sig["rate"]) * notional
        pnl_net += funding_collected

        pnl_pct = pnl_net / balance * 100

        trade = Trade(
            entry_time=sig["time_str"],
            exit_time=datetime.fromtimestamp(exit_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            side=sig["side"],
            entry_price=entry_price,
            exit_price=actual_exit_price,
            funding_rate_bps=sig["rate"] * 10_000,
            pnl_pct=pnl_pct,
            pnl_usd=pnl_net,
            result=trade_result,
        )
        result.trades.append(trade)

        balance += pnl_net
        if balance > peak_balance:
            peak_balance = balance
        dd = (peak_balance - balance) / peak_balance * 100
        if dd > max_dd:
            max_dd = dd

        last_exit_ms = exit_ms
        processed += 1

        if processed % 10 == 0:
            time.sleep(0.5)

    result.final_balance = balance
    result.max_drawdown_pct = max_dd
    print(f" → {processed} executed, PnL ${result.total_pnl:+.2f}")
    return result


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_result(r: BacktestResult) -> None:
    if r.total_trades == 0:
        return
    print(f"\n{'='*60}")
    print(f"  Threshold: {r.threshold_bps} bps | Hold: {r.hold_hours}h")
    print(f"{'='*60}")
    print(f"  Total trades:    {r.total_trades}")
    print(f"  Wins / Losses:   {r.wins} / {r.losses}")
    print(f"  Win rate:        {r.win_rate:.1f}%")
    print(f"  Avg win:         ${r.avg_win:.2f}")
    print(f"  Avg loss:        ${r.avg_loss:.2f}")
    print(f"  Total PnL:       ${r.total_pnl:.2f}")
    print(f"  Final balance:   ${r.final_balance:.2f}")
    print(f"  Return:          {(r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100:.2f}%")
    print(f"  Max drawdown:    {r.max_drawdown_pct:.2f}%")

    # Monthly breakdown
    months = {}
    for t in r.trades:
        month = t.entry_time[:7]
        if month not in months:
            months[month] = {"trades": 0, "pnl": 0.0, "wins": 0}
        months[month]["trades"] += 1
        months[month]["pnl"] += t.pnl_usd
        if t.pnl_usd > 0:
            months[month]["wins"] += 1

    print(f"\n  Monthly breakdown:")
    for month in sorted(months):
        m = months[month]
        wr = m["wins"] / m["trades"] * 100 if m["trades"] > 0 else 0
        print(f"    {month}: {m['trades']:2d} trades, WR {wr:5.1f}%, PnL ${m['pnl']:+.2f}")

    # Result type distribution
    results = {}
    for t in r.trades:
        results[t.result] = results.get(t.result, 0) + 1
    print(f"\n  Exit types: {results}")


def print_summary_table(all_results: list[BacktestResult]) -> None:
    has_trades = [r for r in all_results if r.total_trades > 0]
    if not has_trades:
        print("\n  NO CONFIGURATIONS PRODUCED TRADES.")
        return

    print(f"\n{'='*80}")
    print(f"  SUMMARY: Configurations with trades, ranked by return")
    print(f"{'='*80}")
    print(f"  {'Thr':>5s} {'Hold':>5s} {'Trades':>7s} {'WR%':>6s} {'PnL$':>8s} "
          f"{'Ret%':>7s} {'MaxDD%':>7s} {'AvgWin':>7s} {'AvgLoss':>8s}")
    print(f"  {'-'*5} {'-'*5} {'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*8}")

    sorted_results = sorted(has_trades, key=lambda r: r.total_pnl, reverse=True)
    for r in sorted_results:
        ret = (r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
        print(f"  {r.threshold_bps:5.1f} {r.hold_hours:5d} {r.total_trades:7d} "
              f"{r.win_rate:6.1f} {r.total_pnl:8.2f} {ret:7.2f} "
              f"{r.max_drawdown_pct:7.2f} {r.avg_win:7.2f} {r.avg_loss:8.2f}")


def print_funding_distribution(funding_rates: list[dict], symbol: str) -> None:
    """Print detailed distribution of funding rates."""
    rates_bps = [fr["rate"] * 10_000 for fr in funding_rates]
    abs_rates = [abs(r) for r in rates_bps]

    print(f"\n  {symbol} Funding rate distribution (bps):")
    print(f"    Records:  {len(rates_bps)}")
    print(f"    Mean:     {sum(rates_bps)/len(rates_bps):+.4f}")
    print(f"    Min:      {min(rates_bps):+.4f}")
    print(f"    Max:      {max(rates_bps):+.4f}")
    print(f"    Std dev:  {(sum((r - sum(rates_bps)/len(rates_bps))**2 for r in rates_bps) / len(rates_bps)) ** 0.5:.4f}")

    # Percentiles
    sorted_rates = sorted(abs_rates)
    for p in [50, 75, 90, 95, 99]:
        idx = int(len(sorted_rates) * p / 100)
        print(f"    P{p:02d} |rate|: {sorted_rates[min(idx, len(sorted_rates)-1)]:.4f} bps")

    # Count by bucket
    print(f"\n    Distribution of |rate|:")
    buckets = [0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0, 30.0, 50.0]
    for b in buckets:
        count = sum(1 for r in abs_rates if r > b)
        pct = count / len(abs_rates) * 100
        if count > 0:
            print(f"      |rate| > {b:5.1f} bps: {count:4d} ({pct:5.1f}%)")
        else:
            print(f"      |rate| > {b:5.1f} bps:    0 — threshold too high")
            break


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Funding Rate Backtest")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (default: BTCUSDT)")
    parser.add_argument("--months", type=int, default=12, help="Months of history (default: 12)")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    months = args.months

    print("=" * 60)
    print(f"  FUNDING RATE BACKTEST — {symbol}")
    print("=" * 60)
    print(f"  Balance: ${INITIAL_BALANCE}, Leverage: {LEVERAGE}x")
    print(f"  Position: {POSITION_PCT*100:.0f}% of balance per trade")
    print(f"  SL: {SL_PCT*100:.1f}%, TP: {TP_PCT*100:.1f}%")
    print(f"  Fees: {FEE_PCT*100:.2f}%, Slippage: {SLIPPAGE_PCT*100:.2f}%")
    print()

    # 1. Fetch funding rates
    funding_rates = fetch_funding_history(symbol, months=months)
    if not funding_rates:
        print("ERROR: No funding data retrieved!")
        return

    # 2. Show distribution first — helps decide thresholds
    print_funding_distribution(funding_rates, symbol)

    # 3. Choose thresholds dynamically based on actual distribution
    rates_bps = [abs(fr["rate"] * 10_000) for fr in funding_rates]
    sorted_abs = sorted(rates_bps)

    # Use percentile-based thresholds: P75, P90, P95, P99
    p75 = sorted_abs[int(len(sorted_abs) * 0.75)]
    p90 = sorted_abs[int(len(sorted_abs) * 0.90)]
    p95 = sorted_abs[int(len(sorted_abs) * 0.95)]
    p99 = sorted_abs[min(int(len(sorted_abs) * 0.99), len(sorted_abs)-1)]

    # Round to nice numbers and deduplicate
    candidate_thresholds = sorted(set([
        round(p75, 2),
        round(p90, 2),
        round(p95, 2),
        round(p99, 2),
        round(p75 * 0.5, 2),    # half of P75 for aggressive entry
    ]))

    # Also add some fixed micro-thresholds if rates are very low
    max_rate = max(rates_bps)
    if max_rate < 5:
        candidate_thresholds = sorted(set(
            [0.3, 0.5, 0.7, 1.0] + candidate_thresholds
        ))

    # Filter out zeros and too-high thresholds
    test_thresholds = [t for t in candidate_thresholds if 0 < t <= max_rate * 1.2]

    if not test_thresholds:
        print(f"\n  WARNING: Max |rate| is only {max_rate:.4f} bps — no viable thresholds.")
        print(f"  This symbol has extremely stable funding. Consider altcoins (ETHUSDT, SOLUSDT).")
        return

    print(f"\n  Testing thresholds (based on distribution): {test_thresholds}")

    hold_hours_options = [4, 8, 12]

    # 4. Run backtests
    all_results = []
    for threshold_bps in test_thresholds:
        for hold_hours in hold_hours_options:
            result = run_backtest(funding_rates, threshold_bps, hold_hours, symbol=symbol)
            all_results.append(result)

    # 5. Print results
    for r in all_results:
        print_result(r)

    print_summary_table(all_results)

    # 6. Best config recommendation
    has_trades = [r for r in all_results if r.total_trades > 0]
    if has_trades:
        best = max(has_trades, key=lambda r: r.total_pnl)
        span_days = (funding_rates[-1]["time_ms"] - funding_rates[0]["time_ms"]) / (1000 * 86400)
        monthly_ret = best.total_pnl / (span_days / 30) if span_days > 0 else 0
        print(f"\n  RECOMMENDED CONFIG:")
        print(f"    Symbol:    {symbol}")
        print(f"    Threshold: {best.threshold_bps} bps")
        print(f"    Hold time: {best.hold_hours} hours")
        print(f"    Expected WR: {best.win_rate:.1f}%")
        print(f"    Monthly return: ~${monthly_ret:.2f} ({monthly_ret/INITIAL_BALANCE*100:.2f}%)")
        print(f"    Max drawdown: {best.max_drawdown_pct:.2f}%")

        # Save trades
        if best.trades:
            csv_path = f"data/funding_backtest_{symbol.lower()}.csv"
            with open(csv_path, "w") as f:
                f.write("entry_time,exit_time,side,entry_price,exit_price,"
                        "funding_bps,pnl_pct,pnl_usd,result\n")
                for t in best.trades:
                    f.write(f"{t.entry_time},{t.exit_time},{t.side},"
                            f"{t.entry_price:.2f},{t.exit_price:.2f},"
                            f"{t.funding_rate_bps:.4f},{t.pnl_pct:.4f},"
                            f"{t.pnl_usd:.2f},{t.result}\n")
            print(f"\n  Trades saved to {csv_path}")
    else:
        print(f"\n  NO VIABLE STRATEGY for {symbol} — funding rates too flat.")
        print(f"  Try: python scripts/funding_backtest.py --symbol ETHUSDT")
        print(f"  Try: python scripts/funding_backtest.py --symbol SOLUSDT")


if __name__ == "__main__":
    main()
