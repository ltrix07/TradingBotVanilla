"""
orderflow_backtest.py — Backtest order flow (aggTrades delta) strategy.

Downloads historical aggTrades from data.binance.vision, pre-aggregates
into 1-second buckets, then runs fast backtests.

Usage:
    python scripts/orderflow_backtest.py --download --days 7   # download only
    python scripts/orderflow_backtest.py --days 7              # run backtest
    python scripts/orderflow_backtest.py --symbol ETHUSDT      # different pair

Performance: ~10M trades are aggregated into ~600K buckets (1 per second),
making each backtest config run in seconds, not hours.
"""

import argparse
import csv
import io
import os
import sys
import time
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional


# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "aggtrades")
BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"

LEVERAGE = 5
POSITION_PCT = 0.30
INITIAL_BALANCE = 1000.0
FEE_PCT = 0.0005           # 0.05% per side
SLIPPAGE_PCT = 0.0002      # 0.02% per side


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class SecondBucket:
    """One-second aggregation of all trades."""
    timestamp_sec: int      # unix seconds
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    buy_volume_usd: float   # aggressive buy volume
    sell_volume_usd: float  # aggressive sell volume
    trade_count: int

    @property
    def total_volume(self) -> float:
        return self.buy_volume_usd + self.sell_volume_usd

    @property
    def delta(self) -> float:
        return self.buy_volume_usd - self.sell_volume_usd


@dataclass
class Trade:
    entry_time_sec: int
    exit_time_sec: int
    side: str
    entry_price: float
    exit_price: float
    delta_intensity: float
    pnl_usd: float
    pnl_pct: float
    result: str
    hold_seconds: int


@dataclass
class BacktestConfig:
    window_sec: int
    intensity_threshold: float
    persistence_sec: int
    sl_pct: float
    tp_pct: float
    cooldown_sec: int
    max_hold_sec: int
    min_volume_usd: float


@dataclass
class BacktestResult:
    config: BacktestConfig
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
    def win_rate(self) -> float:
        return self.wins / self.total_trades * 100 if self.total_trades else 0

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl_usd for t in self.trades)

    @property
    def avg_win(self) -> float:
        w = [t.pnl_usd for t in self.trades if t.pnl_usd > 0]
        return sum(w) / len(w) if w else 0

    @property
    def avg_loss(self) -> float:
        lo = [t.pnl_usd for t in self.trades if t.pnl_usd <= 0]
        return sum(lo) / len(lo) if lo else 0

    @property
    def avg_hold_sec(self) -> float:
        return sum(t.hold_seconds for t in self.trades) / len(self.trades) if self.trades else 0

    @property
    def profit_factor(self) -> float:
        gross_win = sum(t.pnl_usd for t in self.trades if t.pnl_usd > 0)
        gross_loss = abs(sum(t.pnl_usd for t in self.trades if t.pnl_usd <= 0))
        return gross_win / gross_loss if gross_loss > 0 else 0


# ── Data download ─────────────────────────────────────────────────────────────

def download_aggtrades(symbol: str, days: int = 7) -> list[str]:
    """Download aggTrades ZIPs from data.binance.vision."""
    os.makedirs(DATA_DIR, exist_ok=True)
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    paths = []

    for i in range(days):
        date = end_date - timedelta(days=days - 1 - i)
        date_str = date.strftime("%Y-%m-%d")
        csv_path = os.path.join(DATA_DIR, f"{symbol}-aggTrades-{date_str}.csv")

        if os.path.exists(csv_path):
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"  {date_str}: exists ({size_mb:.1f} MB)")
            paths.append(csv_path)
            continue

        url = f"{BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
        print(f"  {date_str}: downloading...", end="", flush=True)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=120)
            zip_data = resp.read()
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f_in, open(csv_path, "wb") as f_out:
                    f_out.write(f_in.read())
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f" OK ({size_mb:.1f} MB)")
            paths.append(csv_path)
            time.sleep(0.5)
        except Exception as e:
            print(f" FAILED: {e}")

    return paths


# ── Pre-aggregation: trades → 1-second buckets ───────────────────────────────

def aggregate_to_buckets(csv_paths: list[str]) -> list[SecondBucket]:
    """Read raw CSVs and aggregate into 1-second OHLCV+delta buckets.

    This is the key optimization: ~10M raw trades become ~600K buckets.
    All subsequent backtests operate on buckets, not individual trades.
    """
    # Pass 1: build dict of second → accumulated data
    buckets_dict: dict[int, list] = {}
    # list = [open_price, high, low, close, buy_vol, sell_vol, count, first_price_set]

    total_raw = 0
    for path in csv_paths:
        file_trades = 0
        with open(path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    ts_ms = int(row[5])
                    price = float(row[1])
                    qty = float(row[2])
                    is_buyer_maker = (row[6].strip().lower() == "true")
                except (ValueError, IndexError):
                    continue

                ts_sec = ts_ms // 1000
                usd = price * qty

                if ts_sec not in buckets_dict:
                    # [open, high, low, close, buy_vol, sell_vol, count]
                    buckets_dict[ts_sec] = [price, price, price, price, 0.0, 0.0, 0]

                b = buckets_dict[ts_sec]
                if price > b[1]:
                    b[1] = price  # high
                if price < b[2]:
                    b[2] = price  # low
                b[3] = price      # close (last price)

                if not is_buyer_maker:  # aggressive BUY
                    b[4] += usd
                else:                   # aggressive SELL
                    b[5] += usd
                b[6] += 1
                file_trades += 1

        total_raw += file_trades
        print(f"  {os.path.basename(path)}: {file_trades:,} trades")

    # Pass 2: convert to sorted list of SecondBucket
    buckets = []
    for ts_sec in sorted(buckets_dict.keys()):
        b = buckets_dict[ts_sec]
        buckets.append(SecondBucket(
            timestamp_sec=ts_sec,
            open_price=b[0],
            high_price=b[1],
            low_price=b[2],
            close_price=b[3],
            buy_volume_usd=b[4],
            sell_volume_usd=b[5],
            trade_count=b[6],
        ))

    print(f"  Aggregated: {total_raw:,} trades → {len(buckets):,} second-buckets "
          f"({total_raw / max(len(buckets), 1):.1f} trades/sec avg)")

    return buckets


# ── Backtest engine (operates on buckets) ─────────────────────────────────────

def run_backtest(buckets: list[SecondBucket], cfg: BacktestConfig) -> BacktestResult:
    """Run backtest on pre-aggregated second buckets. Fast."""
    result = BacktestResult(config=cfg)
    balance = INITIAL_BALANCE
    peak_balance = balance
    max_dd = 0.0
    n = len(buckets)

    # Pre-compute prefix sums for rolling window
    # This avoids re-summing the window for every bucket
    buy_prefix = [0.0] * (n + 1)
    sell_prefix = [0.0] * (n + 1)
    for i, b in enumerate(buckets):
        buy_prefix[i + 1] = buy_prefix[i] + b.buy_volume_usd
        sell_prefix[i + 1] = sell_prefix[i] + b.sell_volume_usd

    # Map timestamp → index for binary search
    timestamps = [b.timestamp_sec for b in buckets]

    def find_window_start(current_idx: int) -> int:
        """Find the first bucket index within the rolling window."""
        target_ts = buckets[current_idx].timestamp_sec - cfg.window_sec
        lo, hi = 0, current_idx
        while lo < hi:
            mid = (lo + hi) // 2
            if timestamps[mid] < target_ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    # State
    in_position = False
    position_side = ""
    entry_price = 0.0
    entry_idx = 0
    entry_intensity = 0.0
    last_exit_idx = -1
    signal_start_idx = -1
    signal_side = ""
    notional = 0.0
    qty = 0.0

    cooldown_end_ts = 0

    for i in range(cfg.window_sec, n):  # skip first window_sec to have full window
        b = buckets[i]
        ts = b.timestamp_sec
        price = b.close_price

        # ── Position management ──────────────────────────────────────
        if in_position:
            hold_sec = ts - buckets[entry_idx].timestamp_sec

            # Check SL/TP using high/low of this second
            exit_reason = None
            exit_price = 0.0

            if position_side == "LONG":
                # SL check (low)
                sl_price = entry_price * (1 - cfg.sl_pct)
                tp_price = entry_price * (1 + cfg.tp_pct)
                if b.low_price <= sl_price:
                    exit_reason = "SL"
                    exit_price = sl_price
                elif b.high_price >= tp_price:
                    exit_reason = "TP"
                    exit_price = tp_price
            else:  # SHORT
                sl_price = entry_price * (1 + cfg.sl_pct)
                tp_price = entry_price * (1 - cfg.tp_pct)
                if b.high_price >= sl_price:
                    exit_reason = "SL"
                    exit_price = sl_price
                elif b.low_price <= tp_price:
                    exit_reason = "TP"
                    exit_price = tp_price

            if not exit_reason and hold_sec >= cfg.max_hold_sec:
                exit_reason = "TIMEOUT"
                exit_price = price

            if exit_reason:
                if position_side == "LONG":
                    pnl_gross = (exit_price - entry_price) * qty
                else:
                    pnl_gross = (entry_price - exit_price) * qty

                fee = FEE_PCT * notional + FEE_PCT * abs(exit_price * qty)
                slip = SLIPPAGE_PCT * notional
                pnl_net = pnl_gross - fee - slip

                result.trades.append(Trade(
                    entry_time_sec=buckets[entry_idx].timestamp_sec,
                    exit_time_sec=ts,
                    side=position_side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    delta_intensity=entry_intensity,
                    pnl_usd=pnl_net,
                    pnl_pct=pnl_net / balance * 100 if balance > 0 else 0,
                    result=exit_reason,
                    hold_seconds=hold_sec,
                ))

                balance += pnl_net
                if balance > peak_balance:
                    peak_balance = balance
                dd = (peak_balance - balance) / peak_balance * 100 if peak_balance > 0 else 0
                if dd > max_dd:
                    max_dd = dd

                in_position = False
                cooldown_end_ts = ts + cfg.cooldown_sec
                continue

            continue  # still in position, no exit

        # ── Signal detection ─────────────────────────────────────────
        if ts < cooldown_end_ts:
            continue

        # Compute rolling intensity using prefix sums
        win_start = find_window_start(i)
        buy_vol = buy_prefix[i + 1] - buy_prefix[win_start]
        sell_vol = sell_prefix[i + 1] - sell_prefix[win_start]
        total_vol = buy_vol + sell_vol

        if total_vol < cfg.min_volume_usd:
            signal_side = ""
            continue

        intensity = (buy_vol - sell_vol) / total_vol

        if abs(intensity) >= cfg.intensity_threshold:
            new_side = "LONG" if intensity > 0 else "SHORT"

            if signal_side == new_side and signal_start_idx >= 0:
                elapsed = ts - buckets[signal_start_idx].timestamp_sec
                if elapsed >= cfg.persistence_sec:
                    # ENTER
                    in_position = True
                    position_side = new_side
                    entry_price = price * (1 + SLIPPAGE_PCT) if new_side == "LONG" \
                        else price * (1 - SLIPPAGE_PCT)
                    entry_idx = i
                    entry_intensity = intensity
                    notional = balance * LEVERAGE * POSITION_PCT
                    qty = notional / entry_price
                    signal_side = ""
                    signal_start_idx = -1
            else:
                signal_side = new_side
                signal_start_idx = i
        else:
            signal_side = ""
            signal_start_idx = -1

    result.final_balance = balance
    result.max_drawdown_pct = max_dd
    return result


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_result(r: BacktestResult) -> None:
    if r.total_trades == 0:
        return
    cfg = r.config
    ret = (r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
    print(f"\n{'='*65}")
    print(f"  Win={cfg.window_sec}s Inten={cfg.intensity_threshold:.2f} "
          f"Persist={cfg.persistence_sec}s SL={cfg.sl_pct*100:.1f}% TP={cfg.tp_pct*100:.1f}%")
    print(f"{'='*65}")
    print(f"  Trades:        {r.total_trades}")
    print(f"  Win / Loss:    {r.wins} / {r.total_trades - r.wins}")
    print(f"  Win rate:      {r.win_rate:.1f}%")
    print(f"  Avg win:       ${r.avg_win:.2f}")
    print(f"  Avg loss:      ${r.avg_loss:.2f}")
    print(f"  Profit factor: {r.profit_factor:.2f}")
    print(f"  Total PnL:     ${r.total_pnl:.2f}")
    print(f"  Return:        {ret:+.2f}%")
    print(f"  Max drawdown:  {r.max_drawdown_pct:.2f}%")
    print(f"  Avg hold:      {r.avg_hold_sec:.0f}s")

    exits = {}
    for t in r.trades:
        exits[t.result] = exits.get(t.result, 0) + 1
    print(f"  Exit types:    {exits}")

    # Daily breakdown
    days: dict[str, dict] = {}
    for t in r.trades:
        day = datetime.fromtimestamp(t.entry_time_sec, tz=timezone.utc).strftime("%Y-%m-%d")
        if day not in days:
            days[day] = {"trades": 0, "pnl": 0.0, "wins": 0}
        days[day]["trades"] += 1
        days[day]["pnl"] += t.pnl_usd
        if t.pnl_usd > 0:
            days[day]["wins"] += 1
    print(f"  Daily:")
    for day in sorted(days):
        d = days[day]
        wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
        print(f"    {day}: {d['trades']:3d} trades, WR {wr:5.1f}%, PnL ${d['pnl']:+.2f}")


def print_summary(all_results: list[BacktestResult], span_days: float) -> None:
    has_trades = [r for r in all_results if r.total_trades >= 3]
    if not has_trades:
        print("\n  NO CONFIGURATIONS PRODUCED 3+ TRADES.")
        return

    print(f"\n{'='*105}")
    print(f"  SUMMARY — {span_days:.1f} days, ranked by PnL (top 20)")
    print(f"{'='*105}")
    print(f"  {'Win':>4s} {'Int':>5s} {'Per':>4s} {'SL%':>5s} {'TP%':>5s} "
          f"{'#Tr':>5s} {'WR%':>6s} {'PnL$':>9s} {'Ret%':>7s} {'MxDD%':>6s} "
          f"{'PF':>5s} {'AvgW$':>7s} {'AvgL$':>7s} {'Hold':>5s} {'Tr/d':>5s}")
    print(f"  {'-'*4} {'-'*5} {'-'*4} {'-'*5} {'-'*5} "
          f"{'-'*5} {'-'*6} {'-'*9} {'-'*7} {'-'*6} "
          f"{'-'*5} {'-'*7} {'-'*7} {'-'*5} {'-'*5}")

    sorted_r = sorted(has_trades, key=lambda r: r.total_pnl, reverse=True)
    for r in sorted_r[:20]:
        c = r.config
        ret = (r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
        tpd = r.total_trades / span_days if span_days > 0 else 0
        print(
            f"  {c.window_sec:4d} {c.intensity_threshold:5.2f} {c.persistence_sec:4d} "
            f"{c.sl_pct*100:5.1f} {c.tp_pct*100:5.1f} "
            f"{r.total_trades:5d} {r.win_rate:6.1f} {r.total_pnl:9.2f} {ret:7.2f} "
            f"{r.max_drawdown_pct:6.2f} {r.profit_factor:5.2f} "
            f"{r.avg_win:7.2f} {r.avg_loss:7.2f} {r.avg_hold_sec:4.0f}s {tpd:5.1f}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Order Flow Backtest (optimized)")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--download", action="store_true", help="Download data only")
    args = parser.parse_args()

    symbol = args.symbol.upper()

    print("=" * 65)
    print(f"  ORDER FLOW BACKTEST — {symbol}")
    print("=" * 65)
    print(f"  Balance: ${INITIAL_BALANCE}, Leverage: {LEVERAGE}x, "
          f"Position: {POSITION_PCT*100:.0f}%")
    print(f"  Fees: {FEE_PCT*100:.2f}%/side, Slippage: {SLIPPAGE_PCT*100:.2f}%/side")
    print()

    # 1. Download
    print(f"Step 1: Data ({args.days} days of {symbol} aggTrades)...")
    csv_paths = download_aggtrades(symbol, days=args.days)
    if not csv_paths:
        print("ERROR: No data!")
        return
    if args.download:
        print(f"\nDownload complete. Run without --download to backtest.")
        return

    # 2. Pre-aggregate into 1-second buckets
    print(f"\nStep 2: Aggregating into 1-second buckets...")
    t0 = time.time()
    buckets = aggregate_to_buckets(csv_paths)
    agg_time = time.time() - t0
    print(f"  Aggregation took {agg_time:.1f}s")

    if not buckets:
        print("ERROR: No data after aggregation!")
        return

    span_sec = buckets[-1].timestamp_sec - buckets[0].timestamp_sec
    span_days = span_sec / 86400

    total_buy = sum(b.buy_volume_usd for b in buckets)
    total_sell = sum(b.sell_volume_usd for b in buckets)
    print(f"  Span: {span_days:.1f} days")
    print(f"  Buy volume:  ${total_buy/1e9:.2f}B")
    print(f"  Sell volume: ${total_sell/1e9:.2f}B")

    # 3. Run backtests — Phase 1: coarse grid (fast)
    print(f"\nStep 3: Coarse grid search...")

    configs = []
    for window in [10, 20, 30, 60]:
        for intensity in [0.30, 0.45, 0.60]:
            for persist in [5, 10, 20]:
                for sl, tp in [(0.003, 0.005), (0.004, 0.007), (0.006, 0.010)]:
                    configs.append(BacktestConfig(
                        window_sec=window,
                        intensity_threshold=intensity,
                        persistence_sec=persist,
                        sl_pct=sl,
                        tp_pct=tp,
                        cooldown_sec=30,
                        max_hold_sec=300,
                        min_volume_usd=50_000,
                    ))

    print(f"  Testing {len(configs)} configs...", flush=True)
    t0 = time.time()
    all_results = []
    for i, cfg in enumerate(configs):
        r = run_backtest(buckets, cfg)
        all_results.append(r)
        if (i + 1) % 20 == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (len(configs) - i - 1)
            print(f"    {i+1}/{len(configs)} done ({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)")

    total_time = time.time() - t0
    print(f"  Completed in {total_time:.1f}s ({total_time/len(configs):.2f}s per config)")

    # 4. Results
    has_trades = [r for r in all_results if r.total_trades >= 3]
    if has_trades:
        top5 = sorted(has_trades, key=lambda r: r.total_pnl, reverse=True)[:5]
        print(f"\n  Top 5 configs (detailed):")
        for r in top5:
            print_result(r)

    print_summary(all_results, span_days)

    # 5. Recommendation
    viable = [r for r in all_results if r.total_trades >= 3]
    if viable:
        best = max(viable, key=lambda r: r.total_pnl)
        c = best.config
        daily_pnl = best.total_pnl / span_days if span_days > 0 else 0
        daily_trades = best.total_trades / span_days if span_days > 0 else 0

        print(f"\n  BEST CONFIG:")
        print(f"    Window:       {c.window_sec}s")
        print(f"    Intensity:    {c.intensity_threshold:.2f}")
        print(f"    Persistence:  {c.persistence_sec}s")
        print(f"    SL / TP:      {c.sl_pct*100:.1f}% / {c.tp_pct*100:.1f}%")
        print(f"    Trades/day:   {daily_trades:.1f}")
        print(f"    Daily PnL:    ${daily_pnl:.2f}")
        print(f"    Monthly est:  ${daily_pnl*30:.2f} ({daily_pnl*30/INITIAL_BALANCE*100:.1f}%)")
        print(f"    Win rate:     {best.win_rate:.1f}%")
        print(f"    Max drawdown: {best.max_drawdown_pct:.2f}%")
        print(f"    Profit factor:{best.profit_factor:.2f}")

        if best.profit_factor >= 1.3 and best.win_rate >= 52:
            print(f"\n  VERDICT: Strategy looks VIABLE. Proceed with bot implementation.")
        elif best.profit_factor >= 1.1:
            print(f"\n  VERDICT: MARGINAL edge. Needs tighter execution or better filters.")
        else:
            print(f"\n  VERDICT: NO CLEAR EDGE. Consider different approach.")

        # Save trades
        if best.trades:
            csv_out = os.path.join(os.path.dirname(__file__), "..", "data",
                                   f"orderflow_backtest_{symbol.lower()}.csv")
            os.makedirs(os.path.dirname(csv_out), exist_ok=True)
            with open(csv_out, "w") as f:
                f.write("entry_time,exit_time,side,entry_price,exit_price,"
                        "intensity,pnl_usd,pnl_pct,result,hold_sec\n")
                for t in best.trades:
                    et = datetime.fromtimestamp(t.entry_time_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    xt = datetime.fromtimestamp(t.exit_time_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{et},{xt},{t.side},{t.entry_price:.2f},{t.exit_price:.2f},"
                            f"{t.delta_intensity:.4f},{t.pnl_usd:.2f},{t.pnl_pct:.4f},"
                            f"{t.result},{t.hold_seconds}\n")
            print(f"\n  Trades saved to {csv_out}")
    else:
        print(f"\n  NO VIABLE CONFIGS found for {symbol}.")


if __name__ == "__main__":
    main()
