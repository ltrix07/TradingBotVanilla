"""
orderflow_backtest.py — Backtest order flow (aggTrades delta) strategy.

Downloads historical aggTrades from data.binance.vision, computes rolling
buy/sell delta, simulates entries/exits, and prints performance metrics.

Usage:
    # 1. Download data first (run separately — large files):
    python scripts/orderflow_backtest.py --download --days 7

    # 2. Run backtest on downloaded data:
    python scripts/orderflow_backtest.py

    # 3. Specific symbol:
    python scripts/orderflow_backtest.py --symbol ETHUSDT --days 5

Data source:
    https://data.binance.vision/data/futures/um/daily/aggTrades/{SYMBOL}/
    CSV: agg_trade_id, price, quantity, first_trade_id, last_trade_id,
         transact_time, is_buyer_maker

Strategy:
    - Compute rolling buy/sell delta over a time window (e.g. 30s)
    - Delta intensity = net_delta / total_volume (normalized to [-1, +1])
    - When intensity > +threshold for persistence_sec → LONG
    - When intensity < -threshold for persistence_sec → SHORT
    - SL/TP as percentage of entry price
    - Cooldown between trades to avoid overtrading
"""

import argparse
import csv
import gzip
import io
import os
import sys
import time
import urllib.request
import zipfile
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta


# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "aggtrades")
BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"

LEVERAGE = 5
POSITION_PCT = 0.30
INITIAL_BALANCE = 1000.0
FEE_PCT = 0.0005           # 0.05% per side → 0.10% round-trip
SLIPPAGE_PCT = 0.0002      # 0.02% per side


# ── Data download ─────────────────────────────────────────────────────────────

def download_aggtrades(symbol: str, days: int = 7) -> list[str]:
    """Download aggTrades CSVs from data.binance.vision. Returns list of file paths."""
    os.makedirs(DATA_DIR, exist_ok=True)

    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)  # yesterday
    paths = []

    for i in range(days):
        date = end_date - timedelta(days=days - 1 - i)
        date_str = date.strftime("%Y-%m-%d")
        csv_path = os.path.join(DATA_DIR, f"{symbol}-aggTrades-{date_str}.csv")

        if os.path.exists(csv_path):
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f"  {date_str}: already exists ({size_mb:.1f} MB)")
            paths.append(csv_path)
            continue

        url = f"{BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
        print(f"  {date_str}: downloading from {url} ...", end="", flush=True)

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=120)
            zip_data = resp.read()

            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                # ZIP contains one CSV file
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f_in:
                    with open(csv_path, "wb") as f_out:
                        f_out.write(f_in.read())

            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f" OK ({size_mb:.1f} MB)")
            paths.append(csv_path)
            time.sleep(0.5)

        except Exception as e:
            print(f" FAILED: {e}")
            # Try .zip with uppercase
            continue

    return paths


# ── Data loading ──────────────────────────────────────────────────────────────

@dataclass
class AggTrade:
    """Single aggregated trade from Binance."""
    timestamp_ms: int
    price: float
    qty: float
    is_buyer_maker: bool  # True = aggressive SELL, False = aggressive BUY

    @property
    def is_buy(self) -> bool:
        """True if the aggressor was a buyer (taker buy)."""
        return not self.is_buyer_maker

    @property
    def usd_volume(self) -> float:
        return self.price * self.qty


def load_trades(csv_path: str) -> list[AggTrade]:
    """Load aggTrades from CSV. Returns sorted list."""
    trades = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                # Columns: agg_trade_id, price, quantity, first_trade_id,
                #          last_trade_id, transact_time, is_buyer_maker
                trades.append(AggTrade(
                    timestamp_ms=int(row[5]),
                    price=float(row[1]),
                    qty=float(row[2]),
                    is_buyer_maker=(row[6].strip().lower() == "true"),
                ))
            except (ValueError, IndexError):
                continue  # skip header or malformed rows
    return trades


# ── Rolling delta engine ─────────────────────────────────────────────────────

class DeltaEngine:
    """Computes rolling buy/sell delta over a time window.

    Delta = sum(buy_volume) - sum(sell_volume) over the last `window_ms`.
    Intensity = delta / total_volume, normalized to [-1, +1].

    A persistent strong intensity indicates real order flow pressure,
    not just a single large order (which could be a stop hunt).
    """

    def __init__(self, window_ms: int = 30_000):
        self.window_ms = window_ms
        self._buys: deque[tuple[int, float]] = deque()   # (ts_ms, usd_vol)
        self._sells: deque[tuple[int, float]] = deque()  # (ts_ms, usd_vol)
        self._buy_sum: float = 0.0
        self._sell_sum: float = 0.0

    def update(self, trade: AggTrade) -> None:
        """Add a new trade and evict expired ones."""
        ts = trade.timestamp_ms
        usd = trade.usd_volume
        cutoff = ts - self.window_ms

        if trade.is_buy:
            self._buys.append((ts, usd))
            self._buy_sum += usd
        else:
            self._sells.append((ts, usd))
            self._sell_sum += usd

        # Evict old buys
        while self._buys and self._buys[0][0] < cutoff:
            _, old_usd = self._buys.popleft()
            self._buy_sum -= old_usd

        # Evict old sells
        while self._sells and self._sells[0][0] < cutoff:
            _, old_usd = self._sells.popleft()
            self._sell_sum -= old_usd

    @property
    def delta(self) -> float:
        """Net delta in USD: positive = more aggressive buying."""
        return self._buy_sum - self._sell_sum

    @property
    def total_volume(self) -> float:
        return self._buy_sum + self._sell_sum

    @property
    def intensity(self) -> float:
        """Delta / total_volume, in [-1, +1]. 0 = balanced."""
        total = self.total_volume
        if total < 1.0:
            return 0.0
        return self.delta / total

    @property
    def buy_volume(self) -> float:
        return self._buy_sum

    @property
    def sell_volume(self) -> float:
        return self._sell_sum

    def reset(self) -> None:
        self._buys.clear()
        self._sells.clear()
        self._buy_sum = 0.0
        self._sell_sum = 0.0


# ── Backtest engine ───────────────────────────────────────────────────────────

@dataclass
class Trade:
    entry_time_ms: int
    exit_time_ms: int
    side: str           # "LONG" or "SHORT"
    entry_price: float
    exit_price: float
    delta_intensity: float
    pnl_usd: float
    pnl_pct: float
    result: str         # "TP", "SL", "TIMEOUT"
    hold_seconds: float


@dataclass
class BacktestConfig:
    window_sec: int         # rolling delta window (seconds)
    intensity_threshold: float  # min |intensity| to trigger signal
    persistence_sec: float  # how long intensity must stay above threshold
    sl_pct: float           # stop loss percentage
    tp_pct: float           # take profit percentage
    cooldown_sec: float     # minimum seconds between trades
    max_hold_sec: float     # max hold time before timeout exit
    min_volume_usd: float   # min total volume in window to consider signal valid


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
        l = [t.pnl_usd for t in self.trades if t.pnl_usd <= 0]
        return sum(l) / len(l) if l else 0

    @property
    def avg_hold_sec(self) -> float:
        if not self.trades:
            return 0
        return sum(t.hold_seconds for t in self.trades) / len(self.trades)

    @property
    def profit_factor(self) -> float:
        gross_win = sum(t.pnl_usd for t in self.trades if t.pnl_usd > 0)
        gross_loss = abs(sum(t.pnl_usd for t in self.trades if t.pnl_usd <= 0))
        return gross_win / gross_loss if gross_loss > 0 else 0


def run_backtest(all_trades: list[AggTrade], cfg: BacktestConfig) -> BacktestResult:
    """Run a single backtest configuration on pre-loaded aggTrades."""
    result = BacktestResult(config=cfg)
    balance = INITIAL_BALANCE
    peak_balance = balance
    max_dd = 0.0

    engine = DeltaEngine(window_ms=cfg.window_sec * 1000)

    # State machine
    in_position = False
    position_side = ""
    entry_price = 0.0
    entry_time_ms = 0
    entry_intensity = 0.0
    last_exit_time_ms = 0
    signal_start_ms = 0        # when intensity first crossed threshold
    current_signal_side = ""   # "LONG" or "SHORT" or ""

    notional = 0.0
    qty = 0.0

    for trade in all_trades:
        engine.update(trade)

        ts = trade.timestamp_ms
        price = trade.price
        intensity = engine.intensity
        total_vol = engine.total_volume

        # ── Position management: check SL/TP/timeout ─────────────────────
        if in_position:
            hold_sec = (ts - entry_time_ms) / 1000

            # Calculate current PnL
            if position_side == "LONG":
                current_pnl_pct = (price - entry_price) / entry_price
            else:
                current_pnl_pct = (entry_price - price) / entry_price

            exit_reason = None

            if current_pnl_pct >= cfg.tp_pct:
                exit_reason = "TP"
                exit_price = entry_price * (1 + cfg.tp_pct) if position_side == "LONG" \
                    else entry_price * (1 - cfg.tp_pct)
            elif current_pnl_pct <= -cfg.sl_pct:
                exit_reason = "SL"
                exit_price = entry_price * (1 - cfg.sl_pct) if position_side == "LONG" \
                    else entry_price * (1 + cfg.sl_pct)
            elif hold_sec >= cfg.max_hold_sec:
                exit_reason = "TIMEOUT"
                exit_price = price

            if exit_reason:
                # Calculate PnL
                if position_side == "LONG":
                    pnl_gross = (exit_price - entry_price) * qty
                else:
                    pnl_gross = (entry_price - exit_price) * qty

                fee = FEE_PCT * notional + FEE_PCT * abs(exit_price * qty)
                slip = SLIPPAGE_PCT * notional
                pnl_net = pnl_gross - fee - slip
                pnl_pct = pnl_net / balance * 100

                result.trades.append(Trade(
                    entry_time_ms=entry_time_ms,
                    exit_time_ms=ts,
                    side=position_side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    delta_intensity=entry_intensity,
                    pnl_usd=pnl_net,
                    pnl_pct=pnl_pct,
                    result=exit_reason,
                    hold_seconds=hold_sec,
                ))

                balance += pnl_net
                if balance > peak_balance:
                    peak_balance = balance
                dd = (peak_balance - balance) / peak_balance * 100
                if dd > max_dd:
                    max_dd = dd

                in_position = False
                last_exit_time_ms = ts
                continue

        # ── Signal detection (only when not in position) ─────────────────
        if in_position:
            continue

        # Cooldown check
        if ts - last_exit_time_ms < cfg.cooldown_sec * 1000:
            continue

        # Volume gate
        if total_vol < cfg.min_volume_usd:
            continue

        # Detect persistent intensity
        if abs(intensity) >= cfg.intensity_threshold:
            new_side = "LONG" if intensity > 0 else "SHORT"

            if current_signal_side == new_side:
                # Check if persistence requirement is met
                elapsed_ms = ts - signal_start_ms
                if elapsed_ms >= cfg.persistence_sec * 1000:
                    # SIGNAL FIRED → ENTER
                    in_position = True
                    position_side = new_side
                    # Slippage on entry
                    if new_side == "LONG":
                        entry_price = price * (1 + SLIPPAGE_PCT)
                    else:
                        entry_price = price * (1 - SLIPPAGE_PCT)
                    entry_time_ms = ts
                    entry_intensity = intensity
                    notional = balance * LEVERAGE * POSITION_PCT
                    qty = notional / entry_price

                    # Reset signal tracking
                    current_signal_side = ""
                    signal_start_ms = 0
            else:
                # New signal direction — reset timer
                current_signal_side = new_side
                signal_start_ms = ts
        else:
            # Intensity dropped below threshold — reset
            current_signal_side = ""
            signal_start_ms = 0

    # Close any open position at last price
    if in_position and all_trades:
        last_price = all_trades[-1].price
        hold_sec = (all_trades[-1].timestamp_ms - entry_time_ms) / 1000
        if position_side == "LONG":
            pnl_gross = (last_price - entry_price) * qty
        else:
            pnl_gross = (entry_price - last_price) * qty
        fee = FEE_PCT * notional + FEE_PCT * abs(last_price * qty)
        slip = SLIPPAGE_PCT * notional
        pnl_net = pnl_gross - fee - slip
        balance += pnl_net
        result.trades.append(Trade(
            entry_time_ms=entry_time_ms,
            exit_time_ms=all_trades[-1].timestamp_ms,
            side=position_side,
            entry_price=entry_price,
            exit_price=last_price,
            delta_intensity=entry_intensity,
            pnl_usd=pnl_net,
            pnl_pct=pnl_net / balance * 100,
            result="EOF",
            hold_seconds=hold_sec,
        ))

    result.final_balance = balance
    result.max_drawdown_pct = max_dd
    return result


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_result(r: BacktestResult, verbose: bool = False) -> None:
    if r.total_trades == 0:
        return

    cfg = r.config
    ret = (r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100

    print(f"\n{'='*65}")
    print(f"  Window {cfg.window_sec}s | Intensity {cfg.intensity_threshold:.2f} | "
          f"Persist {cfg.persistence_sec}s | SL {cfg.sl_pct*100:.1f}% | TP {cfg.tp_pct*100:.1f}%")
    print(f"{'='*65}")
    print(f"  Trades:       {r.total_trades}")
    print(f"  Win / Loss:   {r.wins} / {r.total_trades - r.wins}")
    print(f"  Win rate:     {r.win_rate:.1f}%")
    print(f"  Avg win:      ${r.avg_win:.2f}")
    print(f"  Avg loss:     ${r.avg_loss:.2f}")
    print(f"  Profit factor:{r.profit_factor:.2f}")
    print(f"  Total PnL:    ${r.total_pnl:.2f}")
    print(f"  Return:       {ret:.2f}%")
    print(f"  Max drawdown: {r.max_drawdown_pct:.2f}%")
    print(f"  Avg hold:     {r.avg_hold_sec:.0f}s ({r.avg_hold_sec/60:.1f}min)")

    # Exit type distribution
    exits = {}
    for t in r.trades:
        exits[t.result] = exits.get(t.result, 0) + 1
    print(f"  Exit types:   {exits}")

    # Daily breakdown
    if verbose and r.trades:
        days = {}
        for t in r.trades:
            day = datetime.fromtimestamp(t.entry_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if day not in days:
                days[day] = {"trades": 0, "pnl": 0.0, "wins": 0}
            days[day]["trades"] += 1
            days[day]["pnl"] += t.pnl_usd
            if t.pnl_usd > 0:
                days[day]["wins"] += 1

        print(f"\n  Daily breakdown:")
        for day in sorted(days):
            d = days[day]
            wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
            print(f"    {day}: {d['trades']:3d} trades, WR {wr:5.1f}%, PnL ${d['pnl']:+.2f}")


def print_summary(all_results: list[BacktestResult], days: int) -> None:
    has_trades = [r for r in all_results if r.total_trades > 0]
    if not has_trades:
        print("\n  NO CONFIGURATIONS PRODUCED TRADES.")
        return

    print(f"\n{'='*100}")
    print(f"  SUMMARY — {days} days of data, ranked by PnL")
    print(f"{'='*100}")
    print(f"  {'Win':>4s} {'Pers':>5s} {'Inten':>6s} {'SL%':>5s} {'TP%':>5s} "
          f"{'Trades':>7s} {'WR%':>6s} {'PnL$':>8s} {'Ret%':>7s} {'MaxDD%':>7s} "
          f"{'PF':>5s} {'AvgW$':>7s} {'AvgL$':>7s} {'Hold':>6s}")
    print(f"  {'-'*4} {'-'*5} {'-'*6} {'-'*5} {'-'*5} "
          f"{'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*7} "
          f"{'-'*5} {'-'*7} {'-'*7} {'-'*6}")

    sorted_results = sorted(has_trades, key=lambda r: r.total_pnl, reverse=True)
    for r in sorted_results[:20]:  # top 20
        cfg = r.config
        ret = (r.final_balance - INITIAL_BALANCE) / INITIAL_BALANCE * 100
        print(
            f"  {cfg.window_sec:4d} {cfg.persistence_sec:5.0f} {cfg.intensity_threshold:6.2f} "
            f"{cfg.sl_pct*100:5.1f} {cfg.tp_pct*100:5.1f} "
            f"{r.total_trades:7d} {r.win_rate:6.1f} {r.total_pnl:8.2f} {ret:7.2f} "
            f"{r.max_drawdown_pct:7.2f} {r.profit_factor:5.2f} "
            f"{r.avg_win:7.2f} {r.avg_loss:7.2f} {r.avg_hold_sec:5.0f}s"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Order Flow (aggTrades Delta) Backtest")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (default: BTCUSDT)")
    parser.add_argument("--days", type=int, default=7, help="Days of data (default: 7)")
    parser.add_argument("--download", action="store_true", help="Download data only (no backtest)")
    parser.add_argument("--verbose", action="store_true", help="Show daily breakdown for each config")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    days = args.days

    print("=" * 65)
    print(f"  ORDER FLOW BACKTEST — {symbol}")
    print("=" * 65)
    print(f"  Balance: ${INITIAL_BALANCE}, Leverage: {LEVERAGE}x")
    print(f"  Position: {POSITION_PCT*100:.0f}% of balance per trade")
    print(f"  Fees: {FEE_PCT*100:.2f}%/side, Slippage: {SLIPPAGE_PCT*100:.2f}%/side")
    print()

    # 1. Download data
    print(f"Step 1: Checking/downloading {days} days of {symbol} aggTrades...")
    csv_paths = download_aggtrades(symbol, days=days)
    if not csv_paths:
        print("ERROR: No data files available!")
        return

    if args.download:
        print(f"\nDownload complete. {len(csv_paths)} files ready.")
        print(f"Run without --download to execute backtest.")
        return

    # 2. Load all trades
    print(f"\nStep 2: Loading trades from {len(csv_paths)} files...")
    all_trades = []
    for path in csv_paths:
        day_trades = load_trades(path)
        print(f"  {os.path.basename(path)}: {len(day_trades):,} trades")
        all_trades.extend(day_trades)

    # Sort by timestamp (should already be sorted, but make sure)
    all_trades.sort(key=lambda t: t.timestamp_ms)
    print(f"  Total: {len(all_trades):,} trades")

    if not all_trades:
        print("ERROR: No trades loaded!")
        return

    # Time span
    span_sec = (all_trades[-1].timestamp_ms - all_trades[0].timestamp_ms) / 1000
    span_days = span_sec / 86400
    print(f"  Span: {span_days:.1f} days")

    # Quick stats
    total_buy_vol = sum(t.usd_volume for t in all_trades if t.is_buy)
    total_sell_vol = sum(t.usd_volume for t in all_trades if not t.is_buy)
    print(f"  Buy volume:  ${total_buy_vol/1e9:.2f}B")
    print(f"  Sell volume: ${total_sell_vol/1e9:.2f}B")
    print(f"  Net delta:   ${(total_buy_vol - total_sell_vol)/1e6:+.1f}M")

    # 3. Run backtests with multiple configurations
    print(f"\nStep 3: Running backtests...")

    configs = []

    # Vary: window size, intensity threshold, persistence, SL/TP
    for window_sec in [15, 30, 60]:
        for intensity in [0.30, 0.40, 0.50, 0.60, 0.70]:
            for persist in [5, 10, 20]:
                for sl, tp in [(0.003, 0.004), (0.004, 0.006), (0.005, 0.008),
                               (0.003, 0.006), (0.005, 0.010)]:
                    configs.append(BacktestConfig(
                        window_sec=window_sec,
                        intensity_threshold=intensity,
                        persistence_sec=persist,
                        sl_pct=sl,
                        tp_pct=tp,
                        cooldown_sec=30,
                        max_hold_sec=300,   # 5 min max hold
                        min_volume_usd=50_000,  # min $50K volume in window
                    ))

    print(f"  Testing {len(configs)} configurations...")

    all_results = []
    for i, cfg in enumerate(configs):
        if (i + 1) % 50 == 0:
            print(f"    ... {i+1}/{len(configs)}")
        result = run_backtest(all_trades, cfg)
        all_results.append(result)

    # 4. Print results
    # Show top 5 detailed results
    has_trades = sorted(
        [r for r in all_results if r.total_trades >= 5],
        key=lambda r: r.total_pnl, reverse=True,
    )

    if has_trades:
        print(f"\n  Top 5 configurations (detailed):")
        for r in has_trades[:5]:
            print_result(r, verbose=args.verbose)
    else:
        print("\n  No configurations produced 5+ trades.")

    print_summary(all_results, days)

    # 5. Recommendation
    if has_trades:
        best = has_trades[0]
        cfg = best.config
        daily_pnl = best.total_pnl / span_days if span_days > 0 else 0
        daily_trades = best.total_trades / span_days if span_days > 0 else 0

        print(f"\n  RECOMMENDED CONFIG:")
        print(f"    Window:       {cfg.window_sec}s")
        print(f"    Intensity:    {cfg.intensity_threshold:.2f}")
        print(f"    Persistence:  {cfg.persistence_sec}s")
        print(f"    SL/TP:        {cfg.sl_pct*100:.1f}% / {cfg.tp_pct*100:.1f}%")
        print(f"    Cooldown:     {cfg.cooldown_sec}s")
        print(f"    Trades/day:   {daily_trades:.1f}")
        print(f"    Daily PnL:    ${daily_pnl:.2f}")
        print(f"    Monthly est:  ${daily_pnl * 30:.2f} ({daily_pnl * 30 / INITIAL_BALANCE * 100:.1f}%)")
        print(f"    Win rate:     {best.win_rate:.1f}%")
        print(f"    Max drawdown: {best.max_drawdown_pct:.2f}%")
        print(f"    Profit factor:{best.profit_factor:.2f}")

        # Verdict
        if best.profit_factor >= 1.3 and best.win_rate >= 52:
            print(f"\n  VERDICT: Strategy looks viable. Proceed with implementation.")
        elif best.profit_factor >= 1.1:
            print(f"\n  VERDICT: Marginal edge. Needs tighter execution or better filters.")
        else:
            print(f"\n  VERDICT: No clear edge found. Consider different approach.")

        # Save best trades
        if best.trades:
            csv_out = f"data/orderflow_backtest_{symbol.lower()}.csv"
            os.makedirs("data", exist_ok=True)
            with open(csv_out, "w") as f:
                f.write("entry_time,exit_time,side,entry_price,exit_price,"
                        "intensity,pnl_usd,pnl_pct,result,hold_sec\n")
                for t in best.trades:
                    et = datetime.fromtimestamp(t.entry_time_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    xt = datetime.fromtimestamp(t.exit_time_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{et},{xt},{t.side},{t.entry_price:.2f},{t.exit_price:.2f},"
                            f"{t.delta_intensity:.4f},{t.pnl_usd:.2f},{t.pnl_pct:.4f},"
                            f"{t.result},{t.hold_seconds:.0f}\n")
            print(f"\n  Best config trades saved to {csv_out}")


if __name__ == "__main__":
    main()
