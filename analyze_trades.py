import pandas as pd
import glob
import os

# Пути к файлам (поменяй, если они лежат в другом месте)
TRADES_DIR = "data"
MARKET_DATA_FILE = "data/market_regime.csv" # Файл с 15-минутными логами рынка

def load_and_prepare_data():
    # Загружаем данные рынка
    if not os.path.exists(MARKET_DATA_FILE):
        print(f"Файл {MARKET_DATA_FILE} не найден. Проверьте путь.")
        return None, None

    market_df = pd.read_csv(MARKET_DATA_FILE)
    market_df['timestamp'] = pd.to_datetime(market_df['timestamp'])
    market_df = market_df.sort_values('timestamp')

    # Загружаем все файлы сделок
    trade_files = glob.glob(f"{TRADES_DIR}/trades_*.csv")
    all_trades = []

    for file in trade_files:
        config_name = os.path.basename(file).replace("trades_", "").replace(".csv", "")
        df = pd.read_csv(file)
        if df.empty:
            continue
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['config'] = config_name
        all_trades.append(df)

    if not all_trades:
        print("Файлы сделок не найдены.")
        return None, None

    trades_df = pd.concat(all_trades, ignore_index=True)
    trades_df = trades_df.sort_values('timestamp')
    return trades_df, market_df

def analyze_data(trades_df, market_df):
    # Объединяем сделки с ближайшим предшествующим состоянием рынка (до 15 минут назад)
    merged_df = pd.merge_asof(
        trades_df, 
        market_df, 
        on='timestamp', 
        direction='backward',
        tolerance=pd.Timedelta(minutes=15)
    )

    print("="*50)
    print("📊 АНАЛИЗ КОРРЕЛЯЦИИ ТРЕНДА И РЕЗУЛЬТАТОВ")
    print("="*50)

    # 1. Зависимость Win Rate от силы тренда (ADX)
    # Границы буckets совпадают с orchestrator'ом: <20 sniper, 20–35 volume, ≥35 trend.
    print("\n📈 1. Зависимость Win Rate от силы тренда (ADX):")
    merged_df['adx_bucket'] = pd.cut(
        merged_df['adx_15m'],
        bins=[0, 20, 35, 100],
        labels=['Weak (<20)', 'Medium (20-35)', 'Strong (>=35)']
    )

    # Win rate определяется по знаку PnL, т.к. result={SL,TP,TIME_STOP,MAX_HOLD,REVERSE_CLOSE}
    # и TIME_STOP/MAX_HOLD/REVERSE_CLOSE могут закрыть сделку как в плюс, так и в минус.
    adx_stats = merged_df.groupby(['config', 'adx_bucket']).apply(
        lambda x: pd.Series({
            'Trades': len(x),
            'Win Rate %': (x['pnl'] > 0).mean() * 100 if len(x) > 0 else 0,
            'Total PnL': x['pnl'].sum(),
            'Avg PnL':   x['pnl'].mean() if len(x) > 0 else 0,
        })
    ).reset_index()
    print(adx_stats.to_string())

    # 2. Зависимость от Волатильности (ATR)
    print("\n🌪 2. Корреляция PnL и Волатильности (ATR):")
    for config in merged_df['config'].unique():
        config_data = merged_df[merged_df['config'] == config]
        correlation = config_data['pnl'].corr(config_data['atr_15m'])
        print(f" - {config}: корреляция {correlation:.2f} "
              f"(>0 = бот лучше работает при высокой волатильности, <0 наоборот)")

    # 3. Анализ срабатываний по типам выхода
    print("\n🛑 3. Анализ типов закрытий:")
    for config in merged_df['config'].unique():
        bot = merged_df[merged_df['config'] == config]
        by_result = bot.groupby('result').agg(
            count=('pnl', 'size'),
            total_pnl=('pnl', 'sum'),
            avg_pnl=('pnl', 'mean'),
        ).round(2)
        print(f"\n— {config}:")
        print(by_result.to_string())

if __name__ == "__main__":
    trades, market = load_and_prepare_data()
    if trades is not None and market is not None:
        analyze_data(trades, market)