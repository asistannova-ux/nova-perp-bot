import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, UTC

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret_pass_2026") # CORRECTED
DB_NAME = os.getenv("DB_NAME", "swing_bot")

INTERVAL = "1h"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

def fetch_all_klines(symbol):
    conn = get_db_connection()
    try:
        query = """
            SELECT time, open, high, low, close, volume 
            FROM klines 
            WHERE symbol = %s AND interval = %s 
            ORDER BY time ASC;
        """
        df = pd.read_sql(query, conn, params=(symbol, INTERVAL))
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        print(f"Error fetching klines for {symbol}: {e}", flush=True)
        return pd.DataFrame()
    finally:
        conn.close()

def run_backtest_for_symbol(symbol, df, weights, fee=0.0005, slippage=0.0005):
    if len(df) < 50:
        return []
        
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()
    
    trades = []
    active_position = None
    
    for i in range(50, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1]
        
        if active_position:
            p = active_position
            if p['direction'] == 'LONG':
                if row['low'] <= p['sl']:
                    exit_price = p['sl'] * (1 - slippage)
                    pnl = (exit_price - p['entry_price']) * p['size'] - (p['entry_price'] + exit_price) * p['size'] * fee
                    trades.append({**p, 'exit_price': exit_price, 'exit_time': row['time'], 'pnl': pnl, 'result': 'SL'})
                    active_position = None
                elif row['high'] >= p['tp']:
                    exit_price = p['tp'] * (1 - slippage)
                    pnl = (exit_price - p['entry_price']) * p['size'] - (p['entry_price'] + exit_price) * p['size'] * fee
                    trades.append({**p, 'exit_price': exit_price, 'exit_time': row['time'], 'pnl': pnl, 'result': 'TP'})
                    active_position = None
            elif p['direction'] == 'SHORT':
                if row['high'] >= p['sl']:
                    exit_price = p['sl'] * (1 + slippage)
                    pnl = (p['entry_price'] - exit_price) * p['size'] - (p['entry_price'] + exit_price) * p['size'] * fee
                    trades.append({**p, 'exit_price': exit_price, 'exit_time': row['time'], 'pnl': pnl, 'result': 'SL'})
                    active_position = None
                elif row['low'] <= p['tp']:
                    exit_price = p['tp'] * (1 + slippage)
                    pnl = (p['entry_price'] - exit_price) * p['size'] - (p['entry_price'] + exit_price) * p['size'] * fee
                    trades.append({**p, 'exit_price': exit_price, 'exit_time': row['time'], 'pnl': pnl, 'result': 'TP'})
                    active_position = None
                    
        if not active_position:
            is_bullish_trend = row['close'] > row['ema_50'] and row['ema_20'] > row['ema_50']
            is_bearish_trend = row['close'] < row['ema_50'] and row['ema_20'] < row['ema_50']
            
            score = 0.0
            if is_bullish_trend or is_bearish_trend: score += 100 * weights["trend_alignment"]
            if row['rsi'] < 35 or row['rsi'] > 65: score += 100 * weights["rsi_divergence"]
            if (prev['macd_hist'] < 0 and row['macd_hist'] > 0) or (prev['macd_hist'] > 0 and row['macd_hist'] < 0): score += 100 * weights["macd_momentum"]
                
            if score > 45:
                direction = "LONG" if is_bullish_trend else "SHORT"
                entry_price = row['close'] * (1 + slippage) if direction == "LONG" else row['close'] * (1 - slippage)
                atr_val = row['atr'] if not np.isnan(row['atr']) else entry_price * 0.02
                sl_dist = atr_val * 2.0
                sl = entry_price - sl_dist if direction == "LONG" else entry_price + sl_dist
                tp = entry_price + (sl_dist * 3.0) if direction == "LONG" else entry_price - (sl_dist * 3.0)
                active_position = {
                    'symbol': symbol, 'direction': direction, 'entry_price': entry_price,
                    'entry_time': row['time'], 'sl': sl, 'tp': tp, 'margin': 150.0, 'leverage': 10,
                    'size': (150.0 * 10) / entry_price
                }
    return trades

def run_portfolio_backtest():
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ARBUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
    weights = {
        "trend_alignment": 0.40, "rsi_divergence": 0.30,
        "macd_momentum": 0.20, "volume_support": 0.10
    }
    print("="*60 + "\nRUNNING WALK-FORWARD MULTI-SYMBOL PORTFOLIO BACKTEST\n" + "="*60, flush=True)
    all_trades = []
    for symbol in symbols:
        df = fetch_all_klines(symbol)
        if df.empty: continue
        trades = run_backtest_for_symbol(symbol, df, weights)
        all_trades.extend(trades)
        print(f"{symbol}: Backtested. Total Trades Generated: {len(trades)}", flush=True)
        
    if not all_trades:
        print("\nBacktest generated 0 trades.")
        return
        
    df_trades = pd.DataFrame(all_trades)
    total_trades = len(df_trades)
    wins = len(df_trades[df_trades['pnl'] > 0])
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
    total_pnl = df_trades['pnl'].sum()
    avg_pnl = df_trades['pnl'].mean()
    
    print("\n" + "="*60 + "\nNİHAİ BACKTEST PERFORMANS RAPORU\n" + "="*60)
    print(f"Toplam İşlem Sayısı   : {total_trades}")
    print(f"Kazanan İşlemler     : {wins} (%{win_rate:.1f})")
    print(f"Kaybeden İşlemler    : {total_trades - wins} (%{100 - win_rate:.1f})")
    print(f"Toplam Net PnL       : {total_pnl:.2f}$")
    print(f"Ortalama İşlem Getirisi: {avg_pnl:.2f}$")
    
    if total_trades > 5:
        std = df_trades['pnl'].std()
        sharpe = (avg_pnl / std) * np.sqrt(365*24) if std > 0 else 0
        print(f"Sharpe Oranı (Saatlik): {sharpe:.2f}")
    print("="*60, flush=True)

if __name__ == "__main__":
    run_portfolio_backtest()
