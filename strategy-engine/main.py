import time
import os
import json
import psycopg2
import redis
import numpy as np
import pandas as pd
from prometheus_client import start_http_server, Counter

# Prometheus Metrics
METRICS_PORT = 8000
SIGNALS_GENERATED = Counter('strategy_engine_signals_generated_total', 'Total signals generated', ['symbol', 'direction'])

# Configuration
DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = "secret_postgres_password_2026"
DB_NAME = os.getenv("DB_NAME", "swing_bot")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

INTERVAL = "1h"

# Default Weights Config for scoring
DEFAULT_WEIGHTS = {
    "trend_alignment": 0.40,
    "rsi_divergence": 0.30,
    "macd_momentum": 0.20,
    "volume_support": 0.10
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

def fetch_recent_klines(symbol, limit=100):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT time, open, high, low, close, volume 
                FROM klines 
                WHERE symbol = %s AND interval = %s 
                ORDER BY time DESC 
                LIMIT %s;
            """, (symbol, INTERVAL, limit))
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows[::-1], columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            return df
    except Exception as e:
        print(f"Error fetching recent klines for {symbol}: {e}", flush=True)
        return pd.DataFrame()
    finally:
        conn.close()

def calculate_indicators(df):
    if len(df) < 30:
        return df
    
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
    
    return df

def score_and_rank(df):
    if df.empty or 'ema_50' not in df.columns:
        return 0.0, "NEUTRAL"
        
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    score = 0.0
    direction = "NEUTRAL"
    
    is_bullish_trend = last['close'] > last['ema_50'] and last['ema_20'] > last['ema_50']
    is_bearish_trend = last['close'] < last['ema_50'] and last['ema_20'] < last['ema_50']
    
    trend_score = 0.0
    if is_bullish_trend or is_bearish_trend:
        trend_score = 100.0
    score += trend_score * DEFAULT_WEIGHTS["trend_alignment"]
    
    rsi_score = 0.0
    if last['rsi'] < 35 or last['rsi'] > 65:
        rsi_score = 100.0
    score += rsi_score * DEFAULT_WEIGHTS["rsi_divergence"]
    
    macd_score = 0.0
    if (prev['macd_hist'] < 0 and last['macd_hist'] > 0) or (prev['macd_hist'] > 0 and last['macd_hist'] < 0):
        macd_score = 100.0
    score += macd_score * DEFAULT_WEIGHTS["macd_momentum"]
    
    vol_score = 0.0
    vol_ma = df['volume'].rolling(20).mean().iloc[-1]
    if last['volume'] > vol_ma:
        vol_score = 100.0
    score += vol_score * DEFAULT_WEIGHTS["volume_support"]
    
    if is_bullish_trend and (last['rsi'] < 45 or last['macd_hist'] > 0):
        direction = "LONG"
    elif is_bearish_trend and (last['rsi'] > 55 or last['macd_hist'] < 0):
        direction = "SHORT"
        
    return score, direction

def generate_signals():
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ARBUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    print("Strategy Engine: Evaluating Screener Rules...", flush=True)
    conn = get_db_connection()
    
    candidates = []
    
    for symbol in symbols:
        df = fetch_recent_klines(symbol, limit=100)
        if df.empty or len(df) < 50:
            continue
            
        df = calculate_indicators(df)
        score, direction = score_and_rank(df)
        
        if direction != "NEUTRAL" and score > 40:
            last_time = df.iloc[-1]['time']
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signals (time, symbol, direction, score, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (time, symbol) DO NOTHING;
                """, (last_time, symbol, direction, score, json.dumps({
                    "rsi": float(df.iloc[-1]['rsi']),
                    "atr": float(df.iloc[-1]['atr']),
                    "close": float(df.iloc[-1]['close'])
                })))
            conn.commit()
            
            candidates.append({
                "symbol": symbol,
                "direction": direction,
                "score": score,
                "close": float(df.iloc[-1]['close']),
                "atr": float(df.iloc[-1]['atr'])
            })
            SIGNALS_GENERATED.labels(symbol=symbol, direction=direction).inc()

    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)
    selected = []
    directions_taken = set()
    
    for cand in candidates:
        if cand["direction"] not in directions_taken or cand["score"] > 80:
            selected.append(cand)
            directions_taken.add(cand["direction"])
            
    if selected:
        print(f"Strategy Engine: Found active signal candidates: {selected}", flush=True)
        r_client.publish("signal_alert", json.dumps(selected))
        
    conn.close()

def r_listener():
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r_client.pubsub()
    pubsub.subscribe("kline_closed")
    
    print("Strategy Engine listening for kline_closed events...", flush=True)
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                generate_signals()
            except Exception as e:
                print(f"Error executing strategy cycle: {e}", flush=True)

if __name__ == "__main__":
    print("Starting Strategy Engine Microservice...", flush=True)
    start_http_server(METRICS_PORT)
    
    try:
        generate_signals()
    except Exception as e:
        print(f"Initial startup signal generation failed: {e}", flush=True)
        
    r_listener()
