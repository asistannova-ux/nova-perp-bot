import os
import requests
import psycopg2
from datetime import datetime, timedelta, UTC

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret_pass_2026")
DB_NAME = os.getenv("DB_NAME", "swing_bot")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ARBUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
INTERVAL = "1h"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

def fetch_and_store_historical(symbol, days=30):
    print(f"Fetching historical klines for {symbol} for past {days} days...", flush=True)
    conn = get_db_connection()
    
    # Calculate start time in ms
    start_time = int((datetime.now(UTC) - timedelta(days=days)).timestamp() * 1000)
    limit = 1000 # Binance max limit per call
    
    all_klines = []
    current_start = start_time
    
    while True:
        try:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={INTERVAL}&startTime={current_start}&limit={limit}"
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            data = r.json()
            
            if not data or len(data) == 0:
                break
                
            all_klines.extend(data)
            # Last kline close time + 1 to avoid overlap
            last_close_time = data[-1][6]
            if len(data) < limit:
                break
            current_start = last_close_time + 1
        except Exception as e:
            print(f"Error fetching historical for {symbol}: {e}", flush=True)
            break
            
    # Save to Database
    if all_klines:
        with conn.cursor() as cur:
            inserted = 0
            for k in all_klines:
                dt = datetime.fromtimestamp(k[0] / 1000.0, UTC)
                cur.execute("""
                    INSERT INTO klines (time, symbol, interval, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time, symbol, interval) DO NOTHING;
                """, (dt, symbol, INTERVAL, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
                inserted += cur.rowcount
            conn.commit()
            print(f"Successfully backfilled {inserted} historical klines for {symbol}.", flush=True)
    conn.close()

if __name__ == "__main__":
    print("Starting historical data backfiller...", flush=True)
    for sym in SYMBOLS:
        try:
            fetch_and_store_historical(sym, days=30) # Backfill 30 days for strategy warm-up
        except Exception as e:
            print(f"Failed to backfill {sym}: {e}", flush=True)
