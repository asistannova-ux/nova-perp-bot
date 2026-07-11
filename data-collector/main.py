import asyncio
import os
import json
import psycopg2
from psycopg2.extras import execute_values
import redis
import websockets
from datetime import datetime, UTC
from prometheus_client import start_http_server, Counter

# Prometheus metrics
METRICS_PORT = 8000
KLINES_COLLECTED = Counter('data_collector_klines_collected_total', 'Total klines collected and saved', ['symbol'])
WS_ERRORS = Counter('data_collector_ws_errors_total', 'Total WebSocket connection errors')

# Configuration
DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret_pass_2026")
DB_NAME = os.getenv("DB_NAME", "swing_bot")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Standard Universe of Symbols (Weekly volume $10M+)
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ARBUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
INTERVAL = "1h"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )

def save_kline_to_db(conn, symbol, time_ms, open_p, high, low, close, volume):
    with conn.cursor() as cur:
        # Convert ms timestamp to ISO timestamptz
        dt = datetime.fromtimestamp(time_ms / 1000.0, UTC)
        cur.execute("""
            INSERT INTO klines (time, symbol, interval, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, symbol, interval) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """, (dt, symbol, INTERVAL, open_p, high, low, close, volume))
    conn.commit()

async def binance_ws_listener():
    # Construct stream path for multiple symbols
    # e.g., wss://fstream.binance.com/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h
    streams = "/".join([f"{sym.lower()}@kline_{INTERVAL}" for sym in SYMBOLS])
    ws_url = f"wss://fstream.binance.com/stream?streams={streams}"
    
    print(f"Connecting to Binance Futures WS: {ws_url}", flush=True)
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                print("Binance Futures WebSocket connected successfully!", flush=True)
                conn = get_db_connection()
                
                async for message in ws:
                    data = json.loads(message)
                    stream = data.get("stream")
                    event_data = data.get("data", {})
                    kline = event_data.get("k", {})
                    
                    if not kline:
                        continue
                    
                    symbol = event_data.get("s")
                    is_closed = kline.get("x") # Is this kline bar closed?
                    
                    # We store closed bars, but we publish current price state to Redis for real-time risk checks
                    t_start = kline.get("t")
                    o = float(kline.get("o"))
                    h = float(kline.get("h"))
                    l = float(kline.get("l"))
                    c = float(kline.get("c"))
                    v = float(kline.get("v"))
                    
                    # Broadcast live ticker to Redis for real-time monitoring / risk execution
                    ticker_payload = {
                        "symbol": symbol,
                        "price": c,
                        "time": t_start,
                        "is_closed": is_closed
                    }
                    r_client.publish(f"ticker:{symbol}", json.dumps(ticker_payload))
                    
                    if is_closed:
                        save_kline_to_db(conn, symbol, t_start, o, h, l, c, v)
                        KLINES_COLLECTED.labels(symbol=symbol).inc()
                        print(f"Saved closed kline for {symbol} at {datetime.fromtimestamp(t_start/1000.0, UTC)}", flush=True)
                        
                        # Publish event to strategy queue
                        r_client.publish("kline_closed", json.dumps({
                            "symbol": symbol,
                            "time": t_start,
                            "interval": INTERVAL,
                            "close": c
                        }))
                        
        except Exception as e:
            print(f"WebSocket Connection Lost/Error: {e}. Reconnecting in 5s...", flush=True)
            WS_ERRORS.inc()
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("Starting Data Collector Microservice...", flush=True)
    # Start Prometheus Exporter
    start_http_server(METRICS_PORT)
    
    # Run WS Loop
    asyncio.run(binance_ws_listener())
