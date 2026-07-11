import os
import json
import redis
import psycopg2
from prometheus_client import start_http_server, Counter

# Prometheus Metrics
METRICS_PORT = 8000
RISK_EVALUATIONS = Counter('risk_manager_evaluations_total', 'Total risk sizing evaluations completed')

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret_pass_2026")
DB_NAME = os.getenv("DB_NAME", "swing_bot")

MAX_CONCURRENT_POSITIONS = 3
BASE_RISK_PCT = 0.02 # Risk 2% of equity per trade under $500 bakiye

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

def fetch_current_equity():
    # Simple default capital, later fetched from Binance or Database account tables
    return 1000.0

def fetch_active_positions_count():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM positions;")
            return cur.fetchone()[0]
    except Exception as e:
        print(f"Error checking active positions: {e}", flush=True)
        return 0
    finally:
        conn.close()

def evaluate_risk_and_size(signal):
    """
    Sizing rules:
    - Dynamic Leverage: ATR wide -> Lower Leverage (2-3x), ATR tight -> Higher Leverage (max 7-8x)
    - Margin: Based on risk percentage (BASE_RISK_PCT) and distance to stop loss.
    - Liquidation safety check: verify SL is triggered BEFORE liquidation price.
    """
    symbol = signal["symbol"]
    direction = signal["direction"]
    entry_price = float(signal["close"])
    atr = float(signal["atr"])
    score = float(signal["score"])
    
    # Capital management
    equity = fetch_current_equity()
    active_count = fetch_active_positions_count()
    
    if active_count >= MAX_CONCURRENT_POSITIONS:
        print(f"Risk Manager: Max concurrent positions ({MAX_CONCURRENT_POSITIONS}) reached. Rejecting signal for {symbol}.", flush=True)
        return None
        
    # 1. Stop Loss placement based on ATR
    sl_dist = atr * 1.5
    sl = entry_price - sl_dist if direction == "LONG" else entry_price + sl_dist
    
    # 2. Capital Risk amount ($)
    # Dynamic sizing: Büyüdükçe risk oranını düşür ($500 altı %2, $2000 üstü %1)
    risk_pct = BASE_RISK_PCT
    if equity > 2000.0:
        risk_pct = 0.01
    elif equity > 500.0:
        risk_pct = 0.015
        
    risk_amount = equity * risk_pct
    
    # Size based on stop loss distance (Risk Amount = Position Size * SL distance in price)
    position_size_coin = risk_amount / sl_dist
    position_value = position_size_coin * entry_price
    
    # 3. Dynamic Leverage calculation
    # Leverage = Position Value / Margin
    # High ATR (high volatility) -> low leverage (e.g. 3x), low ATR -> high leverage (e.g. 7x)
    volatility_pct = atr / entry_price
    leverage = int(min(8, max(2, round(0.15 / volatility_pct)))) # Max 8x leverage, min 2x
    
    margin_required = position_value / leverage
    
    # Take Profit (1:3.5 RR ratio)
    tp = entry_price + (sl_dist * 3.5) if direction == "LONG" else entry_price - (sl_dist * 3.5)
    
    # 4. Liquidation Check (Simplified check for isolated margin)
    # Long Liq ~ Entry - (Margin / Size) = Entry * (1 - 1/Leverage)
    if direction == "LONG":
        est_liq_price = entry_price * (1 - 1/leverage)
        if sl <= est_liq_price:
            # Shift SL slightly higher for safety
            sl = est_liq_price * 1.01
    else:
        est_liq_price = entry_price * (1 + 1/leverage)
        if sl >= est_liq_price:
            sl = est_liq_price * 0.99
            
    risk_decision = {
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "size": position_size_coin,
        "margin": margin_required,
        "leverage": leverage,
        "stop_loss": sl,
        "take_profit": tp,
        "score": score
    }
    
    RISK_EVALUATIONS.inc()
    return risk_decision

def r_listener():
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r_client.pubsub()
    pubsub.subscribe("signal_alert")
    
    print("Risk Manager listening for signal_alert events...", flush=True)
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                signals = json.loads(message['data'])
                # signals is a list of candidate dictionaries
                for signal in signals:
                    decision = evaluate_risk_and_size(signal)
                    if decision:
                        print(f"Risk Manager Approved: {decision}", flush=True)
                        # Publish risk-sized orders to executor channel
                        r_client.publish("approved_order", json.dumps(decision))
            except Exception as e:
                print(f"Error processing risk decision: {e}", flush=True)

if __name__ == "__main__":
    print("Starting Risk Manager Microservice...", flush=True)
    start_http_server(METRICS_PORT)
    r_listener()
