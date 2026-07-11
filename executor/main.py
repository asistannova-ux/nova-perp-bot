import os
import json
import redis
import time
import requests
import hmac
import hashlib
from datetime import datetime, UTC
from prometheus_client import start_http_server, Counter

# Prometheus Metrics
METRICS_PORT = 8000
ORDERS_SENT = Counter('executor_orders_sent_total', 'Total orders sent to Binance Futures', ['symbol', 'type'])
ORDER_ERRORS = Counter('executor_errors_total', 'Total execution errors')

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Binance Keys from env
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
BINANCE_USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "true").lower() == "true"

# Base URL selection
if BINANCE_USE_TESTNET:
    BASE_URL = "https://testnet.binancefuture.com"
else:
    BASE_URL = "https://fapi.binance.com"

# Idempotency lock expiration (10 seconds)
IDEMPOTENCY_LOCK_TTL = 10

def get_signature(params):
    query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
    return hmac.new(BINANCE_SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def send_signed_request(method, endpoint, params={}):
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        print("Executor Error: API Key or Secret Key is missing in configuration!", flush=True)
        return {"status": "error", "message": "API keys missing"}
        
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = get_signature(params)
    
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "POST":
            r = requests.post(url, data=params, headers=headers, timeout=10)
        elif method.upper() == "DELETE":
            r = requests.delete(url, data=params, headers=headers, timeout=10)
        else:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            
        return r.json()
    except Exception as e:
        print(f"Executor HTTP Error: {e}", flush=True)
        return {"status": "error", "message": str(e)}

def execute_order_flow(decision):
    """
    Executes a complete order flow on Binance Futures:
    1. Check Idempotency lock via Redis
    2. Change leverage and margin type (Isolated)
    3. Place LIMIT order (or market order for immediate entry if desired)
    4. Set up Stop-Loss and Take-Profit as REDUCE_ONLY trigger orders.
    """
    symbol = decision["symbol"]
    direction = decision["direction"]
    entry_price = float(decision["entry_price"])
    qty = float(decision["size"])
    leverage = int(decision["leverage"])
    sl = float(decision["stop_loss"])
    tp = float(decision["take_profit"])
    score = float(decision["score"])
    
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # 1. Idempotency Check using Redis lock to avoid duplicate placement
    lock_key = f"lock:order:{symbol}:{int(time.time() / 10)}"
    if r_client.set(lock_key, "locked", ex=IDEMPOTENCY_LOCK_TTL, nx=True) is not True:
        print(f"Executor Warning: Order for {symbol} is locked by idempotency check. Skipping.", flush=True)
        return
        
    print(f"Executing order flow for {symbol} ({direction}) with {leverage}x leverage...", flush=True)
    
    # 2. Configure Leverage & Margin Type
    # Adjust leverage
    send_signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
    # Set Isolated margin (errors ignored if already set)
    send_signed_request("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"})
    
    # 3. Send Entry Limit Order (using close price as limit price)
    side = "BUY" if direction == "LONG" else "SELL"
    order_params = {
        "symbol": symbol,
        "side": side,
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": f"{qty:.3f}" if qty > 1 else f"{qty:.5f}", # Safe format adjustments
        "price": f"{entry_price:.4f}"
    }
    
    print(f"Sending entry limit order params: {order_params}", flush=True)
    entry_res = send_signed_request("POST", "/fapi/v1/order", order_params)
    
    if "orderId" not in entry_res:
        print(f"Executor Error: Entry order failed: {entry_res}", flush=True)
        ORDER_ERRORS.inc()
        return
        
    print(f"Entry Limit Order Placed successfully! ID: {entry_res.get('orderId')}", flush=True)
    ORDERS_SENT.labels(symbol=symbol, type="ENTRY").inc()
    
    # 4. Set OCO (Stop-Loss and Take-Profit) Reduce-Only orders
    opposite_side = "SELL" if direction == "LONG" else "BUY"
    
    # A. Stop Loss (STOP_MARKET)
    sl_params = {
        "symbol": symbol,
        "side": opposite_side,
        "type": "STOP_MARKET",
        "stopPrice": f"{sl:.4f}",
        "closePosition": "true", # Reduces/Closes entire position on trigger
        "timeInForce": "GTC"
    }
    sl_res = send_signed_request("POST", "/fapi/v1/order", sl_params)
    print(f"Stop Loss order placed status: {sl_res.get('orderId', 'FAILED')}", flush=True)
    
    # B. Take Profit (TAKE_PROFIT_MARKET)
    tp_params = {
        "symbol": symbol,
        "side": opposite_side,
        "type": "TAKE_PROFIT_MARKET",
        "stopPrice": f"{tp:.4f}",
        "closePosition": "true",
        "timeInForce": "GTC"
    }
    tp_res = send_signed_request("POST", "/fapi/v1/order", tp_params)
    print(f"Take Profit order placed status: {tp_res.get('orderId', 'FAILED')}", flush=True)
    
    # Broadcast trade state to Telegram notifier
    r_client.publish("executor_alert", json.dumps({
        "status": "SUCCESS",
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "leverage": leverage,
        "qty": qty,
        "sl": sl,
        "tp": tp,
        "score": score
    }))

def r_listener():
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r_client.pubsub()
    pubsub.subscribe("approved_order")
    
    print("Executor Microservice listening for approved_order events...", flush=True)
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                decision = json.loads(message['data'])
                execute_order_flow(decision)
            except Exception as e:
                print(f"Error in Executor listening loop: {e}", flush=True)

if __name__ == "__main__":
    print("Starting Executor Microservice...", flush=True)
    start_http_server(METRICS_PORT)
    r_listener()
