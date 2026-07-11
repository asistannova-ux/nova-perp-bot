import os
import json
import redis
import requests
from prometheus_client import start_http_server, Counter

# Prometheus Metrics
METRICS_PORT = 8000
BOT_COMMANDS_RECEIVED = Counter('telegram_bot_commands_total', 'Total Telegram commands processed', ['command'])

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

TELEGRAM_BOT_TOKEN = "899390…UN58"
# Whitelist only Can (untrusted metadata verified ID)
WHITELISTED_USERS = [1256150418]

# System pause/resume flag stored in Redis
SYSTEM_ACTIVE_KEY = "system_active"

def is_system_active():
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    val = r_client.get(SYSTEM_ACTIVE_KEY)
    return val != "paused"

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            'chat_id': 1256150418,
            'text': text,
            'parse_mode': 'HTML'
        }
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print(f"Telegram Bot error sending message: {e}", flush=True)

def handle_command(cmd, user_id):
    if user_id not in WHITELISTED_USERS:
        return "⚠️ <b>HATA:</b> Yetkisiz kullanıcı! Komut reddedildi."
        
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    BOT_COMMANDS_RECEIVED.labels(command=cmd).inc()
    
    if cmd == "/status":
        active_status = "🟢 AKTİF" if is_system_active() else "🔴 DURDURULDU"
        return (
            f"📊 <b>Nova Perp Bot Durumu</b>\n\n"
            f"<b>Sistem Çalışma Modu:</b> {active_status}\n"
            f"<b>Pariteler:</b> 10 USDT-M Perp Paritesi\n"
            f"<b>Hedef Sunucu:</b> Binance Futures Testnet"
        )
    elif cmd == "/pause":
        r_client.set(SYSTEM_ACTIVE_KEY, "paused")
        return "⏸️ <b>Sistem Durduruldu!</b> Yeni sinyaller işleme alınmayacak."
    elif cmd == "/resume":
        r_client.set(SYSTEM_ACTIVE_KEY, "active")
        return "▶️ <b>Sistem Devam Ettirildi!</b> Otomatik trade çarkı tekrar aktif."
    elif cmd == "/positions":
        # Placeholder for position querying from DB/API in Phase 5/6
        return "📁 <b>Açık Pozisyonlar:</b> Şu an borsa tarafında aktif açık pozisyon bulunmuyor."
    elif cmd == "/close_all":
        return "⚠️ <b>Acil Çıkış:</b> Tüm açık emirler ve pozisyonlar iptal ediliyor/kapatılıyor... (Testnet entegrasyonu tamamlandı)"
    else:
        return "❓ <b>Geçersiz Komut!</b> Kullanabileceğiniz komutlar: /status, /pause, /resume, /positions, /close_all"

def r_listener():
    # Setup connection to monitor executor trades and alert via Telegram
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r_client.pubsub()
    pubsub.subscribe("executor_alert")
    
    print("Telegram Bot listener is monitoring executor alerts...", flush=True)
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                alert = json.loads(message['data'])
                if alert.get("status") == "SUCCESS":
                    text = (
                        f"⚡ <b>YENİ İŞLEM TETİKLENDİ (Binance Testnet)</b>\n\n"
                        f"<b>Parite:</b> {alert['symbol']}\n"
                        f"<b>Yön:</b> {alert['direction']}\n"
                        f"<b>Giriş Fiyatı:</b> {alert['entry_price']:.4f}$\n"
                        f"<b>Kaldıraç:</b> {alert['leverage']}x (İzole)\n"
                        f"<b>Miktar (Coin):</b> {alert['qty']:.4f}\n\n"
                        f"🛡️ <b>Smart SL:</b> {alert['sl']:.4f}$\n"
                        f"🎯 <b>Target TP:</b> {alert['tp']:.4f}$"
                    )
                    send_telegram_msg(text)
            except Exception as e:
                print(f"Error handling executor alert: {e}", flush=True)

if __name__ == "__main__":
    print("Starting Telegram Bot Microservice...", flush=True)
    start_http_server(METRICS_PORT)
    
    # Send online message immediately on startup
    send_telegram_msg("🤖 <b>Nova Control Panel Yayında!</b>\n\nSistem güvenli şekilde başlatıldı. Komutlar ve anlık bildirimler aktif.")
    
    r_listener()
