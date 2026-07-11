const express = require('express');
const { Pool } = require('pg');
const redis = require('redis');
const path = require('path');
const bodyParser = require('body-parser');

const app = express();
const PORT = 3001;

app.use(bodyParser.json());

const pool = new Pool({
  host: process.env.DB_HOST || 'timescaledb',
  port: parseInt(process.env.DB_PORT || '5432'),
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'secret_pass_2026',
  database: process.env.DB_NAME || 'swing_bot'
});

const redisClient = redis.createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:${process.env.REDIS_PORT || '6379'}`
});
redisClient.connect().catch(console.error);

app.use(express.static(path.join(__dirname, 'static')));

app.get('/api/dashboard', async (req, res) => {
  try {
    const tradesRes = await pool.query('SELECT * FROM trades ORDER BY exit_time DESC LIMIT 50;');
    const klinesCount = await pool.query('SELECT COUNT(*) FROM klines;');
    const signalsCount = await pool.query('SELECT COUNT(*) FROM signals;');
    const activePositions = await pool.query('SELECT * FROM positions;');
    const equityRes = await pool.query('SELECT total_equity FROM equity_curve ORDER BY time DESC LIMIT 1;');

    const isPaused = await redisClient.get('lock:system_pause');
    
    res.json({
      status: isPaused === 'paused' ? 'PAUSED' : 'RUNNING',
      klines_count: parseInt(klinesCount.rows[0].count),
      signals_count: parseInt(signalsCount.rows[0].count),
      active_positions: activePositions.rows,
      recent_trades: tradesRes.rows,
      equity: equityRes.rows.length > 0 ? parseFloat(equityRes.rows[0].total_equity) : 5000.0
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query error' });
  }
});

app.post('/api/manual-trade', async (req, res) => {
    const { symbol, direction } = req.body;

    // Basit bir örnek manuel trade sinyali
    const decision = {
        symbol: symbol.toUpperCase(),
        direction: direction.toUpperCase(),
        // Risk Manager bu sinyali alıp boyutlandırmalı
        // Bu yüzden sadece sembol ve yön gönderiyoruz.
        // Daha gelişmiş bir sistemde, UI üzerinden SL/TP de alınabilir.
        entry_price: 0, // Risk manager belirleyecek
        size: 0, // Risk manager belirleyecek
        margin: 0, 
        leverage: 0,
        stop_loss: 0,
        take_profit: 0,
        score: 99.0, // Manuel emirler en yüksek önceliğe sahiptir
        is_manual: true
    };
    
    try {
        await redisClient.publish('signal_alert', JSON.stringify([decision]));
        res.json({ status: 'SUCCESS', message: `Manuel ${direction} sinyali ${symbol} için gönderildi.` });
    } catch (err) {
        console.error("Manuel trade sinyal hatası:", err);
        res.status(500).json({ error: 'Redis publish error' });
    }
});

// Sistemi durdurma/başlatma endpoint'i
app.post('/api/toggle-autopilot', async (req, res) => {
    const isPaused = await redisClient.get('lock:system_pause');
    let newState;
    if (isPaused === 'paused') {
        await redisClient.set('lock:system_pause', 'active');
        newState = 'RUNNING';
    } else {
        await redisClient.set('lock:system_pause', 'paused');
        newState = 'PAUSED';
    }
    res.json({ status: 'SUCCESS', new_state: newState });
});


app.listen(PORT, '0.0.0.0', () => {
  console.log(`Control Panel Server running on port ${PORT}`);
});
