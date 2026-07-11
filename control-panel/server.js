const express = require('express');
const { Pool } = require('pg');
const redis = require('redis');
const path = require('path');

const app = express();
const PORT = 3001;

// DB ve Redis yapılandırması
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
    // 1. Toplam bakiye ve işlem istatistikleri
    const tradesRes = await pool.query('SELECT * FROM trades ORDER BY exit_time DESC LIMIT 50;');
    const klinesCount = await pool.query('SELECT COUNT(*) FROM klines;');
    const signalsCount = await pool.query('SELECT COUNT(*) FROM signals;');
    const activePositions = await pool.query('SELECT * FROM positions;');

    // 2. Redis'ten botun aktiflik durumunu çek
    const isPaused = await redisClient.get('lock:system_pause');
    
    res.json({
      status: isPaused === 'paused' ? 'PAUSED' : 'RUNNING',
      klines_count: parseInt(klinesCount.rows[0].count),
      signals_count: parseInt(signalsCount.rows[0].count),
      active_positions: activePositions.rows,
      recent_trades: tradesRes.rows,
      equity: 1054.45 // Simüle edilen güncel kasa
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query error' });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Control Panel Server running on port ${PORT}`);
});
