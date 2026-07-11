-- TimescaleDB Schema for Binance Perp Swing Trading Bot

-- Enable TimescaleDB extension if not already loaded
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Klines Table (Hypertable)
CREATE TABLE IF NOT EXISTS klines (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(5) NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    PRIMARY KEY (time, symbol, interval)
);

-- Convert klines to hypertable on time column
SELECT create_hypertable('klines', 'time', if_not_exists => TRUE);

-- 2. Trades Table
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    trade_id VARCHAR(50) UNIQUE,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL, -- LONG / SHORT
    entry_price NUMERIC NOT NULL,
    exit_price NUMERIC,
    leverage INT NOT NULL,
    margin NUMERIC NOT NULL,
    pnl NUMERIC,
    pnl_pct NUMERIC,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ,
    reason VARCHAR(100)
);

-- 3. Positions Table (Current open positions)
CREATE TABLE IF NOT EXISTS positions (
    symbol VARCHAR(20) PRIMARY KEY,
    direction VARCHAR(10) NOT NULL,
    entry_price NUMERIC NOT NULL,
    leverage INT NOT NULL,
    margin NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    stop_loss NUMERIC NOT NULL,
    take_profit NUMERIC NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL
);

-- 4. Signals Table
CREATE TABLE IF NOT EXISTS signals (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    score NUMERIC NOT NULL,
    metadata JSONB,
    processed BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('signals', 'time', if_not_exists => TRUE);

-- 5. Equity Curve Table
CREATE TABLE IF NOT EXISTS equity_curve (
    time TIMESTAMPTZ NOT NULL,
    balance NUMERIC NOT NULL,
    unrealized_pnl NUMERIC NOT NULL,
    total_equity NUMERIC NOT NULL,
    PRIMARY KEY (time)
);

SELECT create_hypertable('equity_curve', 'time', if_not_exists => TRUE);
