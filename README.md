# OKX ETH Grid Bot

Python grid trading bot for OKX perpetual futures.

## Features

- ETH-USDT-SWAP perpetual futures
- LONG, cross margin
- Dynamic grid with 25 active BUY LIMIT orders
- Grid step: $0.25
- Take Profit: $1.75
- Exchange-attached LIMIT TP orders
- 75 active exchange TP orders
- Virtual TP management with persistent CycleID
- Maximum 500 occupied grid levels
- Public and private WebSocket
- REST reconciliation and recovery
- Actual trading fees and Maker/Taker statistics
- Realized and unrealized PnL
- Daily, monthly and annualized performance statistics
- systemd deployment support

## Configuration

Copy the example configuration files:

    cp config.example.json config.json
    cp .env.example .env

Edit them with your own settings and API credentials.

Install dependencies:

    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt

Run a read-only audit before enabling live trading:

    OKX_BOT04_AUDIT=READ_ONLY .venv/bin/python okx_bot059.py

## Risk warning

This software trades real perpetual futures.

Virtual TP orders exist only in bot state and are not
protected by exchange-side orders while the bot is offline.

Test thoroughly before live trading.
