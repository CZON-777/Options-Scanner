# Options-Scanner
# Unusual Options Flow Scanner — v12

*Code and documentation only. Deployment files, the ticker list, and credentials are kept in a private repository.*

A real-time Python service that reads every options trade from the Massive (Polygon)
WebSocket feed, logs it to Google BigQuery, and sends Telegram alerts when trading
looks unusual: large single trades, steady accumulation on quiet tickers, sudden
volume on contracts that were inactive, and one-sided short-dated flow.

## How it works

```
Massive WebSocket (T.* — all options trades)
        │
        ▼
  scanner.py (asyncio, one connection)
        │
        ├── System 1: tiered alerts ──► main Telegram bot
        │     ETF ($3M single trade) · Large cap ($1M) · Small cap ($250K daily tally)
        │     Silent tier: vol spike vs 3-day history, persistent buyer (4 of 5 days),
        │     $400K → $750K → $1M → $2M accumulation, call+put straddle, pre-market
        │
        ├── System 2: 0–5 DTE flow ──► Expirarybot
        │     Size thresholds by ticker class · pre-market 0DTE · 20:1 ratio in 5 min
        │
        └── Buffered streaming inserts ──► BigQuery
              options_flow.trades / options_flow.trades_0dte (partitioned by trade_date)
```

Scheduled tasks (Eastern Time): 9:25 AM briefing on yesterday's silent-tier
accumulation, 4:05 PM small-cap tally summary, midnight reset of daily state and
reload of the 5-day history.

## Design notes

- **Credentials:** none are stored in the code. API keys, bot tokens, and the
  Google service-account key are read from environment variables at startup.
- **Storage:** trades are buffered in memory and streamed to BigQuery in batches
  every 30 seconds (or sooner at 300 rows). Invalid rows are dropped, failed batches
  are retried, and the retry buffer is capped to protect memory.
- **Time:** all market logic runs on Eastern Time, using each trade's exchange
  timestamp, so daylight-saving changes don't shift the schedule or trade dates.
- **Spot price:** taken from the options chain snapshot and cached for 60 seconds.
- **Known limit:** market holidays are not excluded from the "last N trading days"
  lookback.
