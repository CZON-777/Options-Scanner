#!/usr/bin/env python3
"""
Unusual Options Flow Scanner — v12
===================================
ONE WebSocket connection. TWO alert systems. TWO BigQuery tables.

What changed from v10
---------------------
* Secrets live in environment variables (no keys in the code or Git history).
* All market times run on America/New_York, so DST changes no longer shift
  the pre-market window, morning briefing, daily summary, or trade dates.
* Trade date/time come from the exchange timestamp on each trade event.
* Daily state (volume, alert flags, tallies) resets at midnight ET, and the
  5-day history reloads every new day instead of only at startup.
* Silent tickers load from silent_tickers.txt (edit the list without
  touching code).
* Spot price comes from the options snapshot (works on options-only plans),
  with a 60-second cache.
* Earnings dates load from earnings.csv (the old loader pulled financial
  filing dates, not earnings dates).
* BigQuery flushes check insert errors, drop only invalid rows, and cap the
  retry buffer so a long outage can't run Railway out of memory.
* Telegram sends no longer block the WebSocket loop.

SYSTEM 1 — Tiered alerts → main bot → `trades` table
  Tier 1 ETFs:      $3M single trade | DTE <= 30
  Tier 2 Large Cap: $1M single trade | DTE <= 30
  Tier 3 Small Cap: $250K daily tally per side
  Tier 4 Silent:    BQ logging + vol spike, persistent buyer,
                    $400K/$750K/$1M/$2M accumulation, straddle,
                    pre-market activity, 9:25 AM ET morning briefing

SYSTEM 2 — 0DTE / short-dated suspicious flow → Expirarybot → `trades_0dte`
  0DTE:    pre-market $50K | watchlist $500K | large cap $2M | ETF $5M
  1-5DTE:  watchlist $2M | large cap $2M | ETF $3M
  Ratio:   20:1 in 5-min window, $200K floor, watchlist only
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
import websockets
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

VERSION = "v12"
ET = ZoneInfo("America/New_York")
BASE_DIR = Path(__file__).resolve().parent

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION — secrets come from environment variables
# ============================================================
# Filled in by load_config() at startup so the module can be imported
# (e.g. by tests) without any secrets present.
MASSIVE_API_KEY = ""
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""
TELEGRAM_0DTE_TOKEN = ""
TELEGRAM_0DTE_CHAT_ID = ""
GOOGLE_CREDS: dict = {}

WS_URL = os.environ.get("WS_URL", "wss://socket.massive.com/options")
REST_BASE = os.environ.get("REST_BASE", "https://api.massive.com")
STARTUP_DELAY = int(os.environ.get("STARTUP_DELAY", "20"))

BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "options-scanner-493920")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "options_flow")
BQ_TABLE_ID = "trades"
BQ_0DTE_TABLE_ID = "trades_0dte"

SILENT_TICKERS_FILE = Path(os.environ.get("SILENT_TICKERS_FILE", BASE_DIR / "silent_tickers.txt"))
EARNINGS_FILE = Path(os.environ.get("EARNINGS_FILE", BASE_DIR / "earnings.csv"))


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def load_config() -> None:
    """Read secrets from the environment. Exits with a clear message if any are missing."""
    global MASSIVE_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    global TELEGRAM_0DTE_TOKEN, TELEGRAM_0DTE_CHAT_ID, GOOGLE_CREDS, BQ_PROJECT_ID

    MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY", "").strip() or _require_env("POLYGON_API_KEY")
    TELEGRAM_BOT_TOKEN = _require_env("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = _require_env("TELEGRAM_CHAT_ID")
    TELEGRAM_0DTE_TOKEN = _require_env("TELEGRAM_0DTE_TOKEN")
    TELEGRAM_0DTE_CHAT_ID = os.environ.get("TELEGRAM_0DTE_CHAT_ID", "").strip() or TELEGRAM_CHAT_ID

    raw = _require_env("GOOGLE_CREDENTIALS_JSON")
    try:
        GOOGLE_CREDS = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")
    BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID") or GOOGLE_CREDS.get("project_id", BQ_PROJECT_ID)


# ============================================================
# SYSTEM 1 TICKERS
# ============================================================
ETF_TICKERS = {"XLE", "XOP", "GLD", "GDX", "GDXJ"}

LARGE_CAP_TICKERS = {"EQNR", "XOM", "CVX", "OXY", "JPM", "BA"}

SMALL_CAP_TICKERS = {
    "CRML", "MP", "UAMY", "ALM", "NB", "TMQ", "LITM",
    "LAC", "PLL", "SLI", "REE", "USA",
    "CCJ", "UEC", "DNN", "UUUU", "NXE", "URG", "LTBR",
    "CEIX", "ARLP", "BTU", "AMR",
    "AVAV", "KTOS", "RCAT", "SAIC",
    "RKT", "UWMC", "PFSI", "SOFI", "HOOD", "UPST",
    "MRNA", "BNTX",
    "GOLD", "NEM", "FCX", "CLF", "X",
    "FRO", "DHT", "INSW", "TNK", "STNG",
    "VTLE", "SM", "CIVI",
    "MOS", "NTR", "CF", "IPI", "SMG",
    "LYB", "DOW", "ALB", "WLK", "OLN",
    "USO", "BNO", "WEAT", "CORN", "SOYB",
    "DBA", "DBC", "CPER", "SLV",
}


def load_silent_tickers(path: Path) -> set:
    """One ticker per line (commas/spaces also fine). Lines starting with # are ignored."""
    if not path.exists():
        log.warning(f"Silent ticker file not found: {path} — silent tier is empty")
        return set()
    tickers = set()
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0]
        for tok in re.split(r"[,\s]+", line):
            tok = tok.strip().strip('"').strip("'").upper()
            if tok:
                tickers.add(tok)
    return tickers


ALL_ALERT_TICKERS = ETF_TICKERS | LARGE_CAP_TICKERS | SMALL_CAP_TICKERS
ALL_SILENT_TICKERS: set = set()   # filled at startup
ALL_TICKERS: set = set(ALL_ALERT_TICKERS)

# ============================================================
# SYSTEM 2 TICKERS — 0DTE SUSPICIOUS ACTIVITY
# ============================================================
BROAD_ETF_0DTE = {
    "SPY", "QQQ", "IWM", "RSP", "DIA", "VTI", "VOO",
    "SQQQ", "SPXS", "SPXL", "TQQQ", "SDOW", "UDOW",
    "UVXY", "VIXY", "SVIX",
    "XLF", "XLE", "XLK", "XLI", "XLV", "XLU", "XLB", "XLP", "XLY",
    "EWU", "EWZ", "EWJ", "EWG", "EWA", "EEM", "EFA", "FXI",
    "TLT", "HYG", "LQD", "SHY", "IEF",
    "DBC", "PDBC",
}

LARGE_CAP_0DTE = {
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "TSLA",
    "JPM", "BAC", "GS", "MS", "WFC", "C",
    "XOM", "CVX", "COP", "SLB",
    "BA", "CAT", "UNH", "JNJ", "PFE", "WMT", "HD",
    "AMD", "INTC", "QCOM", "AVGO",
    "NFLX", "UBER", "ABNB", "ARM", "SNPS", "SNDK", "RMBS", "SMCI",
    "NVO", "SAP", "ASX", "ALAB", "TTMI", "AAOI", "XNDU",
}

WATCHLIST_0DTE = {
    "EQNR", "USO", "BNO", "XOP",
    "GLD", "GDX", "GDXJ", "SLV", "FCX", "CLF", "GOLD", "NEM", "CPER", "ALB",
    "CCJ", "UEC", "DNN", "UUUU", "NXE", "URG", "LTBR",
    "CRML", "MP", "UAMY", "ALM", "NB", "TMQ", "LAC",
    "WEAT", "CORN", "SOYB", "DBA", "MOS", "NTR", "CF",
    "MSTR", "COIN", "HOOD",
    "AVAV", "KTOS", "RCAT", "SAIC", "PLTR",
    "FRO", "DHT", "INSW", "TNK", "STNG",
    "SOFI", "RKT", "UWMC", "PFSI", "UPST",
    "MRNA", "BNTX",
    "CEIX", "ARLP", "BTU", "AMR",
    "LYB", "DOW", "WLK", "OLN",
    "AR", "ENVA", "MXL", "OGN", "SXT", "APPF", "POET", "CHE", "AMBA", "SYNA", "KALU",
}

ALL_0DTE_TICKERS = BROAD_ETF_0DTE | LARGE_CAP_0DTE | WATCHLIST_0DTE

# ============================================================
# THRESHOLDS
# ============================================================
ETF_MIN_PREMIUM = 3_000_000
LARGE_CAP_MIN_PREMIUM = 1_000_000
SMALL_CAP_DAILY_THRESHOLD = 250_000
ETF_MAX_DTE = 30
LARGE_CAP_MAX_DTE = 30
SMALL_CAP_MAX_DTE = 30

SILENT_VOL_SPIKE_MIN_CONTRACTS = 200
SILENT_VOL_SPIKE_MULT = 10.0
SILENT_ACCUM_TIER1 = 400_000
SILENT_ACCUM_TIER2 = 750_000
SILENT_ACCUM_TIER3 = 1_000_000
SILENT_ACCUM_TIER4 = 2_000_000
SILENT_MIN_PRINTS = 5
SILENT_DTE_MIN = 7
SILENT_DTE_MAX = 60
SILENT_PREMARKET_MIN = 25_000
SILENT_EXTREME_OTM_CALL_MULT = 1.50
SILENT_EXTREME_OTM_PUT_MULT = 0.50
SILENT_PERSISTENT_DAYS = 4
SILENT_STRADDLE_MIN_EACH_SIDE = 150_000

BROAD_ETF_0DTE_MIN = 5_000_000
LARGE_CAP_0DTE_MIN = 2_000_000
WATCHLIST_0DTE_MIN = 500_000
PREMARKET_0DTE_MIN = 50_000
BROAD_ETF_15DTE_MIN = 3_000_000
LARGE_CAP_15DTE_MIN = 2_000_000
WATCHLIST_15DTE_MIN = 2_000_000

RATIO_THRESHOLD = 20
RATIO_PREMIUM_FLOOR = 200_000
RATIO_WINDOW_SECS = 300

# ============================================================
# INFRASTRUCTURE
# ============================================================
RECONNECT_DELAY_SHORT = 20
RECONNECT_DELAY_LONG = 30
BATCH_FLUSH_INTERVAL = 30
BATCH_SIZE_LIMIT = 300
MAX_BUFFER_ROWS = 50_000          # cap on rows held for retry during a BQ outage
SPOT_CACHE_SECS = 60

MARKET_OPEN = dtime(9, 30)
BRIEFING_START = dtime(9, 25)
BRIEFING_END = dtime(9, 30)
SUMMARY_TIME = dtime(16, 5)

# ============================================================
# TIME HELPERS — everything market-related runs on Eastern Time
# ============================================================
def now_et() -> datetime:
    return datetime.now(ET)


def today_et() -> date:
    return now_et().date()


def event_time_et(event: dict) -> datetime:
    """Exchange timestamp (ms since epoch, field 't') converted to ET; falls back to now."""
    ts = event.get("t")
    if isinstance(ts, (int, float)) and ts > 0:
        # Massive sends ms; guard against ns just in case
        secs = ts / 1_000_000_000 if ts > 1e14 else ts / 1000
        return datetime.fromtimestamp(secs, tz=ET)
    return now_et()


def is_premarket(at: datetime = None) -> bool:
    """Before the 9:30 AM ET open on a weekday."""
    at = at or now_et()
    return at.weekday() < 5 and at.time() < MARKET_OPEN


def get_last_n_trading_days(n: int, ref: date = None) -> list:
    """Last N weekdays before `ref` (default: today ET), newest first. Holidays are not excluded."""
    ref = ref or today_et()
    days, check = [], ref - timedelta(days=1)
    while len(days) < n:
        if check.weekday() < 5:
            days.append(check)
        check -= timedelta(days=1)
    return days


def fmt_clock(at: datetime = None) -> str:
    return (at or now_et()).strftime("%I:%M:%S %p ET")


# ============================================================
# STATE
# ============================================================
current_day: date = None          # ET trading day the daily state belongs to

daily_tally: dict = {}            # small cap: ticker → {calls, puts, alerted_calls, alerted_puts}
silent_contract_tally: dict = {}  # contract → {premium, contracts, prints, alert_level}
contract_vol_today: dict = defaultdict(int)
vol_spike_alerted: set = set()
straddle_tally: dict = {}         # ticker → {calls, puts, alerted}
persistent_alerted: set = set()
alerted_today: set = set()
ratio_window: dict = defaultdict(list)

# Loaded from BigQuery each new day
contract_vol_history: dict = {}   # (ticker, type, strike, expiry, date_str) → contracts
persistent_days: dict = {}        # (ticker, type, strike, expiry) → active days in last 5

earnings_calendar: dict = {}      # ticker → "YYYY-MM-DD"
morning_briefing_sent = False
daily_summary_sent = False

_spot_cache: dict = {}            # ticker → (price, monotonic time)


def reset_daily_state(new_day: date) -> None:
    """Clear every per-day structure. Called at the first trade/tick of a new ET day."""
    global current_day, morning_briefing_sent, daily_summary_sent
    current_day = new_day
    daily_tally.clear()
    silent_contract_tally.clear()
    contract_vol_today.clear()
    vol_spike_alerted.clear()
    straddle_tally.clear()
    persistent_alerted.clear()
    alerted_today.clear()
    ratio_window.clear()
    morning_briefing_sent = False
    daily_summary_sent = False


async def rollover_if_new_day() -> None:
    """Reset daily state and reload history when the ET date changes."""
    today = today_et()
    if current_day == today:
        return
    first_run = current_day is None
    reset_daily_state(today)
    if not first_run:
        log.info(f"New trading day {today} — daily state reset, reloading history")
        asyncio.create_task(load_historical_vol())
        load_earnings_calendar()


# ============================================================
# BIGQUERY SCHEMAS
# ============================================================
# trade_date / trade_time are Eastern Time from the exchange timestamp (v12+).
# Rows written by v10 and earlier used UTC server time.
BQ_SCHEMA = [
    bigquery.SchemaField("trade_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("trade_time", "TIME", mode="REQUIRED"),
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("option_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("strike", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("expiry", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("dte", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("size", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("premium", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("tier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
]

BQ_0DTE_SCHEMA = BQ_SCHEMA[:10] + [
    bigquery.SchemaField("trigger", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("tier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("section", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_premarket", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
]


# ============================================================
# BIGQUERY SINK — buffered streaming inserts with error handling
# ============================================================
class BQSink:
    def __init__(self, name: str, table_id: str, schema: list):
        self.name = name
        self.table_id = table_id
        self.schema = schema
        self.client = None
        self.table_ref = None
        self.rows: list = []
        self._flushing = False

    @property
    def full_ref(self) -> str:
        return f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{self.table_id}"

    def init(self, client: "bigquery.Client") -> None:
        try:
            self.client = client
            dataset_ref = bigquery.DatasetReference(BQ_PROJECT_ID, BQ_DATASET_ID)
            try:
                client.get_dataset(dataset_ref)
            except Exception:
                client.create_dataset(bigquery.Dataset(dataset_ref))
                log.info(f"BQ dataset {BQ_DATASET_ID} created ✅")
            self.table_ref = bigquery.TableReference(dataset_ref, self.table_id)
            try:
                client.get_table(self.table_ref)
                log.info(f"BQ [{self.name}] table {self.full_ref} ready ✅")
            except Exception:
                table = bigquery.Table(self.table_ref, schema=self.schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field="trade_date"
                )
                client.create_table(table)
                log.info(f"BQ [{self.name}] table {self.full_ref} created ✅")
        except Exception as e:
            self.client = None
            log.error(f"BQ [{self.name}] init error: {e}")

    def add(self, row: dict) -> None:
        self.rows.append(row)

    def _requeue(self, rows: list) -> None:
        self.rows[:0] = rows
        overflow = len(self.rows) - MAX_BUFFER_ROWS
        if overflow > 0:
            del self.rows[:overflow]
            log.warning(f"BQ [{self.name}] buffer full — dropped {overflow:,} oldest rows")

    async def flush(self) -> None:
        if not self.rows or self.client is None or self._flushing:
            return
        self._flushing = True
        batch = self.rows[:]
        self.rows.clear()
        try:
            loop = asyncio.get_running_loop()
            errors = await loop.run_in_executor(
                None, lambda: self.client.insert_rows_json(self.table_ref, batch)
            )
            if not errors:
                log.info(f"BQ [{self.name}] flushed {len(batch):,} rows ✅")
                return
            # Rows with a real error are dropped (retrying would loop forever).
            # Rows marked "stopped" were valid but not written — retry those.
            bad = {
                e["index"] for e in errors
                if any(err.get("reason") != "stopped" for err in e.get("errors", []))
            }
            retry = [r for i, r in enumerate(batch) if i not in bad]
            self._requeue(retry)
            log.error(
                f"BQ [{self.name}] insert errors: {len(bad)} bad rows dropped, "
                f"{len(retry)} requeued. First error: {errors[0]}"
            )
        except Exception as e:
            self._requeue(batch)
            log.error(f"BQ [{self.name}] flush error: {e} — {len(batch):,} rows requeued")
        finally:
            self._flushing = False

    async def maybe_flush(self) -> None:
        if len(self.rows) >= BATCH_SIZE_LIMIT:
            await self.flush()

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            await self.flush()


main_sink = BQSink("MAIN", BQ_TABLE_ID, BQ_SCHEMA)
dte_sink = BQSink("0DTE", BQ_0DTE_TABLE_ID, BQ_0DTE_SCHEMA)


def _base_row(trade_at, ticker, option_type, strike, expiry, dte, price, size, premium) -> dict:
    return {
        "trade_date": trade_at.strftime("%Y-%m-%d"),
        "trade_time": trade_at.strftime("%H:%M:%S"),
        "ticker": ticker,
        "option_type": option_type,
        "strike": float(strike),
        "expiry": str(expiry),
        "dte": int(dte),
        "price": float(price),
        "size": int(size),
        "premium": round(float(premium), 2),
    }


def queue_main(trade_at, ticker, option_type, strike, expiry, dte, price, size, premium, tier):
    row = _base_row(trade_at, ticker, option_type, strike, expiry, dte, price, size, premium)
    row["tier"] = tier
    row["inserted_at"] = datetime.now(timezone.utc).isoformat()
    main_sink.add(row)


def queue_0dte(trade_at, ticker, option_type, strike, expiry, dte, price, size, premium,
               trigger, tier, section, premarket):
    row = _base_row(trade_at, ticker, option_type, strike, expiry, dte, price, size, premium)
    row.update({
        "trigger": trigger,
        "tier": tier,
        "section": section,
        "is_premarket": bool(premarket),
        "inserted_at": datetime.now(timezone.utc).isoformat(),
    })
    dte_sink.add(row)


# ============================================================
# TELEGRAM — two bots, sent off the event loop
# ============================================================
def _post_telegram(token: str, chat_id: str, message: str, label: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        if r.status_code == 200:
            log.info(f"Telegram [{label}] sent ✅")
            return True
        log.error(f"Telegram [{label}] failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.error(f"Telegram [{label}] error: {e}")
    return False


async def send_main(message: str) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _post_telegram, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, message, "MAIN"
    )


async def send_0dte(message: str) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _post_telegram, TELEGRAM_0DTE_TOKEN, TELEGRAM_0DTE_CHAT_ID, message, "0DTE"
    )


# ============================================================
# SHARED HELPERS
# ============================================================
OPTION_SYMBOL_RE = re.compile(r"^([A-Z][A-Z0-9.]*?)(\d{6})([CP])(\d{8})$")


def parse_option_symbol(sym: str) -> dict:
    """O:SPY260116C00565000 → {ticker, expiry, option_type, strike}"""
    try:
        m = OPTION_SYMBOL_RE.match(sym.replace("O:", "", 1))
        if not m:
            return {}
        d = m.group(2)
        return {
            "ticker": m.group(1),
            "expiry": date(2000 + int(d[:2]), int(d[2:4]), int(d[4:6])),
            "option_type": "CALL" if m.group(3) == "C" else "PUT",
            "strike": int(m.group(4)) / 1000.0,
        }
    except Exception:
        return {}


def calculate_premium(price: float, size: int) -> float:
    return price * size * 100


def contract_key(ticker, option_type, strike, expiry) -> tuple:
    return (ticker, option_type, round(float(strike), 2), str(expiry))


def is_otm(option_type: str, strike: float, spot: float) -> tuple:
    if spot <= 0:
        return False, 0.0
    if option_type == "CALL":
        return strike > spot, (strike - spot) / spot * 100
    return strike < spot, (spot - strike) / spot * 100


def otm_label(option_type: str, strike: float, spot: float) -> str:
    if spot <= 0:
        return "Spot unavailable"
    otm, pct = is_otm(option_type, strike, spot)
    return f"{'OTM' if otm else 'ITM'} ({pct:+.1f}%)"


def is_extreme_otm(option_type: str, strike: float, spot: float) -> bool:
    if spot <= 0:
        return False
    if option_type == "CALL":
        return strike >= spot * SILENT_EXTREME_OTM_CALL_MULT
    return strike <= spot * SILENT_EXTREME_OTM_PUT_MULT


def format_dte(dte: int) -> str:
    if dte == 0:
        return "0DTE — EXPIRES TODAY"
    if dte == 1:
        return "1DTE — EXPIRES TOMORROW"
    return f"{dte}DTE"


def format_premium(p: float) -> str:
    return f"${p / 1_000_000:.2f}M" if p >= 1_000_000 else f"${p / 1_000:.0f}K"


def fmt_spot(spot: float) -> str:
    return f"${spot:.2f}" if spot > 0 else "n/a"


def get_earnings_tag(ticker: str) -> str:
    earn = earnings_calendar.get(ticker)
    if not earn:
        return ""
    try:
        days = (date.fromisoformat(earn) - today_et()).days
        if 0 <= days <= 60:
            return f"\n📅 Earnings: {earn} ({days}d away)"
    except ValueError:
        pass
    return ""


# ============================================================
# SPOT PRICE — options snapshot first (works on options-only plans)
# ============================================================
def _fetch_spot_sync(ticker: str) -> float:
    params = {"apiKey": MASSIVE_API_KEY}
    # 1) Options chain snapshot carries the underlying's price
    try:
        r = requests.get(f"{REST_BASE}/v3/snapshot/options/{ticker}",
                         params={**params, "limit": 1}, timeout=5)
        results = r.json().get("results") or []
        if results:
            price = float((results[0].get("underlying_asset") or {}).get("price") or 0)
            if price > 0:
                return price
    except Exception as e:
        log.debug(f"spot snapshot {ticker}: {e}")
    # 2) Stock last trade (needs a stocks data entitlement)
    try:
        r = requests.get(f"{REST_BASE}/v2/last/trade/{ticker}", params=params, timeout=5)
        price = float((r.json().get("results") or {}).get("p") or 0)
        if price > 0:
            return price
    except Exception as e:
        log.debug(f"spot last trade {ticker}: {e}")
    log.warning(f"Spot price unavailable for {ticker}")
    return 0.0


async def get_underlying_price(ticker: str) -> float:
    cached = _spot_cache.get(ticker)
    if cached and time.monotonic() - cached[1] < SPOT_CACHE_SECS:
        return cached[0]
    loop = asyncio.get_running_loop()
    price = await loop.run_in_executor(None, _fetch_spot_sync, ticker)
    if price > 0:
        _spot_cache[ticker] = (price, time.monotonic())
    return price


# ============================================================
# STARTUP / DAILY DATA LOADERS
# ============================================================
async def load_historical_vol() -> None:
    """
    Pull the last 5 trading days of silent-tier contract volume from BigQuery.
    Builds new dicts and swaps them in, so lookups never see a half-loaded state.
    """
    global contract_vol_history, persistent_days
    if main_sink.client is None:
        log.warning("load_historical_vol: BQ client not ready, skipping")
        return

    days = get_last_n_trading_days(5)
    date_csv = ", ".join(f"'{d.isoformat()}'" for d in days)
    query = f"""
        SELECT ticker, option_type,
               CAST(strike AS FLOAT64) AS strike,
               CAST(expiry AS STRING) AS expiry,
               CAST(trade_date AS STRING) AS trade_date,
               SUM(size) AS total_size
        FROM `{main_sink.full_ref}`
        WHERE trade_date IN ({date_csv})
          AND tier = 'SILENT'
          AND dte BETWEEN {SILENT_DTE_MIN} AND {SILENT_DTE_MAX}
        GROUP BY ticker, option_type, strike, expiry, trade_date
    """
    try:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(
            None, lambda: list(main_sink.client.query(query).result())
        )
        history, persist = {}, defaultdict(int)
        for row in rows:
            ck = contract_key(row.ticker, row.option_type, row.strike, row.expiry)
            history[ck + (row.trade_date,)] = int(row.total_size)
            persist[ck] += 1
        contract_vol_history, persistent_days = history, dict(persist)
        log.info(f"Historical vol: {len(rows):,} rows over {len(days)} trading days ✅")
    except Exception as e:
        log.error(f"load_historical_vol error: {e}")


def load_earnings_calendar() -> None:
    """
    Read earnings dates from earnings.csv (columns: ticker,date — date as YYYY-MM-DD).
    Missing file → alerts simply omit the earnings line.
    """
    earnings_calendar.clear()
    if not EARNINGS_FILE.exists():
        log.info(f"No earnings file at {EARNINGS_FILE} — alerts will omit earnings dates")
        return
    try:
        with EARNINGS_FILE.open(newline="") as f:
            for row in csv.DictReader(f):
                t = (row.get("ticker") or "").strip().upper()
                d = (row.get("date") or "").strip()
                if t and d:
                    date.fromisoformat(d)   # validate
                    earnings_calendar[t] = d
        log.info(f"Earnings calendar: {len(earnings_calendar)} entries ✅")
    except Exception as e:
        log.warning(f"Earnings file error: {e} — alerts will omit earnings dates")


def get_3day_avg_vol(ck: tuple) -> float:
    days = get_last_n_trading_days(3)
    return sum(contract_vol_history.get(ck + (d.isoformat(),), 0) for d in days) / 3.0


# ============================================================
# SYSTEM 1 — SMALL CAP DAILY TALLY
# ============================================================
async def handle_small_cap(t, ticker, option_type, strike, expiry, dte, price, size, premium):
    try:
        queue_main(t, ticker, option_type, strike, expiry, dte, price, size, premium, "SMALL CAP")
        tally = daily_tally.setdefault(ticker, {
            "calls": 0.0, "puts": 0.0, "alerted_calls": False, "alerted_puts": False,
        })
        side, flag = ("calls", "alerted_calls") if option_type == "CALL" else ("puts", "alerted_puts")
        tally[side] += premium
        total = tally[side]

        if total >= SMALL_CAP_DAILY_THRESHOLD and not tally[flag]:
            tally[flag] = True
            spot = await get_underlying_price(ticker)
            direction = "BULLISH" if option_type == "CALL" else "BEARISH"
            msg = (
                f"⚡ SMALL CAP ACCUMULATION ALERT\n"
                f"{'=' * 32}\n"
                f"Ticker:    {ticker}\n"
                f"Direction: {direction}\n"
                f"Type:      {option_type}\n"
                f"Daily Premium: {format_premium(total)}\n"
                f"Threshold: $250K crossed ✅\n"
                f"Last Strike: ${strike:.2f}\n"
                f"Last Expiry: {expiry} ({format_dte(dte)})\n"
                f"Spot Price: {fmt_spot(spot)}\n"
                f"Status:    {otm_label(option_type, strike, spot)}{get_earnings_tag(ticker)}\n"
                f"{'=' * 32}\n"
                f"Total spent today: ${total:,.0f}\n"
                f"{'=' * 32}\n"
                f"TO TRADE: Buy {ticker} stock or {'calls' if option_type == 'CALL' else 'puts'}\n"
                f"Use GTC+Extended Hours limit order\n"
                f"{'=' * 32}\n"
                f"⏰ {fmt_clock()}"
            )
            await send_main(msg)
    except Exception as e:
        log.error(f"Small cap error: {e}")


# ============================================================
# SYSTEM 1 — ETF / LARGE CAP SINGLE TRADES
# ============================================================
async def handle_large_cap(t, ticker, option_type, strike, expiry, dte, price, size, premium):
    try:
        is_etf = ticker in ETF_TICKERS
        tier = "ETF" if is_etf else "LARGE CAP"
        queue_main(t, ticker, option_type, strike, expiry, dte, price, size, premium, tier)

        min_prem = ETF_MIN_PREMIUM if is_etf else LARGE_CAP_MIN_PREMIUM
        max_dte = ETF_MAX_DTE if is_etf else LARGE_CAP_MAX_DTE
        if dte > max_dte or premium < min_prem:
            return

        spot = await get_underlying_price(ticker)
        log.info(f"ALERT [{tier}]: {ticker} {option_type} ${strike} {format_premium(premium)}")
        msg = (
            f"{'🚨' if is_etf else '🐋'} UNUSUAL OPTIONS — {tier}\n"
            f"{'=' * 32}\n"
            f"Ticker:    {ticker}\n"
            f"Type:      {option_type}\n"
            f"Strike:    ${strike:.2f}\n"
            f"Expiry:    {expiry} ({format_dte(dte)})\n"
            f"Premium:   {format_premium(premium)}\n"
            f"Contracts: {size:,}\n"
            f"Per contract: ${price:.2f}\n"
            f"Spot:      {fmt_spot(spot)}\n"
            f"Status:    {otm_label(option_type, strike, spot)}{get_earnings_tag(ticker)}\n"
            f"{'=' * 32}\n"
            f"TO MIMIC:\n"
            f"Buy {ticker} ${strike} {option_type}\n"
            f"Expiry: {expiry}\n"
            f"{'=' * 32}\n"
            f"⏰ {fmt_clock()}"
        )
        await send_main(msg)
    except Exception as e:
        log.error(f"Large cap error: {e}")


# ============================================================
# SYSTEM 1 — SILENT TIER INTELLIGENCE
# ============================================================
ACCUM_TIERS = [
    (1, SILENT_ACCUM_TIER1, "⚡", "FIRST ALERT"),
    (2, SILENT_ACCUM_TIER2, "🔥", "ESCALATION — still buying"),
    (3, SILENT_ACCUM_TIER3, "🚨", "MAJOR — $1M+ on silent name"),
    (4, SILENT_ACCUM_TIER4, "🚨🚨", "INSTITUTIONAL — $2M+ on silent name"),
]


async def handle_silent(t, ticker, option_type, strike, expiry, dte, price, size, premium):
    try:
        queue_main(t, ticker, option_type, strike, expiry, dte, price, size, premium, "SILENT")
        await main_sink.maybe_flush()

        if not (SILENT_DTE_MIN <= dte <= SILENT_DTE_MAX):
            if is_premarket(t) and premium >= SILENT_PREMARKET_MIN:
                spot = await get_underlying_price(ticker)
                await send_main(
                    f"🌅 PRE-MARKET SILENT TICKER\n"
                    f"{'=' * 34}\n"
                    f"Ticker:   {ticker}\n"
                    f"Type:     {option_type}\n"
                    f"Strike:   ${strike:.2f}\n"
                    f"Expiry:   {expiry} ({format_dte(dte)})\n"
                    f"Premium:  {format_premium(premium)}\n"
                    f"Spot:     {fmt_spot(spot)}{get_earnings_tag(ticker)}\n"
                    f"{'=' * 34}\n"
                    f"⏰ {fmt_clock()}"
                )
            return

        ck = contract_key(ticker, option_type, strike, expiry)
        direction = "BULLISH" if option_type == "CALL" else "BEARISH"

        # ── 1. Vol spike / brand-new contract ──────────────────────
        contract_vol_today[ck] += size
        vol_today = contract_vol_today[ck]
        avg_3day = get_3day_avg_vol(ck)
        is_new = avg_3day == 0 and vol_today >= SILENT_VOL_SPIKE_MIN_CONTRACTS
        is_spike = (avg_3day > 0 and vol_today >= SILENT_VOL_SPIKE_MIN_CONTRACTS
                    and vol_today >= avg_3day * SILENT_VOL_SPIKE_MULT)

        if (is_new or is_spike) and ck not in vol_spike_alerted:
            vol_spike_alerted.add(ck)
            spot = await get_underlying_price(ticker)
            extreme = "\n🔴 EXTREME OTM" if is_extreme_otm(option_type, strike, spot) else ""
            spike_str = ("NEW CONTRACT — zero activity in last 3 days" if is_new
                         else f"{vol_today / avg_3day:.0f}x the 3-day average volume")
            await send_main(
                f"⚡ VOL SPIKE — SILENT TICKER\n"
                f"{'=' * 34}\n"
                f"Ticker:    {ticker}\n"
                f"Type:      {option_type} — {direction}\n"
                f"Strike:    ${strike:.2f}\n"
                f"Expiry:    {expiry} ({format_dte(dte)})\n"
                f"Spot:      {fmt_spot(spot)}\n"
                f"Status:    {otm_label(option_type, strike, spot)}{extreme}{get_earnings_tag(ticker)}\n"
                f"{'=' * 34}\n"
                f"Signal:    {spike_str}\n"
                f"Contracts today: {vol_today:,}\n"
                f"3-day avg:       {avg_3day:.0f}\n"
                f"{'=' * 34}\n"
                f"⏰ {fmt_clock()}"
            )
            log.info(f"VOL SPIKE: {ticker} {option_type} ${strike} {expiry} — {spike_str}")

        # ── 2. Persistent buyer (active 4+ of last 5 days, and again today) ──
        active_days = persistent_days.get(ck, 0)
        if active_days >= SILENT_PERSISTENT_DAYS and ck not in persistent_alerted:
            persistent_alerted.add(ck)
            spot = await get_underlying_price(ticker)
            await send_main(
                f"🐋 PERSISTENT BUYER DETECTED\n"
                f"{'=' * 34}\n"
                f"Ticker:    {ticker}\n"
                f"Type:      {option_type} — {direction}\n"
                f"Strike:    ${strike:.2f}\n"
                f"Expiry:    {expiry} ({format_dte(dte)})\n"
                f"Spot:      {fmt_spot(spot)}\n"
                f"Status:    {otm_label(option_type, strike, spot)}{get_earnings_tag(ticker)}\n"
                f"{'=' * 34}\n"
                f"Active {active_days} of last 5 trading days, and again today\n"
                f"{'=' * 34}\n"
                f"⏰ {fmt_clock()}"
            )
            log.info(f"PERSISTENT BUYER: {ticker} {option_type} ${strike} {expiry} — {active_days} days")

        # ── 3. Accumulation, escalating one level at a time ────────
        bucket = silent_contract_tally.setdefault(
            ck, {"premium": 0.0, "contracts": 0, "prints": 0, "alert_level": 0}
        )
        bucket["premium"] += premium
        bucket["contracts"] += size
        bucket["prints"] += 1

        for level, threshold, emoji, label in ACCUM_TIERS:
            if (bucket["premium"] >= threshold and bucket["alert_level"] < level
                    and bucket["prints"] >= SILENT_MIN_PRINTS):
                bucket["alert_level"] = level
                spot = await get_underlying_price(ticker)
                extreme = "\n🔴 EXTREME OTM" if is_extreme_otm(option_type, strike, spot) else ""
                await send_main(
                    f"{emoji} SILENT ACCUMULATION — {label}\n"
                    f"{'=' * 34}\n"
                    f"Ticker:    {ticker}\n"
                    f"Type:      {option_type} — {direction}\n"
                    f"Strike:    ${strike:.2f}\n"
                    f"Expiry:    {expiry} ({format_dte(dte)})\n"
                    f"Spot:      {fmt_spot(spot)}\n"
                    f"Status:    {otm_label(option_type, strike, spot)}{extreme}{get_earnings_tag(ticker)}\n"
                    f"{'=' * 34}\n"
                    f"Daily Premium: {format_premium(bucket['premium'])}\n"
                    f"Contracts:     {bucket['contracts']:,}\n"
                    f"Prints:        {bucket['prints']:,}\n"
                    f"Alert Level:   {level}/4\n"
                    f"{'=' * 34}\n"
                    f"⏰ {fmt_clock()}"
                )
                log.info(f"SILENT ACCUM L{level}: {ticker} {option_type} ${strike} {expiry} "
                         f"— {format_premium(bucket['premium'])}")
                break

        # ── 4. Straddle: both sides accumulating on the same ticker ──
        st = straddle_tally.setdefault(ticker, {"calls": 0.0, "puts": 0.0, "alerted": False})
        st["calls" if option_type == "CALL" else "puts"] += premium
        if (not st["alerted"] and st["calls"] >= SILENT_STRADDLE_MIN_EACH_SIDE
                and st["puts"] >= SILENT_STRADDLE_MIN_EACH_SIDE):
            st["alerted"] = True
            spot = await get_underlying_price(ticker)
            await send_main(
                f"⚠️ STRADDLE PATTERN — {ticker}\n"
                f"{'=' * 34}\n"
                f"Spot:        {fmt_spot(spot)}{get_earnings_tag(ticker)}\n"
                f"Calls today: {format_premium(st['calls'])}\n"
                f"Puts today:  {format_premium(st['puts'])}\n"
                f"{'=' * 34}\n"
                f"Both sides accumulating — binary event suspected\n"
                f"⏰ {fmt_clock()}"
            )
            log.info(f"STRADDLE: {ticker} calls={format_premium(st['calls'])} "
                     f"puts={format_premium(st['puts'])}")
    except Exception as e:
        log.error(f"Silent tier error: {e}")


# ============================================================
# SYSTEM 2 — 0DTE / SHORT-DATED SUSPICIOUS FLOW
# ============================================================
def update_ratio_window(ticker: str, option_type: str, premium: float) -> None:
    now_ts = time.time()
    entries = ratio_window[ticker]
    entries.append((now_ts, option_type, premium))
    cutoff = now_ts - RATIO_WINDOW_SECS
    while entries and entries[0][0] < cutoff:
        entries.pop(0)


def check_ratio(ticker: str) -> tuple:
    entries = ratio_window.get(ticker, [])
    puts = sum(e[2] for e in entries if e[1] == "PUT")
    calls = sum(e[2] for e in entries if e[1] == "CALL")
    total = puts + calls
    if total < RATIO_PREMIUM_FLOOR:
        return False, "", 0.0, total
    if calls > 0 and puts / calls >= RATIO_THRESHOLD:
        return True, "PUT", puts / calls, total
    if puts > 0 and calls / puts >= RATIO_THRESHOLD:
        return True, "CALL", calls / puts, total
    return False, "", 0.0, total


def already_alerted(key: tuple) -> bool:
    if key in alerted_today:
        return True
    alerted_today.add(key)
    return False


def get_0dte_ticker_class(ticker: str) -> str:
    if ticker in BROAD_ETF_0DTE:
        return "BROAD_ETF"
    if ticker in LARGE_CAP_0DTE:
        return "LARGE_CAP"
    return "WATCHLIST"


def get_0dte_threshold(ticker: str, dte: int) -> float:
    cls = get_0dte_ticker_class(ticker)
    if dte == 0:
        return {"BROAD_ETF": BROAD_ETF_0DTE_MIN, "LARGE_CAP": LARGE_CAP_0DTE_MIN,
                "WATCHLIST": WATCHLIST_0DTE_MIN}[cls]
    return {"BROAD_ETF": BROAD_ETF_15DTE_MIN, "LARGE_CAP": LARGE_CAP_15DTE_MIN,
            "WATCHLIST": WATCHLIST_15DTE_MIN}[cls]


def suspicion_label(dte: int, premarket: bool, premium: float) -> str:
    if premarket and dte == 0:
        return "🔴🔴 PRE-MARKET 0DTE"
    if dte == 0 and premium >= 1_000_000:
        return "🔴 0DTE HIGH"
    if dte == 0:
        return "🟡 0DTE ELEVATED"
    if premium >= 3_000_000:
        return "🔴 SHORT-TERM HIGH"
    return "🟡 SHORT-TERM ELEVATED"


async def fire_0dte_alert(t, ticker, option_type, strike, expiry, dte,
                          price, size, premium, trigger, premarket):
    try:
        cls = get_0dte_ticker_class(ticker)
        header = {"BROAD_ETF": "🏦 SECTION 1 — MARKET ETF",
                  "LARGE_CAP": "🏦 SECTION 1 — LARGE CAP"}.get(cls, "📈 SECTION 2 — WATCHLIST")
        section = "MARKET" if cls in ("BROAD_ETF", "LARGE_CAP") else "WATCHLIST"
        tier_label = "0DTE" if dte == 0 else f"{dte}DTE"
        spot = await get_underlying_price(ticker)
        msg = (
            f"🚨 SUSPICIOUS OPTIONS FLOW\n"
            f"{header}\n"
            f"{'=' * 36}\n"
            f"Ticker:    {ticker} [{tier_label}]\n"
            f"Direction: {'BEARISH 🔻' if option_type == 'PUT' else 'BULLISH 🚀'}\n"
            f"Type:      {option_type}\n"
            f"Strike:    ${strike:.2f}\n"
            f"Expiry:    {expiry} ({format_dte(dte)})\n"
            f"Premium:   {format_premium(premium)}\n"
            f"Contracts: {size:,}\n"
            f"Per Ctrt:  ${price:.2f}\n"
            f"Spot:      {fmt_spot(spot)}\n"
            f"Status:    {otm_label(option_type, strike, spot)}\n"
            f"{'=' * 36}\n"
            f"Suspicion: {suspicion_label(dte, premarket, premium)}\n"
            f"Trigger:   {trigger}\n"
            f"Session:   {'⚠️ PRE-MARKET' if premarket else 'Regular Hours'}\n"
            f"{'=' * 36}\n"
            f"⏰ {fmt_clock()}"
        )
        log.info(f"0DTE ALERT [{tier_label}] [{cls}] {ticker} {option_type} "
                 f"{format_premium(premium)} — {trigger}")
        await send_0dte(msg)
        queue_0dte(t, ticker, option_type, strike, expiry, dte, price, size, premium,
                   trigger, tier_label, section, premarket)
        await dte_sink.maybe_flush()
    except Exception as e:
        log.error(f"fire_0dte_alert error: {e}")


async def fire_ratio_alert(t, ticker, option_type, strike, expiry, dte,
                           price, size, premium, ratio, total_prem):
    try:
        spot = await get_underlying_price(ticker)
        msg = (
            f"📊 RATIO ALERT — SUSPICIOUS FLOW\n"
            f"📈 SECTION 2 — WATCHLIST\n"
            f"{'=' * 36}\n"
            f"Ticker:      {ticker}\n"
            f"Dominant:    {option_type} ({ratio:.0f}:1)\n"
            f"Strike:      ${strike:.2f}\n"
            f"Expiry:      {expiry} ({format_dte(dte)})\n"
            f"Window Prem: {format_premium(total_prem)} (5 min)\n"
            f"Last Trade:  {format_premium(premium)}\n"
            f"Spot:        {fmt_spot(spot)}\n"
            f"{'=' * 36}\n"
            f"⚠️ {ratio:.0f}:1 one-sided flow in 5-min window\n"
            f"⏰ {fmt_clock()}"
        )
        log.info(f"RATIO ALERT {ticker} {option_type} {ratio:.0f}:1")
        await send_0dte(msg)
        queue_0dte(t, ticker, option_type, strike, expiry, dte, price, size, premium,
                   f"RATIO_{ratio:.0f}:1", "0DTE" if dte == 0 else f"{dte}DTE",
                   "WATCHLIST", is_premarket(t))
        await dte_sink.maybe_flush()
    except Exception as e:
        log.error(f"fire_ratio_alert error: {e}")


async def process_0dte_trade(t, ticker, option_type, strike, expiry, dte, price, size, premium):
    try:
        premarket = is_premarket(t)
        if ticker in WATCHLIST_0DTE:
            update_ratio_window(ticker, option_type, premium)

        if dte == 0 and premarket and premium >= PREMARKET_0DTE_MIN:
            if not already_alerted((ticker, option_type, strike, str(expiry), "PREMARKET")):
                await fire_0dte_alert(t, ticker, option_type, strike, expiry, dte,
                                      price, size, premium, "PRE-MARKET 0DTE", premarket)
            return

        threshold = get_0dte_threshold(ticker, dte)
        if premium >= threshold:
            tier_tag = "0DTE" if dte == 0 else f"{dte}DTE"
            if not already_alerted((ticker, option_type, strike, str(expiry), tier_tag)):
                await fire_0dte_alert(t, ticker, option_type, strike, expiry, dte,
                                      price, size, premium,
                                      f"{format_premium(threshold)}+ {tier_tag} SINGLE TRADE",
                                      premarket)

        if ticker in WATCHLIST_0DTE and dte == 0:
            hit, dominant, ratio, total = check_ratio(ticker)
            if hit and not already_alerted((ticker, dominant, "RATIO")):
                await fire_ratio_alert(t, ticker, dominant, strike, expiry, dte,
                                       price, size, premium, ratio, total)
    except Exception as e:
        log.error(f"process_0dte_trade error: {e}")


# ============================================================
# UNIFIED TRADE ROUTER
# ============================================================
async def process_trade(event: dict) -> None:
    try:
        sym = event.get("sym", "")
        price = float(event.get("p", 0))
        size = int(event.get("s", 0))
        if not sym or price <= 0 or size <= 0:
            return
        parsed = parse_option_symbol(sym)
        if not parsed:
            return

        await rollover_if_new_day()

        t = event_time_et(event)
        ticker = parsed["ticker"]
        expiry = parsed["expiry"]
        option_type = parsed["option_type"]
        strike = parsed["strike"]
        dte = (expiry - t.date()).days
        premium = calculate_premium(price, size)
        args = (t, ticker, option_type, strike, expiry, dte, price, size, premium)

        # SYSTEM 1
        if ticker in ALL_SILENT_TICKERS:
            await handle_silent(*args)
        elif ticker in SMALL_CAP_TICKERS:
            if 0 <= dte <= SMALL_CAP_MAX_DTE:
                await handle_small_cap(*args)
        elif ticker in ALL_ALERT_TICKERS:
            if dte >= 0:
                await handle_large_cap(*args)

        # SYSTEM 2
        if ticker in ALL_0DTE_TICKERS and 0 <= dte <= 5:
            await process_0dte_trade(*args)
    except Exception as e:
        log.error(f"process_trade error: {e}")


async def process_messages(events: list) -> None:
    for event in events:
        ev = event.get("ev", "")
        if ev == "T":
            await process_trade(event)
        elif ev == "status":
            log.info(f"Status: {event.get('status')} — {event.get('message', '')}")


# ============================================================
# SCHEDULED TASKS (all times Eastern)
# ============================================================
async def scheduler() -> None:
    """Every 30s: day rollover, 9:25 AM briefing, 4:05 PM summary."""
    global morning_briefing_sent, daily_summary_sent
    while True:
        try:
            await rollover_if_new_day()
            now = now_et()
            clock = now.time()
            weekday = now.weekday() < 5

            if weekday and BRIEFING_START <= clock < BRIEFING_END and not morning_briefing_sent:
                morning_briefing_sent = True
                await send_morning_briefing()

            if weekday and clock >= SUMMARY_TIME and not daily_summary_sent:
                daily_summary_sent = True
                await send_daily_summary()
        except Exception as e:
            log.error(f"scheduler error: {e}")
        await asyncio.sleep(30)


async def send_daily_summary() -> None:
    lines = [
        f"{t}: Calls ${d['calls']:,.0f} | Puts ${d['puts']:,.0f}"
        for t, d in sorted(daily_tally.items())
        if d["calls"] > 0 or d["puts"] > 0
    ]
    if lines:
        await send_main("📊 DAILY TALLY SUMMARY\n" + "=" * 32 + "\n" + "\n".join(lines))


async def send_morning_briefing() -> None:
    """Yesterday's top silent-tier accumulation, sent before the open."""
    if main_sink.client is None:
        return
    yesterday = get_last_n_trading_days(1)[0].isoformat()
    query = f"""
        SELECT ticker, option_type, strike, expiry,
               SUM(size) AS total_contracts,
               SUM(premium) AS total_premium,
               COUNT(*) AS prints
        FROM `{main_sink.full_ref}`
        WHERE trade_date = '{yesterday}'
          AND tier = 'SILENT'
          AND dte BETWEEN {SILENT_DTE_MIN} AND {SILENT_DTE_MAX}
        GROUP BY ticker, option_type, strike, expiry
        HAVING SUM(premium) >= {SILENT_ACCUM_TIER1}
        ORDER BY total_premium DESC
        LIMIT 10
    """
    try:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(
            None, lambda: list(main_sink.client.query(query).result())
        )
        if rows:
            lines = [
                f"• {r.ticker} {r.option_type} ${r.strike} {r.expiry} — "
                f"{format_premium(r.total_premium)} ({r.prints} prints){get_earnings_tag(r.ticker)}"
                for r in rows
            ]
            await send_main(
                f"🌅 MARKET OPEN IN 5 MINUTES\n"
                f"{'=' * 34}\n"
                f"Yesterday's silent accumulation\n"
                f"(watch for continuation at open):\n\n"
                + "\n".join(lines)
                + f"\n\n{'=' * 34}\n⏰ {fmt_clock()}"
            )
            log.info(f"Morning briefing sent: {len(rows)} contracts")
        else:
            await send_main(f"🌅 MARKET OPEN IN 5 MINUTES\n"
                            f"No silent accumulation carryover from yesterday.\n⏰ {fmt_clock()}")
    except Exception as e:
        log.error(f"morning_briefing error: {e}")


# ============================================================
# WEBSOCKET
# ============================================================
async def authenticate(ws) -> bool:
    """Send auth and wait for auth_success (the server may send 'connected' first)."""
    await ws.send(json.dumps({"action": "auth", "params": MASSIVE_API_KEY}))
    for _ in range(3):
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        data = json.loads(raw)
        events = data if isinstance(data, list) else [data]
        statuses = {e.get("status") for e in events}
        log.info(f"Auth response: {statuses}")
        if "auth_success" in statuses:
            return True
        if "auth_failed" in statuses:
            return False
    return False


async def run_scanner() -> None:
    global ALL_SILENT_TICKERS, ALL_TICKERS
    load_config()

    ALL_SILENT_TICKERS = load_silent_tickers(SILENT_TICKERS_FILE) - ALL_ALERT_TICKERS
    ALL_TICKERS = ALL_ALERT_TICKERS | ALL_SILENT_TICKERS

    log.info("=" * 60)
    log.info(f"OPTIONS FLOW SCANNER — {VERSION}")
    log.info(f"System 1: {len(ALL_ALERT_TICKERS)} alert tickers | {len(ALL_SILENT_TICKERS)} silent tickers")
    log.info(f"System 2: {len(ALL_0DTE_TICKERS)} 0DTE tickers")
    log.info(f"WebSocket: {WS_URL}")
    log.info("=" * 60)

    creds = Credentials.from_service_account_info(
        GOOGLE_CREDS, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(project=BQ_PROJECT_ID, credentials=creds)
    main_sink.init(client)
    dte_sink.init(client)

    await rollover_if_new_day()
    await load_historical_vol()
    load_earnings_calendar()

    log.info(f"Startup delay {STARTUP_DELAY}s...")
    await asyncio.sleep(STARTUP_DELAY)

    await send_main(
        f"✅ OPTIONS SCANNER ONLINE — {VERSION}\n"
        f"{'=' * 34}\n"
        f"🚨 ETFs ({len(ETF_TICKERS)}): $3M single trade\n"
        f"🐋 Large Cap ({len(LARGE_CAP_TICKERS)}): $1M single trade\n"
        f"⚡ Small Cap ({len(SMALL_CAP_TICKERS)}): $250K daily\n"
        f"👁 Silent ({len(ALL_SILENT_TICKERS)}): vol spike + accumulation\n"
        f"📅 Earnings entries: {len(earnings_calendar)}\n"
        f"🕘 Times shown in Eastern\n"
        f"⏰ {fmt_clock()}"
    )
    await send_0dte(
        f"🚨 0DTE SCANNER ONLINE — {VERSION}\n"
        f"{'=' * 34}\n"
        f"🏦 Broad ETFs: {len(BROAD_ETF_0DTE)} | Large Caps: {len(LARGE_CAP_0DTE)} | "
        f"Watchlist: {len(WATCHLIST_0DTE)}\n"
        f"0DTE: ETF $5M | LC $2M | WL $500K\n"
        f"1-5DTE: ETF $3M | LC $2M | WL $2M\n"
        f"Pre-mkt: $50K | Ratio: 20:1 / $200K\n"
        f"⏰ {fmt_clock()}"
    )

    asyncio.create_task(scheduler())
    asyncio.create_task(main_sink.flush_loop())
    asyncio.create_task(dte_sink.flush_loop())

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=10,
                                          close_timeout=15) as ws:
                log.info(f"Connected to {WS_URL}")
                if not await authenticate(ws):
                    log.error(f"Auth failed — retrying in {RECONNECT_DELAY_LONG}s")
                    await asyncio.sleep(RECONNECT_DELAY_LONG)
                    continue
                log.info("Authenticated ✅")
                await ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))
                log.info("Subscribed to T.* — full market surveillance active")
                async for message in ws:
                    try:
                        await process_messages(json.loads(message))
                    except json.JSONDecodeError as e:
                        log.error(f"JSON error: {e}")
        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"Connection closed: {e}. Reconnecting in {RECONNECT_DELAY_SHORT}s...")
            await asyncio.sleep(RECONNECT_DELAY_SHORT)
        except Exception as e:
            log.error(f"Error: {e}. Reconnecting in {RECONNECT_DELAY_LONG}s...")
            await asyncio.sleep(RECONNECT_DELAY_LONG)


if __name__ == "__main__":
    asyncio.run(run_scanner())
