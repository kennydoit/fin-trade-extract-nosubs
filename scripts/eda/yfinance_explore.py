#!/usr/bin/env python3
"""
yfinance API Explorer  (tested against yfinance 1.2.x)

Demonstrates the major yfinance API surfaces:
  1.  Ticker          – single-ticker metadata, history, financials, options
  2.  Tickers         – batch fetch for multiple symbols
  3.  download        – vectorised historical OHLCV download
  4.  Market          – market summary / status
  5.  WebSocket       – synchronous real-time price streaming (5-second demo)
  6.  AsyncWebSocket  – async real-time price streaming (5-second demo)
  7.  Search          – full-text search for quotes and news
  8.  Sector          – sector overview + top ETFs / mutual funds
  9.  Industry        – industry overview + top companies
  10. EquityQuery     – programmatic screener filter builder
  11. Screener        – run a pre-built or custom equity screener via yf.screen()

Results are printed to stdout.  No external credentials are required.

Usage
-----
    python scripts/eda/yfinance_explore.py              # run every demo
    python scripts/eda/yfinance_explore.py ticker       # single demo by name
"""

import asyncio
import logging
import threading
import time

import pandas as pd
import yfinance as yf
from yfinance.screener.screener import screen as yf_screen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DIVIDER = "\n" + "=" * 70 + "\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def show(label: str, obj) -> None:
    """Print a labelled object (DataFrame, dict, or scalar) with a header."""
    print(f"\n--- {label} ---")
    if isinstance(obj, pd.DataFrame):
        print(obj.to_string() if len(obj) <= 15 else obj.head(10).to_string())
    elif isinstance(obj, dict):
        for k, v in list(obj.items())[:20]:
            print(f"  {k}: {v}")
    elif isinstance(obj, list):
        for item in obj[:10]:
            print(f"  {item}")
    else:
        print(obj)
    print()


# ===========================================================================
# 1. Ticker
# ===========================================================================

def demo_ticker() -> None:
    print(DIVIDER + "1. TICKER — yf.Ticker('AAPL')")

    ticker = yf.Ticker("AAPL")

    # -- Basic info (curated subset) ---------------------------------------
    interesting_keys = [
        "shortName", "sector", "industry", "marketCap",
        "trailingPE", "forwardPE", "dividendYield",
        "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
        "currentPrice", "currency", "exchange",
    ]
    info = {k: ticker.info.get(k) for k in interesting_keys}
    show("ticker.info (selected fields)", info)

    # -- Historical prices --------------------------------------------------
    show("ticker.history(period='5d')", ticker.history(period="5d"))

    # -- Financials ---------------------------------------------------------
    show("ticker.income_stmt (annual)", ticker.income_stmt)
    show("ticker.balance_sheet (annual)", ticker.balance_sheet)
    show("ticker.cashflow (annual)", ticker.cashflow)

    # -- Dividends & splits -------------------------------------------------
    show("ticker.dividends (last 5)", ticker.dividends.tail(5))
    show("ticker.splits", ticker.splits)

    # -- Earnings dates -----------------------------------------------------
    show("ticker.earnings_dates", ticker.earnings_dates)

    # -- Analyst recommendations --------------------------------------------
    recs = ticker.recommendations
    show("ticker.recommendations (last 5)", recs.tail(5) if recs is not None else "n/a")

    # -- Options chain (nearest expiry) ------------------------------------
    if ticker.options:
        expiry = ticker.options[0]
        chain = ticker.option_chain(expiry)
        show(f"ticker.option_chain('{expiry}') — calls (first 5 rows)", chain.calls.head(5))
        show(f"ticker.option_chain('{expiry}') — puts  (first 5 rows)", chain.puts.head(5))
    else:
        print("  No options data available.\n")

    # -- News ---------------------------------------------------------------
    news = ticker.news
    show("ticker.news (first item)", news[0] if news else "n/a")


# ===========================================================================
# 2. Tickers
# ===========================================================================

def demo_tickers() -> None:
    print(DIVIDER + "2. TICKERS — yf.Tickers('AAPL MSFT GOOGL')")

    tickers = yf.Tickers("AAPL MSFT GOOGL")

    for sym, t in tickers.tickers.items():
        price = t.info.get("currentPrice") or t.info.get("regularMarketPrice")
        name  = t.info.get("shortName", sym)
        print(f"  {sym:6s}  {name:35s}  price: {price}")
    print()

    # Bulk history — returns a MultiIndex DataFrame keyed by (field, symbol)
    hist = tickers.history(period="5d")
    show("tickers.history(period='5d') — Close prices", hist["Close"])


# ===========================================================================
# 3. download
# ===========================================================================

def demo_download() -> None:
    print(DIVIDER + "3. DOWNLOAD — yf.download()")

    # Single ticker, daily bars
    df_single = yf.download("SPY", period="1mo", interval="1d", progress=False)
    show("yf.download('SPY', period='1mo', interval='1d')", df_single)

    # Multiple tickers — MultiIndex columns (field, symbol)
    df_multi = yf.download(
        ["SPY", "QQQ", "IWM"], period="5d", interval="1d", progress=False
    )
    show("yf.download(['SPY','QQQ','IWM'], period='5d') — Close", df_multi["Close"])

    # Intraday (1-minute bars)
    df_intra = yf.download("AAPL", period="5d", interval="1m", progress=False)
    show("yf.download('AAPL', period='5d', interval='1m') — last 5 rows", df_intra.tail(5))


# ===========================================================================
# 4. Market
# ===========================================================================

def demo_market() -> None:
    print(DIVIDER + "4. MARKET — yf.Market('us_market')")

    market = yf.Market("us_market")

    show("market.status", market.status)
    show("market.summary", market.summary)


# ===========================================================================
# 5. WebSocket  (synchronous, 5-second live demo)
# ===========================================================================

def demo_websocket() -> None:
    """
    WebSocket API (yfinance 1.2.x):
      ws = yf.WebSocket()
      ws.subscribe(symbols)      # connect + send subscribe message
      ws.listen(handler)         # blocking loop; exits on KeyboardInterrupt
                                 # or when ws.close() is called externally
      ws.close()
    """
    print(DIVIDER + "5. WEBSOCKET — yf.WebSocket (5-second live stream)")

    received: list[dict] = []

    def on_message(msg: dict) -> None:
        received.append(msg)
        sym   = msg.get("id", "?")
        price = msg.get("price") or msg.get("regularMarketPrice", "?")
        ts    = msg.get("time", "")
        print(f"  [ws] {sym:6s}  price={price}  time={ts}")

    ws = yf.WebSocket(verbose=False)
    ws.subscribe(["AAPL", "MSFT", "GOOGL"])

    # listen() is a blocking while-True loop; run it on a background thread
    # so we can close the connection from the main thread after 5 s.
    listener = threading.Thread(target=ws.listen, args=(on_message,), daemon=True)
    log.info("Starting WebSocket — collecting for 5 seconds …")
    listener.start()
    time.sleep(5)
    ws.close()
    listener.join(timeout=2)

    print(f"\n  Total messages received: {len(received)}\n")


# ===========================================================================
# 6. AsyncWebSocket  (async, 5-second live demo)
# ===========================================================================

def demo_async_websocket() -> None:
    """
    AsyncWebSocket API (yfinance 1.2.x):
      aws = yf.AsyncWebSocket()
      await aws.subscribe(symbols)
      await aws.listen(handler)      # blocking async loop
      await aws.close()
    """
    print(DIVIDER + "6. ASYNC WEBSOCKET — yf.AsyncWebSocket (5-second live stream)")

    received: list[dict] = []

    async def on_message(msg: dict) -> None:
        received.append(msg)
        sym   = msg.get("id", "?")
        price = msg.get("price") or msg.get("regularMarketPrice", "?")
        ts    = msg.get("time", "")
        print(f"  [aws] {sym:6s}  price={price}  time={ts}")

    async def run() -> None:
        aws = yf.AsyncWebSocket(verbose=False)
        await aws.subscribe(["AAPL", "MSFT"])

        # listen() is an async blocking loop; cancel it via a task after 5 s.
        log.info("Starting AsyncWebSocket — collecting for 5 seconds …")
        listen_task = asyncio.create_task(aws.listen(on_message))
        await asyncio.sleep(5)
        listen_task.cancel()
        try:
            await listen_task
        except asyncio.CancelledError:
            pass
        await aws.close()

    asyncio.run(run())
    print(f"\n  Total messages received: {len(received)}\n")


# ===========================================================================
# 7. Search
# ===========================================================================

def demo_search() -> None:
    print(DIVIDER + "7. SEARCH — yf.Search()")

    # Equity / ETF search
    search = yf.Search("Apple Inc", max_results=5, news_count=3)
    show("search.quotes  (Apple Inc)", search.quotes)
    show("search.news    (Apple Inc, first 3)", search.news[:3] if search.news else [])

    # ETF search
    etf_search = yf.Search("S&P 500 ETF", max_results=5)
    show("search.quotes  (S&P 500 ETF)", etf_search.quotes)

    # search.all — combined response dict
    show("search.all keys", list(search.all.keys()) if isinstance(search.all, dict) else search.all)


# ===========================================================================
# 8. Sector
# ===========================================================================

def demo_sector() -> None:
    print(DIVIDER + "8. SECTOR — yf.Sector('technology')")

    sector = yf.Sector("technology")

    show("sector.overview",       sector.overview)
    show("sector.industries",     sector.industries)
    show("sector.top_etfs",       sector.top_etfs)
    show("sector.top_mutual_funds", sector.top_mutual_funds)
    top_cos = sector.top_companies
    show(
        "sector.top_companies (first 10)",
        top_cos.head(10) if isinstance(top_cos, pd.DataFrame) else top_cos,
    )


# ===========================================================================
# 9. Industry
# ===========================================================================

def demo_industry() -> None:
    print(DIVIDER + "9. INDUSTRY — yf.Industry('semiconductors')")

    industry = yf.Industry("semiconductors")

    show("industry.overview", industry.overview)

    top_cos = industry.top_companies
    show(
        "industry.top_companies (first 10)",
        top_cos.head(10) if isinstance(top_cos, pd.DataFrame) else top_cos,
    )

    top_growth = industry.top_growth_companies
    show(
        "industry.top_growth_companies (first 10)",
        top_growth.head(10) if isinstance(top_growth, pd.DataFrame) else top_growth,
    )

    top_perf = industry.top_performing_companies
    show(
        "industry.top_performing_companies (first 10)",
        top_perf.head(10) if isinstance(top_perf, pd.DataFrame) else top_perf,
    )


# ===========================================================================
# 10. EquityQuery
# ===========================================================================

def demo_equity_query() -> None:
    """
    EquityQuery API (yfinance 1.2.x):
      EquityQuery(operator, operand)

    Value operators : 'eq', 'is-in', 'btwn', 'gt', 'lt', 'gte', 'lte'
      operand = [field_name, value]        (gt/lt/eq/…)
      operand = [field_name, v1, v2]       (btwn)
      operand = [field_name, val1, val2, …](is-in)

    Logical operators: 'and', 'or'
      operand = [EquityQuery, EquityQuery, …]

    Execute with: yf.screen(query, size=N, sortField=..., sortAsc=...)
    """
    print(DIVIDER + "10. EQUITY QUERY — yf.EquityQuery")

    # Filter: US large-cap tech stocks with trailing P/E < 30
    # Valid eq fields  : region, sector, exchange, industry, peer_group
    # Valid gt/lt fields: intradaymarketcap, peratio.lasttwelvemonths, etc.
    #   (run yf.EquityQuery('gt',['intradaymarketcap',0]).valid_fields for full list)
    query = yf.EquityQuery(
        "and",
        [
            yf.EquityQuery("eq",  ["region",   "us"]),
            yf.EquityQuery("eq",  ["sector",   "Technology"]),
            yf.EquityQuery("lt",  ["peratio.lasttwelvemonths", 30]),
            yf.EquityQuery("gt",  ["intradaymarketcap", 10_000_000_000]),
        ],
    )

    show("EquityQuery object (repr)", repr(query))

    result = yf_screen(query, size=10, sortField="intradaymarketcap", sortAsc=False)
    quotes = result.get("quotes", [])
    rows = [
        {
            "symbol":    q.get("symbol"),
            "shortName": q.get("shortName"),
            "sector":    q.get("sector"),
            "trailingPE": q.get("trailingPE"),
            "marketCap": q.get("marketCap"),
        }
        for q in quotes
    ]
    show("EquityQuery screener results (top 10 by market cap)", pd.DataFrame(rows))

    # Show available field names for building custom queries
    print("--- EquityQuery.valid_fields (category keys) ---")
    sample_query = yf.EquityQuery("gt", ["percentchange", 0])
    for category in list(sample_query.valid_fields.keys())[:5]:
        print(f"  {category}")
    print("  … (run sample_query.valid_fields for full list)\n")


# ===========================================================================
# 11. Screener  (yf.screen — pre-built and custom)
# ===========================================================================

def demo_screener() -> None:
    """
    yf.screen() function (yfinance 1.2.x):
      yf.screen(query_name_str)          # use a predefined screener by name
      yf.screen(EquityQuery, size=N, …)  # custom query

    Available predefined names: yf.PREDEFINED_SCREENER_QUERIES.keys()
    """
    print(DIVIDER + "11. SCREENER — yf.screen() / yf.PREDEFINED_SCREENER_QUERIES")

    # List available predefined screener names
    show(
        "yf.PREDEFINED_SCREENER_QUERIES (available predefined keys)",
        list(yf.PREDEFINED_SCREENER_QUERIES.keys()),
    )

    def _quotes_table(result: dict, n: int = 10) -> pd.DataFrame:
        quotes = result.get("quotes", [])
        return pd.DataFrame(
            [
                {
                    "symbol":             q.get("symbol"),
                    "shortName":          q.get("shortName"),
                    "price":              q.get("regularMarketPrice"),
                    "changePct":          round(q.get("regularMarketChangePercent", 0) * 100, 2),
                    "volume":             q.get("regularMarketVolume"),
                    "marketCap":          q.get("marketCap"),
                }
                for q in quotes[:n]
            ]
        )

    # --- most actives ---
    show("most_actives (top 10)", _quotes_table(yf_screen("most_actives", count=10)))

    # --- day gainers ---
    show("day_gainers (top 10)",  _quotes_table(yf_screen("day_gainers",  count=10)))

    # --- day losers ---
    show("day_losers (top 10)",   _quotes_table(yf_screen("day_losers",   count=10)))

    # --- undervalued large caps ---
    show(
        "undervalued_large_caps (top 10)",
        _quotes_table(yf_screen("undervalued_large_caps", count=10)),
    )

    # --- growth technology stocks ---
    show(
        "growth_technology_stocks (top 10)",
        _quotes_table(yf_screen("growth_technology_stocks", count=10)),
    )


# ===========================================================================
# Entry point
# ===========================================================================

DEMOS = {
    "ticker":          demo_ticker,
    "tickers":         demo_tickers,
    "download":        demo_download,
    "market":          demo_market,
    "websocket":       demo_websocket,
    "async_websocket": demo_async_websocket,
    "search":          demo_search,
    "sector":          demo_sector,
    "industry":        demo_industry,
    "equity_query":    demo_equity_query,
    "screener":        demo_screener,
}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="yfinance API explorer — run all demos or a specific one."
    )
    parser.add_argument(
        "demo",
        nargs="?",
        choices=list(DEMOS.keys()),
        help="Name of a single demo to run (default: run all).",
    )
    args = parser.parse_args()

    targets = [args.demo] if args.demo else list(DEMOS.keys())

    for name in targets:
        try:
            DEMOS[name]()
        except Exception as exc:
            log.error("Demo '%s' failed: %s", name, exc, exc_info=True)

    print(DIVIDER + "All demos complete.")


if __name__ == "__main__":
    main()
