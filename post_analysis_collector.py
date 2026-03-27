"""
RidingHigh Pro - Post Analysis Collector
Runs every morning via GitHub Actions.
For every stock with Score >= 60 from the daily_snapshots sheet,
fetches D+1 to D+5 OHLC data and saves to post_analysis sheet.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import sys
import os

sys.path.insert(0, os.path.expanduser("~/RidingHighPro"))
from gsheets_sync import load_post_analysis_from_sheets, save_post_analysis_to_sheets

PERU_TZ = pytz.timezone("America/Lima")
MIN_SCORE = 60
DAYS_FORWARD = 5


def get_trading_days_after(scan_date_str: str, n: int) -> list:
    """Return n trading days after scan_date."""
    scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d")
    days = []
    current = scan_date + timedelta(days=1)
    while len(days) < n:
        if current.weekday() < 5:  # Mon-Fri
            days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def fetch_ohlc_for_days(ticker: str, trading_days: list) -> dict:
    """Fetch OHLC for specific trading days using Yahoo Finance."""
    try:
        start = trading_days[0]
        # End = day after last trading day
        end_dt = datetime.strptime(trading_days[-1], "%Y-%m-%d") + timedelta(days=3)
        end = end_dt.strftime("%Y-%m-%d")

        hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if hist.empty:
            return {}

        # Flatten MultiIndex if needed
        if isinstance(hist.columns, pd.MultiIndex):
            # For single ticker, get level 0 (Price type)
            hist.columns = hist.columns.get_level_values(0)
        # Ensure columns are strings
        hist.columns = [str(c) for c in hist.columns]

        hist.index = pd.to_datetime(hist.index).strftime("%Y-%m-%d")
        result = {}
        for i, day in enumerate(trading_days, 1):
            if day in hist.index:
                row = hist.loc[day]
                result[f"D{i}_Open"]  = round(float(row["Open"]), 4)
                result[f"D{i}_High"]  = round(float(row["High"]), 4)
                result[f"D{i}_Low"]   = round(float(row["Low"]), 4)
                result[f"D{i}_Close"] = round(float(row["Close"]), 4)
            else:
                result[f"D{i}_Open"]  = None
                result[f"D{i}_High"]  = None
                result[f"D{i}_Low"]   = None
                result[f"D{i}_Close"] = None
        return result

    except Exception as e:
        print(f"[Collector] Error fetching {ticker}: {e}")
        return {}


def calculate_stats(scan_price: float, ohlc: dict) -> dict:
    """Calculate MaxDrop%, BestDay, TP hits."""
    lows = []
    for i in range(1, 6):
        low = ohlc.get(f"D{i}_Low")
        if low is not None:
            lows.append((i, low))

    if not lows or scan_price <= 0:
        return {"MaxDrop%": None, "BestDay": None,
                "TP10_Hit": 0, "TP15_Hit": 0, "TP20_Hit": 0}

    best_day, min_low = min(lows, key=lambda x: x[1])
    max_drop = round((min_low - scan_price) / scan_price * 100, 2)

    tp10 = 1 if min_low <= scan_price * 0.90 else 0
    tp15 = 1 if min_low <= scan_price * 0.85 else 0
    tp20 = 1 if min_low <= scan_price * 0.80 else 0

    return {
        "MaxDrop%": max_drop,
        "BestDay": best_day,
        "TP10_Hit": tp10,
        "TP15_Hit": tp15,
        "TP20_Hit": tp20
    }


def run():
    print(f"[Collector] Starting post-analysis collection...")

    # Load existing snapshots from Sheets
    from gsheets_sync import _get_client, SPREADSHEET_ID, TAB_DAILY_SNAPSHOT
    gc = _get_client()
    if gc is None:
        print("[Collector] ❌ Cannot connect to Google Sheets")
        return

    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        ws = sh.worksheet(TAB_DAILY_SNAPSHOT)
        data = ws.get_all_values()
    except Exception as e:
        print(f"[Collector] ❌ Cannot load snapshots: {e}")
        return

    if len(data) <= 1:
        print("[Collector] No snapshot data found")
        return

    snapshots_df = pd.DataFrame(data[1:], columns=data[0])
    snapshots_df["Score"] = pd.to_numeric(snapshots_df.get("Score", 0), errors="coerce")

    # Filter score >= 60
    candidates = snapshots_df[snapshots_df["Score"] >= MIN_SCORE].copy()
    print(f"[Collector] Found {len(candidates)} stocks with score >= {MIN_SCORE}")

    if candidates.empty:
        print("[Collector] Nothing to process")
        return

    # Load already-processed rows to avoid re-fetching
    existing_df = load_post_analysis_from_sheets()
    already_done = set()
    if not existing_df.empty and "Ticker" in existing_df.columns and "ScanDate" in existing_df.columns:
        already_done = set(zip(existing_df["Ticker"], existing_df["ScanDate"]))

    today_str = datetime.now(PERU_TZ).strftime("%Y-%m-%d")
    new_rows = []

    for _, row in candidates.iterrows():
        ticker    = str(row.get("Ticker", "")).strip()
        scan_date = str(row.get("Date", "")).strip()
        score     = float(row.get("Score", 0))
        scan_price = pd.to_numeric(row.get("Price", 0), errors="coerce") or 0

        if not ticker or not scan_date:
            continue

        # Skip only future dates
        if scan_date > today_str:
            print(f"[Collector] Skipping {ticker} ({scan_date}) — future date")
            continue

        # Skip if already processed
        if (ticker, scan_date) in already_done:
            print(f"[Collector] Already done: {ticker} {scan_date}")
            continue

        # Fetch whatever days are available (don't wait for D+5)
        trading_days = get_trading_days_after(scan_date, DAYS_FORWARD)
        today_dt = datetime.now()
        available_days = [d for d in trading_days if datetime.strptime(d, "%Y-%m-%d") < today_dt]
        if not available_days:
            print(f"[Collector] {ticker} — no days available yet, skipping")
            continue
        trading_days = available_days
        print(f"[Collector] {ticker} — {len(trading_days)} days available")

        print(f"[Collector] Processing {ticker} (scan: {scan_date}, score: {score})")
        ohlc = fetch_ohlc_for_days(ticker, trading_days)

        if not ohlc:
            print(f"[Collector] No data for {ticker}")
            continue

        stats = calculate_stats(scan_price, ohlc)

        # Grab all metrics from snapshot
        metric_fields = ["MxV","RunUp","RSI","ATRX","REL_VOL","Gap","VWAP","Float%","PriceToHigh","PriceTo52WHigh"]
        metrics = {f: round(pd.to_numeric(row.get(f, None), errors="coerce"), 2) for f in metric_fields}

        new_row = {
            "Ticker":      ticker,
            "ScanDate":    scan_date,
            "Score":       round(float(score), 2),
            "ScanPrice":   round(float(scan_price), 2),
            "ScanChange%": round(pd.to_numeric(row.get("Change", 0), errors="coerce"), 2),
            **metrics,
            **{k: round(v, 2) if isinstance(v, float) else v for k, v in ohlc.items()},
            **{k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items()}
        }
        new_rows.append(new_row)

    if not new_rows:
        print("[Collector] No new rows to save")
        return

    new_df = pd.DataFrame(new_rows)
    save_post_analysis_to_sheets(new_df)
    print(f"[Collector] ✅ Saved {len(new_rows)} new rows to post_analysis")


if __name__ == "__main__":
    run()
