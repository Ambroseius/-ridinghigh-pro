"""
RidingHigh Pro - Post Analysis Collector v3
Runs every morning via GitHub Actions.
For every stock with Score >= 60 from the daily_snapshots sheet,
fetches D+1 to D+5 OHLC data and saves to post_analysis sheet.

FIX v2: Now properly updates rows that have MaxDrop% but missing D5 data.
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

CATALYST_CATEGORIES = [
    "merger_acquisition", "fda_approval", "clinical_trial",
    "marketing_announcement", "earnings_report", "regulatory_compliance",
    "lawsuit", "share_dilution", "reverse_split", "no_clear_reason"
]

def fetch_finviz_news(ticker: str, scan_date: str) -> list:
    """Fetch news headlines from FINVIZ filtered by date range."""
    import urllib.request, re, time
    from datetime import datetime, timedelta

    scan_dt = datetime.strptime(scan_date, "%Y-%m-%d")
    date_from = scan_dt - timedelta(days=30)
    date_to   = scan_dt + timedelta(days=1)

    for attempt in range(1, 4):
        try:
            url = f"https://finviz.com/quote.ashx?t={ticker}&p=d"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            })
            response = urllib.request.urlopen(req, timeout=10)
            html = response.read().decode("utf-8")

            dates  = re.findall(r'width="130"[^>]*>\s*(.*?)\s*</td>', html)
            titles = re.findall(r'class="tab-link-news"[^>]*>\s*(.*?)\s*</a>', html)

            relevant = []
            for date_str, title in zip(dates, titles):
                title = title.strip().lower()
                try:
                    pub_dt = datetime.strptime(date_str.strip()[:9], "%b-%d-%y")
                    if date_from <= pub_dt <= date_to:
                        relevant.append(title)
                except:
                    continue

            print(f"[Collector] FINVIZ: {len(relevant)} headlines for {ticker} around {scan_date}")
            return relevant

        except Exception as e:
            print(f"[Collector] FINVIZ attempt {attempt}/3 failed for {ticker}: {e}")
            time.sleep(2)
    return []


def analyze_catalyst(ticker: str, scan_date: str) -> dict:
    """Analyze catalyst using FINVIZ news + keyword matching."""
    import urllib.request, urllib.parse, time, re
    from datetime import datetime, timedelta

    KEYWORDS = {
        "merger_acquisition":     ["merger", "acquisition", "acquires", "acquired", "merges", "buyout", "takeover", "combines with", "to buy", "to acquire"],
        "fda_approval":           ["fda approved", "fda approval", "fda clearance", "fda grants", "fda accepts", "orphan drug", "breakthrough designation", "nda approved", "bla approved"],
        "clinical_trial":         ["phase 1", "phase 2", "phase 3", "clinical trial", "clinical study", "ind application", "topline data", "trial results", "patient enrollment"],
        "marketing_announcement": ["partnership", "collaboration agreement", "license agreement", "commercialization", "distribution agreement", "strategic alliance"],
        "earnings_report":        ["earnings report", "quarterly results", "q1 results", "q2 results", "q3 results", "q4 results", "annual results", "revenue report", "full year results"],
        "regulatory_compliance":  ["nasdaq compliance", "nyse compliance", "regained compliance", "listing compliance", "deficiency notice", "bid price", "non-compliance"],
        "lawsuit":                ["class action", "sec charges", "sec investigation", "securities fraud", "lawsuit filed", "legal action", "complaint filed"],
        "share_dilution":         ["public offering", "private placement", "at-the-market", "atm offering", "registered direct", "shares offered", "warrant exercise", "capital raise"],
        "reverse_split":          ["reverse stock split", "reverse split", "1-for-", "share consolidation"],
        "no_clear_reason":        []
    }

    relevant_headlines = fetch_finviz_news(ticker, scan_date)

    if not relevant_headlines:
        result = {f"cat_{k}": 0 for k in CATALYST_CATEGORIES}
        result["cat_no_clear_reason"] = 1
        return result

    combined = " ".join(relevant_headlines)
    cats = {}
    any_found = False
    for category, keywords in KEYWORDS.items():
        if category == "no_clear_reason":
            continue
        found = any(kw in combined for kw in keywords)
        cats[f"cat_{category}"] = 1 if found else 0
        if found:
            any_found = True

    cats["cat_no_clear_reason"] = 0 if any_found else 1
    print(f"[Collector] ✅ {ticker}: {[k.replace('cat_','') for k,v in cats.items() if v==1]}")
    return cats


MIN_SCORE = 60
DAYS_FORWARD = 5


def get_trading_days_after(scan_date_str: str, n: int) -> list:
    """Return n trading days after scan_date."""
    scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d")
    days = []
    current = scan_date + timedelta(days=1)
    while len(days) < n:
        if current.weekday() < 5:
            days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def is_complete(existing_row: pd.Series, trading_days: list) -> bool:
    """
    Returns True only if ALL available trading days have Close data stored.
    A row is NOT complete if any available day is missing its Close value.
    """
    today_dt = datetime.now()
    for i, day in enumerate(trading_days, 1):
        # Only check days that have already passed
        if datetime.strptime(day, "%Y-%m-%d") >= today_dt:
            break
        col = f"D{i}_Close"
        val = existing_row.get(col, None)
        if val is None or str(val).strip() in ["", "nan", "None"]:
            return False
    return True


def fetch_ohlc_for_days(ticker: str, trading_days: list) -> dict:
    """Fetch OHLC for specific trading days using Yahoo Finance."""
    import time
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            start = trading_days[0]
            end_dt = datetime.strptime(trading_days[-1], "%Y-%m-%d") + timedelta(days=3)
            end = end_dt.strftime("%Y-%m-%d")

            hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if hist.empty:
                print(f"[Collector] {ticker} attempt {attempt}/{max_retries} — empty data")
                time.sleep(2)
                continue

            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
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
            print(f"[Collector] {ticker} attempt {attempt}/{max_retries} error: {e}")
            time.sleep(2)

    print(f"[Collector] {ticker} — failed after {max_retries} attempts")
    return {}


def calculate_stats(scan_price: float, ohlc: dict) -> dict:
    """Calculate MaxDrop%, BestDay, TP hits from all available lows."""
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

    # D1_Gap%: how much D1 opened vs ScanPrice (positive = gapped up = bad for short)
    d1_open = ohlc.get("D1_Open")
    d1_gap = round((d1_open - scan_price) / scan_price * 100, 2) if d1_open and scan_price > 0 else None

    return {
        "MaxDrop%": max_drop,
        "BestDay":  best_day,
        "TP10_Hit": tp10,
        "TP15_Hit": tp15,
        "TP20_Hit": tp20,
        "D1_Gap%":  d1_gap,
    }


def run():
    print(f"[Collector] Starting post-analysis collection v2...")

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

    candidates = snapshots_df[snapshots_df["Score"] >= MIN_SCORE].copy()
    print(f"[Collector] Found {len(candidates)} stocks with score >= {MIN_SCORE}")

    if candidates.empty:
        print("[Collector] Nothing to process")
        return

    existing_df = load_post_analysis_from_sheets()

    today_str = datetime.now(PERU_TZ).strftime("%Y-%m-%d")
    new_rows = []

    for _, row in candidates.iterrows():
        ticker     = str(row.get("Ticker", "")).strip()
        scan_date  = str(row.get("Date", "")).strip()
        score      = float(row.get("Score", 0))
        scan_price = pd.to_numeric(row.get("Price", 0), errors="coerce") or 0

        if not ticker or not scan_date:
            continue

        if scan_date > today_str:
            print(f"[Collector] Skipping {ticker} ({scan_date}) — future date")
            continue

        # All 5 trading days for this stock
        trading_days = get_trading_days_after(scan_date, DAYS_FORWARD)

        # Check if already in existing_df
        existing_match = pd.DataFrame()
        if not existing_df.empty and "Ticker" in existing_df.columns:
            existing_match = existing_df[
                (existing_df["Ticker"] == ticker) &
                (existing_df["ScanDate"] == scan_date)
            ]

        if not existing_match.empty:
            existing_row = existing_match.iloc[0]
            if is_complete(existing_row, trading_days):
                print(f"[Collector] Complete: {ticker} {scan_date} — skipping")
                continue
            else:
                print(f"[Collector] Incomplete data for {ticker} {scan_date} — updating")

        # Only fetch days that have already passed
        today_dt = datetime.now()
        available_days = [d for d in trading_days if datetime.strptime(d, "%Y-%m-%d") < today_dt]
        print(f"[Collector] {ticker} — {len(available_days)}/{DAYS_FORWARD} days available")

        ohlc  = fetch_ohlc_for_days(ticker, available_days) if available_days else {}
        stats = calculate_stats(scan_price, ohlc)

        metric_fields = ["MxV","RunUp","RSI","ATRX","REL_VOL","Gap","VWAP","Float%","PriceToHigh","PriceTo52WHigh"]
        metrics = {f: round(pd.to_numeric(row.get(f, None), errors="coerce"), 2) for f in metric_fields}

        # Only re-analyze catalyst if this is a new row
        if existing_match.empty:
            print(f"[Collector] Analyzing catalyst for {ticker}...")
            catalyst_data = analyze_catalyst(ticker, scan_date)
        else:
            # Preserve existing catalyst data
            catalyst_data = {}
            for cat in CATALYST_CATEGORIES:
                col = f"cat_{cat}"
                catalyst_data[col] = existing_match.iloc[0].get(col, 0)

        new_row = {
            "Ticker":      ticker,
            "ScanDate":    scan_date,
            "Score":       round(float(score), 2),
            "ScanPrice":   round(float(scan_price), 2),
            "ScanChange%": round(pd.to_numeric(row.get("Change", 0), errors="coerce"), 2),
            **metrics,
            **catalyst_data,
            **{k: round(v, 2) if isinstance(v, float) else v for k, v in ohlc.items()},
            **{k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items()},
        }
        new_rows.append(new_row)

    if not new_rows:
        print("[Collector] No new rows to save")
        return

    new_df = pd.DataFrame(new_rows)
    save_post_analysis_to_sheets(new_df)
    print(f"[Collector] ✅ Saved/updated {len(new_rows)} rows in post_analysis")


if __name__ == "__main__":
    run()
