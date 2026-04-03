"""
RidingHigh Pro - Enrich Post Analysis with Intraday Data v2
Adds:
  - IntraHigh, IntraLow, PeakScoreTime, PeakScorePrice, PeakScore, DayRunUp% (from timeline_live)
  - D0_Close, D0_Volume, D0_Drop%, IntraDay_TP10 (from Yahoo Finance)
"""

import sys
sys.path.insert(0, "/Users/adilevy/RidingHighPro")
from gsheets_sync import _get_client, SPREADSHEET_ID, load_post_analysis_from_sheets, _df_to_sheet
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

def fetch_d0_data(ticker: str, scan_date: str) -> dict:
    """
    Fetch D0 (scan day) closing price and volume from Yahoo Finance.
    Returns D0_Close, D0_Volume, or None if unavailable.
    """
    for attempt in range(1, 4):
        try:
            scan_dt  = datetime.strptime(scan_date, "%Y-%m-%d")
            end_dt   = scan_dt + timedelta(days=1)
            hist = yf.download(
                ticker,
                start=scan_date,
                end=end_dt.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=True
            )
            if hist.empty:
                time.sleep(1)
                continue

            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)
            hist.columns = [str(c) for c in hist.columns]
            hist.index = pd.to_datetime(hist.index).strftime("%Y-%m-%d")

            if scan_date in hist.index:
                row = hist.loc[scan_date]
                return {
                    "D0_Close":  round(float(row["Close"]), 4),
                    "D0_Volume": int(row["Volume"]),
                }
            return {}
        except Exception as e:
            print(f"[Enrich] D0 fetch attempt {attempt}/3 failed for {ticker}: {e}")
            time.sleep(2)
    return {}


def is_trading_day(date=None):
    """Returns True if date is a NASDAQ trading day. Falls back to weekday check."""
    import pytz
    if date is None:
        date = datetime.now(pytz.timezone("America/Lima")).date()
    try:
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar("NASDAQ")
        schedule = nyse.schedule(
            start_date=date.strftime("%Y-%m-%d"),
            end_date=date.strftime("%Y-%m-%d")
        )
        return not schedule.empty
    except ImportError:
        print("[Enrich] ⚠️ pandas_market_calendars not installed — using weekday-only check")
        return date.weekday() < 5


def run():
    print("[Enrich] Starting v2...")

    # ── Check if today is a trading day ────────────────────────────────────
    import pytz
    today = datetime.now(pytz.timezone("America/Lima")).date()
    if not is_trading_day(today):
        print(f"[Enrich] ⛔ {today} is not a trading day — skipping.")
        return

    gc = _get_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    # ── Load timeline_live (batched to handle 138K+ rows) ──────────────────
    print("[Enrich] Loading timeline_live (batched)...")
    ws_tl = sh.worksheet("timeline_live")
    headers_tl = ws_tl.row_values(1)
    total_rows  = len(ws_tl.col_values(1))
    col_range   = chr(64 + len(headers_tl))
    BATCH_SIZE  = 5000
    all_rows    = []
    row_start   = 2
    while row_start <= total_rows:
        row_end = min(row_start + BATCH_SIZE - 1, total_rows)
        batch   = ws_tl.get(f"A{row_start}:{col_range}{row_end}")
        if not batch:
            break
        all_rows.extend(batch)
        row_start += BATCH_SIZE

    tl = pd.DataFrame(all_rows, columns=headers_tl)
    tl["Price"]  = pd.to_numeric(tl["Price"],  errors="coerce")
    tl["Score"]  = pd.to_numeric(tl["Score"],  errors="coerce")
    tl["Volume"] = pd.to_numeric(tl["Volume"], errors="coerce")
    print(f"[Enrich] Timeline rows loaded: {len(tl)}")

    # ── Load post_analysis ──────────────────────────────────────────────────
    pa = load_post_analysis_from_sheets()
    print(f"[Enrich] Post analysis rows: {len(pa)}")

    # Ensure new columns exist
    for col in ["IntraHigh", "IntraLow", "PeakScoreTime", "PeakScorePrice",
                "PeakScore", "DayRunUp%", "D0_Close", "D0_Volume", "D0_Drop%", "IntraDay_TP10"]:
        if col not in pa.columns:
            pa[col] = None

    # ── Enrich each row ─────────────────────────────────────────────────────
    updated = 0
    for idx, row in pa.iterrows():
        ticker    = row["Ticker"]
        scan_date = row["ScanDate"]
        scan_price = pd.to_numeric(row.get("ScanPrice", 0), errors="coerce") or 0

        # ── Timeline-based enrichment ───────────────────────────────────────
        day_tl = tl[(tl["Ticker"] == ticker) & (tl["Date"] == scan_date)]

        if not day_tl.empty:
            intra_high  = round(day_tl["Price"].max(), 2)
            intra_low   = round(day_tl["Price"].min(), 2)
            peak_idx    = day_tl["Score"].idxmax()
            peak_time   = day_tl.loc[peak_idx, "ScanTime"]
            peak_price  = round(day_tl.loc[peak_idx, "Price"], 2)
            peak_score  = round(day_tl.loc[peak_idx, "Score"], 2)
            first_price = round(day_tl.sort_values("ScanTime").iloc[0]["Price"], 2)
            run_up_pct  = round((intra_high - first_price) / first_price * 100, 2) if first_price > 0 else 0

            pa.at[idx, "IntraHigh"]      = intra_high
            pa.at[idx, "IntraLow"]       = intra_low
            pa.at[idx, "PeakScoreTime"]  = peak_time
            pa.at[idx, "PeakScorePrice"] = peak_price
            pa.at[idx, "PeakScore"]      = peak_score
            pa.at[idx, "DayRunUp%"]      = run_up_pct

            # IntraDay_TP10 — did price drop 10% from ScanPrice within scan day?
            if scan_price > 0:
                pa.at[idx, "IntraDay_TP10"] = 1 if intra_low <= scan_price * 0.90 else 0

        # ── D0 enrichment (only if missing or empty) ───────────────────────
        d0_missing = (
            pd.isna(row.get("D0_Close")) or
            str(row.get("D0_Close", "")).strip() in ["", "nan", "None"]
        )

        if d0_missing:
            print(f"[Enrich] Fetching D0 for {ticker} ({scan_date})...")
            d0 = fetch_d0_data(ticker, scan_date)

            if d0:
                pa.at[idx, "D0_Close"]  = d0["D0_Close"]
                pa.at[idx, "D0_Volume"] = d0["D0_Volume"]

                # D0_Drop% = close vs ScanPrice
                if scan_price > 0:
                    d0_drop = round((d0["D0_Close"] - scan_price) / scan_price * 100, 2)
                    pa.at[idx, "D0_Drop%"] = d0_drop

            time.sleep(0.3)  # rate limit

        updated += 1

    print(f"[Enrich] Enriched {updated} rows")

    # ── Save back to Sheets ─────────────────────────────────────────────────
    ws_pa = sh.worksheet("post_analysis")
    _df_to_sheet(ws_pa, pa)
    print("[Enrich] ✅ Saved to post_analysis")


if __name__ == "__main__":
    run()
