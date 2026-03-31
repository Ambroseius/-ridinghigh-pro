"""
RidingHigh Pro - Enrich Post Analysis with Intraday Data
Adds IntraHigh, IntraLow, PeakScoreTime, PeakScorePrice from timeline_live
"""

import sys
sys.path.insert(0, "/Users/adilevy/RidingHighPro")
from gsheets_sync import _get_client, SPREADSHEET_ID, load_post_analysis_from_sheets, _df_to_sheet
import pandas as pd

def run():
    print("[Enrich] Starting...")

    gc = _get_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    # Load timeline_live
    print("[Enrich] Loading timeline_live...")
    ws_tl = sh.worksheet("timeline_live")
    data = ws_tl.get_all_values()
    tl = pd.DataFrame(data[1:], columns=data[0])
    tl["Price"] = pd.to_numeric(tl["Price"], errors="coerce")
    tl["Score"] = pd.to_numeric(tl["Score"], errors="coerce")
    tl["Volume"] = pd.to_numeric(tl["Volume"], errors="coerce")
    print(f"[Enrich] Timeline rows: {len(tl)}")

    # Load post_analysis
    pa = load_post_analysis_from_sheets()
    print(f"[Enrich] Post analysis rows: {len(pa)}")

    # Enrich each row
    updated = 0
    for idx, row in pa.iterrows():
        ticker    = row["Ticker"]
        scan_date = row["ScanDate"]

        # Get all timeline rows for this ticker on scan date
        day_tl = tl[(tl["Ticker"] == ticker) & (tl["Date"] == scan_date)]

        if day_tl.empty:
            continue

        # Calculate intraday stats
        intra_high = round(day_tl["Price"].max(), 2)
        intra_low  = round(day_tl["Price"].min(), 2)

        # Peak score moment
        peak_idx   = day_tl["Score"].idxmax()
        peak_time  = day_tl.loc[peak_idx, "ScanTime"]
        peak_price = round(day_tl.loc[peak_idx, "Price"], 2)
        peak_score = round(day_tl.loc[peak_idx, "Score"], 2)

        # ScanPrice (open of day = first price in timeline)
        first_price = round(day_tl.sort_values("ScanTime").iloc[0]["Price"], 2)

        # RunUp = from first price to IntraHigh
        run_up_pct = round((intra_high - first_price) / first_price * 100, 2) if first_price > 0 else 0

        pa.at[idx, "IntraHigh"]      = intra_high
        pa.at[idx, "IntraLow"]       = intra_low
        pa.at[idx, "PeakScoreTime"]  = peak_time
        pa.at[idx, "PeakScorePrice"] = peak_price
        pa.at[idx, "PeakScore"]      = peak_score
        pa.at[idx, "DayRunUp%"]      = run_up_pct
        updated += 1

    print(f"[Enrich] Updated {updated} rows")

    # Save back to Sheets
    ws_pa = sh.worksheet("post_analysis")
    _df_to_sheet(ws_pa, pa)
    print("[Enrich] ✅ Saved to post_analysis")

if __name__ == "__main__":
    run()
