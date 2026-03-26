"""
RidingHigh Pro - Google Sheets Sync Module
Dual-save: saves locally AND to Google Sheets for cloud persistence
"""

import os
import json
import pandas as pd
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1oyefUPV52SMeAlC4UejECYoPRNRudJJS42rukNGYx5k"
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# Sheet tab names
TAB_PORTFOLIO      = "portfolio"
TAB_DAILY_SNAPSHOT = "daily_snapshots"
TAB_TIMELINE       = "timeline_archive"


def _get_client():
    """
    Returns an authorized gspread client.
    Works both locally (google_credentials.json file) and on Streamlit Cloud
    (st.secrets["gcp_service_account"]).
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        # ── Streamlit Cloud: secrets ─────────────────────────────────────
        try:
            import streamlit as st
            if "gcp_service_account" in st.secrets:
                creds = Credentials.from_service_account_info(
                    dict(st.secrets["gcp_service_account"]),
                    scopes=SCOPES
                )
                return gspread.authorize(creds)
        except Exception:
            pass

        # ── Local Mac: credentials file ──────────────────────────────────
        creds_path = os.path.expanduser("~/RidingHighPro/google_credentials.json")
        if os.path.exists(creds_path):
            creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
            return gspread.authorize(creds)

    except Exception as e:
        print(f"[GSheets] Auth error: {e}")

    return None


def _get_or_create_sheet(spreadsheet, tab_name):
    """Get existing worksheet or create it."""
    try:
        return spreadsheet.worksheet(tab_name)
    except Exception:
        return spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=30)


def _df_to_sheet(ws, df, include_index=False):
    """Write a DataFrame to a worksheet (full overwrite)."""
    if include_index:
        df = df.reset_index()
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.clear()
    ws.update(data)


# ── Public API ───────────────────────────────────────────────────────────────

def save_snapshot_to_sheets(df: pd.DataFrame) -> bool:
    """
    Save daily snapshot to Google Sheets.
    Adds a 'Date' column and appends rows (won't duplicate same-day entries).
    """
    try:
        gc = _get_client()
        if gc is None:
            return False

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_DAILY_SNAPSHOT)

        today = datetime.now().strftime("%Y-%m-%d")
        df = df.copy()
        df.insert(0, "Date", today)

        existing = ws.get_all_values()
        if len(existing) <= 1:
            # Empty sheet — write with header
            _df_to_sheet(ws, df)
        else:
            # Check if today already exists
            existing_df = pd.DataFrame(existing[1:], columns=existing[0])
            if today in existing_df.get("Date", pd.Series()).values:
                # Overwrite today's rows only
                other_days = existing_df[existing_df["Date"] != today]
                combined = pd.concat([other_days, df], ignore_index=True)
                _df_to_sheet(ws, combined)
            else:
                # Append
                rows = df.astype(str).values.tolist()
                ws.append_rows(rows)

        print(f"[GSheets] ✅ Snapshot saved for {today}")
        return True

    except Exception as e:
        print(f"[GSheets] snapshot error: {e}")
        return False


def save_timeline_to_sheets(df: pd.DataFrame, date: str = None) -> bool:
    """
    Save timeline archive to Google Sheets.
    Each date gets its own block of rows with a 'Date' column.
    """
    try:
        gc = _get_client()
        if gc is None:
            return False

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_TIMELINE)

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        df = df.copy()
        if df.index.name in ("Ticker", "index"):
            df = df.reset_index()
        df.insert(0, "Date", date)

        existing = ws.get_all_values()
        if len(existing) <= 1:
            _df_to_sheet(ws, df)
        else:
            existing_df = pd.DataFrame(existing[1:], columns=existing[0])
            other_days = existing_df[existing_df.get("Date", pd.Series()) != date] if "Date" in existing_df else existing_df
            combined = pd.concat([other_days, df], ignore_index=True)
            _df_to_sheet(ws, combined)

        print(f"[GSheets] ✅ Timeline saved for {date}")
        return True

    except Exception as e:
        print(f"[GSheets] timeline error: {e}")
        return False


def save_portfolio_to_sheets(df: pd.DataFrame) -> bool:
    """
    Full overwrite of the portfolio sheet.
    """
    try:
        gc = _get_client()
        if gc is None:
            return False

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_PORTFOLIO)
        _df_to_sheet(ws, df)

        print("[GSheets] ✅ Portfolio saved")
        return True

    except Exception as e:
        print(f"[GSheets] portfolio error: {e}")
        return False


def load_portfolio_from_sheets():
    """
    Load portfolio from Google Sheets (used on cloud where local file doesn't exist).
    """
    try:
        gc = _get_client()
        if gc is None:
            return None

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_PORTFOLIO)
        data = ws.get_all_values()

        if len(data) <= 1:
            return None

        df = pd.DataFrame(data[1:], columns=data[0])
        if df.empty:
            return None

        # Restore numeric types
        for col in ["Score", "BuyPrice", "CurrentPrice", "Change%", "P/L"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    except Exception as e:
        print(f"[GSheets] load portfolio error: {e}")
        return None


def load_timeline_dates_from_sheets() -> list:
    """Return list of dates available in timeline archive sheet."""
    try:
        gc = _get_client()
        if gc is None:
            return []

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_TIMELINE)
        data = ws.get_all_values()

        if len(data) <= 1:
            return []

        df = pd.DataFrame(data[1:], columns=data[0])
        if "Date" not in df.columns:
            return []

        return sorted(df["Date"].unique().tolist(), reverse=True)

    except Exception as e:
        print(f"[GSheets] load dates error: {e}")
        return []


def load_timeline_from_sheets(date: str):
    """Load a specific date's timeline from Google Sheets."""
    try:
        gc = _get_client()
        if gc is None:
            return None

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_TIMELINE)
        data = ws.get_all_values()

        if len(data) <= 1:
            return None

        df = pd.DataFrame(data[1:], columns=data[0])
        if "Date" not in df.columns:
            return None

        day_df = df[df["Date"] == date].drop(columns=["Date"])
        if day_df.empty:
            return None

        # Restore index
        if "Ticker" in day_df.columns:
            day_df = day_df.set_index("Ticker")

        # Restore numeric values
        for col in day_df.columns:
            day_df[col] = pd.to_numeric(day_df[col], errors="coerce")

        day_df = day_df.round(2)
        return day_df

    except Exception as e:
        print(f"[GSheets] load timeline error: {e}")
        return None

# Alias for compatibility
def get_gsheets_client():
    return _get_client()

# Alias for compatibility
def get_or_create_sheet(spreadsheet, tab_name):
    return _get_or_create_sheet(spreadsheet, tab_name)

# ── Post Analysis ────────────────────────────────────────────────────────────

TAB_POST_ANALYSIS = "post_analysis"

def save_post_analysis_to_sheets(df: pd.DataFrame) -> bool:
    """Save post-analysis results to Google Sheets."""
    try:
        gc = _get_client()
        if gc is None:
            return False

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_POST_ANALYSIS)

        existing = ws.get_all_values()
        if len(existing) <= 1:
            _df_to_sheet(ws, df)
        else:
            existing_df = pd.DataFrame(existing[1:], columns=existing[0])
            # Update rows that already exist (same Ticker+ScanDate), append new ones
            key = ["Ticker", "ScanDate"]
            if all(k in existing_df.columns for k in key) and all(k in df.columns for k in key):
                existing_df = existing_df[~existing_df.set_index(key).index.isin(df.set_index(key).index)]
            combined = pd.concat([existing_df, df], ignore_index=True)
            _df_to_sheet(ws, combined)

        print("[GSheets] ✅ Post analysis saved")
        return True

    except Exception as e:
        print(f"[GSheets] post analysis save error: {e}")
        return False


def load_post_analysis_from_sheets() -> pd.DataFrame:
    """Load post-analysis data from Google Sheets."""
    try:
        gc = _get_client()
        if gc is None:
            return pd.DataFrame()

        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = _get_or_create_sheet(sh, TAB_POST_ANALYSIS)
        data = ws.get_all_values()

        if len(data) <= 1:
            return pd.DataFrame()

        df = pd.DataFrame(data[1:], columns=data[0])

        numeric_cols = ["Score", "ScanPrice", "ScanChange%",
                        "D1_Open","D1_High","D1_Low","D1_Close",
                        "D2_Open","D2_High","D2_Low","D2_Close",
                        "D3_Open","D3_High","D3_Low","D3_Close",
                        "D4_Open","D4_High","D4_Low","D4_Close",
                        "D5_Open","D5_High","D5_Low","D5_Close",
                        "MaxDrop%","BestDay","TP10_Hit","TP15_Hit","TP20_Hit"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Round all numeric columns to 2 decimal places
        num_cols = df.select_dtypes(include="number").columns
        df[num_cols] = df[num_cols].round(2)
        return df

    except Exception as e:
        print(f"[GSheets] post analysis load error: {e}")
        return pd.DataFrame()
