#!/usr/bin/env python3
"""
RidingHigh Pro - Auto Scanner for GitHub Actions
Runs without Streamlit, saves directly to Google Sheets
"""

import os
import sys
import json
import time
import pytz
import pandas as pd
from datetime import datetime, time as dt_time

# ── Google Sheets setup ──────────────────────────────────────────────────────
SPREADSHEET_ID = "1oyefUPV52SMeAlC4UejECYoPRNRudJJS42rukNGYx5k"
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

PERU_TZ = pytz.timezone("America/Lima")

def get_peru_time():
    return datetime.now(PERU_TZ)

def is_market_hours():
    now = get_peru_time()
    market_open  = dt_time(8, 30)
    market_close = dt_time(15, 0)
    is_weekday   = now.weekday() < 5
    return is_weekday and market_open <= now.time() <= market_close

def is_snapshot_time():
    now = get_peru_time()
    return dt_time(14, 59) <= now.time() < dt_time(15, 0)

# ── Google Sheets client ─────────────────────────────────────────────────────
def get_gsheets_client():
    import gspread
    from google.oauth2.service_account import Credentials

    # GitHub Actions: credentials in env var
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)

    # Local Mac: credentials file
    creds_path = os.path.expanduser("~/RidingHighPro/google_credentials.json")
    if os.path.exists(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        return gspread.authorize(creds)

    raise Exception("No Google credentials found!")

def get_or_create_sheet(spreadsheet, tab_name):
    try:
        return spreadsheet.worksheet(tab_name)
    except Exception:
        return spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=30)

def df_to_sheet(ws, df):
    data = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.clear()
    ws.update(data)

# ── Market Cap cache ─────────────────────────────────────────────────────────
_mc_cache = {}

def load_mc_cache():
    global _mc_cache
    cache_path = os.path.expanduser("~/RidingHighPro/data/market_cap_cache.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                _mc_cache = json.load(f)
        except:
            _mc_cache = {}

def save_mc_cache():
    cache_path = os.path.expanduser("~/RidingHighPro/data/market_cap_cache.json")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    try:
        with open(cache_path, 'w') as f:
            json.dump(_mc_cache, f)
    except:
        pass

# ── Scanner logic (mirrors dashboard.py exactly) ─────────────────────────────
import yfinance as yf
from finvizfinance.screener.overview import Overview
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

_shares_cache = {}

def parse_market_cap(s):
    try:
        if pd.isna(s) or s == '-': return None
        s = str(s).replace(',', '')
        if 'B' in s: return float(s.replace('B','')) * 1_000_000_000
        if 'M' in s: return float(s.replace('M','')) * 1_000_000
        return float(s)
    except: return None

def parse_volume(s):
    try:
        if pd.isna(s) or s == '-': return None
        s = str(s).replace(',', '')
        if 'M' in s: return int(float(s.replace('M','')) * 1_000_000)
        if 'K' in s: return int(float(s.replace('K','')) * 1_000)
        return int(float(s))
    except: return None

def get_market_cap_smart(ticker, price, finviz_mc=None):
    if finviz_mc and finviz_mc > 0:
        _mc_cache[ticker] = int(finviz_mc)
        return int(finviz_mc)

    try:
        stock = yf.Ticker(ticker)
        mc = stock.info.get('marketCap', None)
        if mc and mc > 0:
            _mc_cache[ticker] = int(mc)
            return int(mc)
    except: pass

    try:
        if ticker not in _shares_cache:
            shares = yf.Ticker(ticker).info.get('sharesOutstanding', None)
            if shares and shares > 0:
                _shares_cache[ticker] = int(shares)
        if ticker in _shares_cache and price > 0:
            mc = int(_shares_cache[ticker] * price)
            _mc_cache[ticker] = mc
            return mc
    except: pass

    if ticker in _mc_cache:
        return _mc_cache[ticker]

    return None

def calculate_mxv(market_cap, price, volume):
    try:
        if market_cap == 0: return 0
        return ((market_cap - (price * volume)) / market_cap) * 100
    except: return 0

def calculate_score(metrics):
    score = 0
    try:
        if metrics['mxv'] < 0:
            score += min(abs(metrics['mxv']) / 50, 1) * 20
    except: pass
    try:
        if metrics['price_to_52w_high'] > 0:
            score += min(metrics['price_to_52w_high'] / 100, 1) * 10
    except: pass
    try:
        if metrics['price_to_high'] < 0:
            score += min(abs(metrics['price_to_high']) / 10, 1) * 15
    except: pass
    try:
        score += min(metrics['rel_vol'] / 2, 1) * 15
    except: pass
    try:
        if metrics['rsi'] > 80: score += 15
        else: score += (metrics['rsi'] / 80) * 15
    except: pass
    try:
        score += min(metrics['atrx'] / 15, 1) * 10
    except: pass
    try:
        if metrics['run_up'] < 0:
            score += min(abs(metrics['run_up']) / 5, 1) * 5
    except: pass
    try:
        score += min(metrics['float_pct'] / 10, 1) * 5
    except: pass
    try:
        score += min(abs(metrics['gap']) / 20, 1) * 3
    except: pass
    try:
        score += min(abs(metrics['vwap_dist']) / 15, 1) * 2
    except: pass
    return round(score, 2)

def analyze_ticker(ticker, finviz_row):
    try:
        price = float(finviz_row.get('Price', None))
        if pd.isna(price) or price < 2: return None

        change = float(finviz_row.get('Change', None))
        if pd.isna(change): return None
        change = change * 100

        volume = parse_volume(finviz_row.get('Volume', None))
        if not volume or volume == 0: return None

        finviz_mc = parse_market_cap(finviz_row.get('Market Cap', None))
        market_cap = get_market_cap_smart(ticker, price, finviz_mc)
        if not market_cap or market_cap == 0: return None

        rsi = 50; atrx = 0; rel_vol = 1.0; run_up = 0
        gap = 0; vwap_dist = 0; price_to_high = 0
        price_to_52w_high = 0; float_pct = 0
        shares_outstanding = _shares_cache.get(ticker, 0)

        try:
            stock = yf.Ticker(ticker)
            hist  = stock.history(period='60d')

            if not hist.empty and len(hist) >= 2:
                info    = stock.info
                current  = hist.iloc[-1]
                previous = hist.iloc[-2]

                if len(hist) >= 14:
                    try:
                        rsi_vals = RSIIndicator(close=hist['Close'], window=14).rsi()
                        if not rsi_vals.empty and not pd.isna(rsi_vals.iloc[-1]):
                            rsi = rsi_vals.iloc[-1]
                    except: pass

                    try:
                        atr_vals = AverageTrueRange(
                            high=hist['High'], low=hist['Low'],
                            close=hist['Close'], window=14
                        ).average_true_range()
                        atr = atr_vals.iloc[-1] if not atr_vals.empty else current['High'] - current['Low']
                        atrx = (atr / price) * 100 if price > 0 else 0
                    except: pass

                try:
                    avg_vol = info.get('averageVolume', volume)
                    rel_vol = volume / avg_vol if avg_vol > 0 else 1.0
                except: pass

                try:
                    run_up = ((price - current['Open']) / current['Open']) * 100
                except: pass

                try:
                    gap = ((current['Open'] - previous['Close']) / previous['Close']) * 100
                except: pass

                try:
                    vwap = (current['High'] + current['Low'] + price) / 3
                    vwap_dist = ((price / vwap) - 1) * 100 if vwap > 0 else 0
                except: pass

                try:
                    price_to_high = ((price - current['High']) / current['High']) * 100
                except: pass

                try:
                    h52 = info.get('fiftyTwoWeekHigh', price)
                    price_to_52w_high = ((price - h52) / h52) * 100 if h52 > 0 else 0
                except: pass

                if shares_outstanding == 0:
                    shares_outstanding = info.get('sharesOutstanding', int(market_cap / price) if price > 0 else 0)

                try:
                    float_pct = (volume / shares_outstanding * 100) if shares_outstanding > 0 else 0
                except: pass

        except: pass

        mxv = calculate_mxv(market_cap, price, volume)
        metrics = {
            'mxv': mxv, 'price_to_52w_high': price_to_52w_high,
            'price_to_high': price_to_high, 'rel_vol': rel_vol,
            'rsi': rsi, 'atrx': atrx, 'run_up': run_up,
            'float_pct': float_pct, 'gap': gap, 'vwap_dist': vwap_dist,
        }
        score = calculate_score(metrics)

        return {
            'Ticker': ticker, 'Price': round(price, 2),
            'Change': round(change, 2), 'Volume': int(volume),
            'MarketCap': int(market_cap),
            'MxV': round(mxv, 2), 'RunUp': round(run_up, 2),
            'PriceToHigh': round(price_to_high, 2),
            'PriceTo52WHigh': round(price_to_52w_high, 2),
            'RSI': round(rsi, 2), 'ATRX': round(atrx, 2),
            'REL_VOL': round(rel_vol, 2), 'Gap': round(gap, 2),
            'VWAP': round(vwap_dist, 2), 'Float%': round(float_pct, 2),
            'Score': round(score, 2),
        }
    except: return None

def fetch_finviz():
    try:
        fviz = Overview()
        fviz.set_filter(filters_dict={'Price': 'Over $2', 'Performance': 'Today +15%'})
        df = fviz.screener_view()
        if df is None or df.empty: return None
        return df.sort_values(by='Change', ascending=False)
    except Exception as e:
        print(f"FINVIZ error: {e}")
        return None

def run_scan():
    print(f"\n🚀 RidingHigh Pro Auto-Scanner")
    now_peru = get_peru_time()
    print(f"⏰ Peru time: {now_peru.strftime('%Y-%m-%d %H:%M:%S')}")

    if not is_market_hours():
        print("⛔ Outside market hours, skipping.")
        return

    load_mc_cache()

    print("🔍 Fetching FINVIZ...")
    finviz_df = fetch_finviz()
    if finviz_df is None:
        print("❌ No stocks from FINVIZ")
        return

    print(f"📊 Analyzing {len(finviz_df)} stocks...")
    results = []
    for idx, row in finviz_df.iterrows():
        ticker = row['Ticker']
        data = analyze_ticker(ticker, row)
        if data:
            results.append(data)
            print(f"  ✅ {ticker}: {data['Score']}")
        time.sleep(0.1)

    if not results:
        print("❌ No results")
        return

    results = sorted(results, key=lambda x: x['Score'], reverse=True)
    save_mc_cache()

    # ── Save to Google Sheets ────────────────────────────────────────────────
    try:
        gc = get_gsheets_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        scan_time = now_peru.strftime('%H:%M')
        today     = now_peru.strftime('%Y-%m-%d')

        # Timeline (every scan)
        ws_timeline = get_or_create_sheet(sh, "timeline_live")
        existing = ws_timeline.get_all_values()
        results_df = pd.DataFrame(results)

        if len(existing) <= 1:
            results_df.insert(0, 'ScanTime', scan_time)
            results_df.insert(0, 'Date', today)
            df_to_sheet(ws_timeline, results_df)
        else:
            ex_df = pd.DataFrame(existing[1:], columns=existing[0])
            new_rows = results_df.copy()
            new_rows.insert(0, 'ScanTime', scan_time)
            new_rows.insert(0, 'Date', today)
            combined = pd.concat([ex_df, new_rows], ignore_index=True)
            df_to_sheet(ws_timeline, combined)

        # Daily snapshot at 14:59
        if is_snapshot_time():
            ws_snap = get_or_create_sheet(sh, "daily_snapshots")
            snap_df = results_df.copy()
            snap_df.insert(0, 'Date', today)
            existing_snap = ws_snap.get_all_values()
            if len(existing_snap) <= 1:
                df_to_sheet(ws_snap, snap_df)
            else:
                ex_snap = pd.DataFrame(existing_snap[1:], columns=existing_snap[0])
                other = ex_snap[ex_snap['Date'] != today]
                combined_snap = pd.concat([other, snap_df], ignore_index=True)
                df_to_sheet(ws_snap, combined_snap)
            print("📸 Daily snapshot saved!")

        print(f"✅ Saved {len(results)} stocks to Google Sheets at {scan_time}")

    except Exception as e:
        print(f"❌ Google Sheets error: {e}")

if __name__ == "__main__":
    run_scan()
