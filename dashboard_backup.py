#!/usr/bin/env python3
"""
RidingHigh Pro v14.1 - PORTFOLIO COLORS FIXED!
- Portfolio Tracker: Only Change% and P/L columns are colored
- All other columns remain white
- Green for profit, Red for loss
"""

import streamlit as st
import pandas as pd
import time
import plotly.express as px
from finvizfinance.screener.overview import Overview
from datetime import datetime, time as dt_time
import pytz
import pytz
from data_logger import DataLogger
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
import os
import shutil
from gsheets_sync import save_snapshot_to_sheets, save_timeline_to_sheets, save_portfolio_to_sheets, load_portfolio_from_sheets, load_timeline_dates_from_sheets, load_timeline_from_sheets
import json

st.set_page_config(
    page_title="RidingHigh Pro v14.1",
    page_icon="🚀",
    layout="wide"
)

st.markdown("""
<style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0.5rem;
    }
    h1 {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
        font-size: 1.8rem;
        margin-top: 0;
    }
    h2 {
        padding-top: 0.3rem;
        padding-bottom: 0.3rem;
        font-size: 1.2rem;
    }
    div[data-testid="metric-container"] {
        padding: 5px;
    }
</style>
""", unsafe_allow_html=True)

class Dashboard:
    
    def __init__(self):
        self.logger = DataLogger()
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.finviz_df = None
        
        self.cache_dir = os.path.expanduser("~/RidingHighPro/data")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, "market_cap_cache.json")
        
        self.market_cap_cache = {}
        self.shares_cache = {}
    
    def load_from_cache_file(self, ticker):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    if ticker in cache_data:
                        return cache_data[ticker].get('market_cap', None)
            return None
        except:
            return None
    
    def save_to_cache_file(self, ticker, market_cap):
        try:
            cache_data = {}
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
            
            cache_data[ticker] = {
                'market_cap': int(market_cap),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            return True
        except:
            return False
    
    def get_from_history_all_days(self, ticker, field):
        try:
            dates = self.logger.get_all_dates()
            
            if not dates:
                return None
            
            for date in dates:
                try:
                    df = self.logger.load_date(date)
                    
                    if df is None or df.empty:
                        continue
                    
                    if 'Ticker' not in df.columns:
                        continue
                    
                    matching_rows = df[df['Ticker'] == ticker]
                    
                    if matching_rows.empty:
                        continue
                    
                    row = matching_rows.iloc[-1]
                    
                    if field in row and pd.notna(row[field]) and row[field] != 0:
                        return row[field]
                except:
                    continue
            
            return None
        except:
            return None
    
    def fetch_finviz_data(self):
        try:
            fviz = Overview()
            filters_dict = {
                'Price': 'Over $2',
                'Performance': 'Today +15%',
            }
            fviz.set_filter(filters_dict=filters_dict)
            
            df = fviz.screener_view()
            
            if df is None or df.empty:
                return None
            
            df_sorted = df.sort_values(by='Change', ascending=False)
            self.finviz_df = df_sorted
            
            return self.finviz_df
            
        except Exception as e:
            return None
    
    def parse_market_cap(self, market_cap_str):
        try:
            if pd.isna(market_cap_str) or market_cap_str == '-':
                return None
            
            market_cap_str = str(market_cap_str).replace(',', '')
            
            if 'B' in market_cap_str:
                return float(market_cap_str.replace('B', '')) * 1_000_000_000
            elif 'M' in market_cap_str:
                return float(market_cap_str.replace('M', '')) * 1_000_000
            else:
                return float(market_cap_str)
        except:
            return None
    
    def parse_volume(self, volume_str):
        try:
            if pd.isna(volume_str) or volume_str == '-':
                return None
            
            volume_str = str(volume_str).replace(',', '')
            
            if 'M' in volume_str:
                return int(float(volume_str.replace('M', '')) * 1_000_000)
            elif 'K' in volume_str:
                return int(float(volume_str.replace('K', '')) * 1_000)
            else:
                return int(float(volume_str))
        except:
            return None
    
    def get_market_cap_smart(self, ticker, price, finviz_mc=None):
        market_cap = None
        
        if finviz_mc and finviz_mc > 0:
            market_cap = int(finviz_mc)
            self.market_cap_cache[ticker] = market_cap
            self.save_to_cache_file(ticker, market_cap)
            return market_cap
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            mc = info.get('marketCap', None)
            if mc and mc > 0:
                market_cap = int(mc)
                self.market_cap_cache[ticker] = market_cap
                self.save_to_cache_file(ticker, market_cap)
                return market_cap
        except:
            pass
        
        try:
            if ticker not in self.shares_cache:
                stock = yf.Ticker(ticker)
                info = stock.info
                shares = info.get('sharesOutstanding', None)
                if shares and shares > 0:
                    self.shares_cache[ticker] = int(shares)
            
            if ticker in self.shares_cache:
                shares = self.shares_cache[ticker]
                if shares > 0 and price > 0:
                    market_cap = int(shares * price)
                    self.market_cap_cache[ticker] = market_cap
                    self.save_to_cache_file(ticker, market_cap)
                    return market_cap
        except:
            pass
        
        try:
            hist_mc = self.get_from_history_all_days(ticker, 'MarketCap')
            if hist_mc and hist_mc > 0:
                market_cap = int(hist_mc)
                self.market_cap_cache[ticker] = market_cap
                self.save_to_cache_file(ticker, market_cap)
                return market_cap
        except:
            pass
        
        try:
            cached_mc = self.load_from_cache_file(ticker)
            if cached_mc and cached_mc > 0:
                market_cap = int(cached_mc)
                self.market_cap_cache[ticker] = market_cap
                return market_cap
        except:
            pass
        
        return None
    
    def preload_market_caps(self, finviz_df, progress_callback=None):
        if finviz_df is None or finviz_df.empty:
            return
        
        total = len(finviz_df)
        
        for idx, row in finviz_df.iterrows():
            ticker = row['Ticker']
            
            if progress_callback:
                progress_callback(idx + 1, total, ticker)
            
            finviz_mc = self.parse_market_cap(row.get('Market Cap', None))
            price = float(row.get('Price', 0))
            
            market_cap = self.get_market_cap_smart(ticker, price, finviz_mc)
            
            time.sleep(0.3)
    
    def analyze_ticker_from_yahoo(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='5d')
            
            if hist.empty or len(hist) < 2:
                return None
            
            info = stock.info
            current = hist.iloc[-1]
            previous = hist.iloc[-2]
            
            price = current['Close']
            change = ((current['Close'] - previous['Close']) / previous['Close']) * 100
            volume = current['Volume']
            
            market_cap = self.get_market_cap_smart(ticker, price)
            
            if market_cap is None or market_cap == 0:
                return None
            
            rsi = 50
            atr = 0
            atrx = 0
            rel_vol = 1.0
            run_up = 0
            gap = 0
            vwap_dist = 0
            price_to_high = 0
            price_to_52w_high = 0
            shares_outstanding = self.shares_cache.get(ticker, 0)
            float_pct = 0
            
            full_hist = stock.history(period='60d')
            
            if not full_hist.empty and len(full_hist) >= 2:
                try:
                    if len(full_hist) >= 14:
                        rsi_indicator = RSIIndicator(close=full_hist['Close'], window=14)
                        rsi_values = rsi_indicator.rsi()
                        if not rsi_values.empty and not pd.isna(rsi_values.iloc[-1]):
                            rsi = rsi_values.iloc[-1]
                except:
                    rsi = 50
                
                try:
                    if len(full_hist) >= 14:
                        atr_indicator = AverageTrueRange(
                            high=full_hist['High'],
                            low=full_hist['Low'],
                            close=full_hist['Close'],
                            window=14
                        )
                        atr_values = atr_indicator.average_true_range()
                        if not atr_values.empty and not pd.isna(atr_values.iloc[-1]):
                            atr = atr_values.iloc[-1]
                        else:
                            atr = current['High'] - current['Low']
                    else:
                        atr = current['High'] - current['Low']
                    atrx = (atr / price) * 100 if price > 0 else 0
                except:
                    atr = 0
                    atrx = 0
                
                try:
                    avg_volume = info.get('averageVolume', volume)
                    if avg_volume > 0:
                        rel_vol = volume / avg_volume
                    else:
                        if len(full_hist) >= 20:
                            avg_vol_20 = full_hist['Volume'].tail(20).mean()
                        else:
                            avg_vol_20 = full_hist['Volume'].mean()
                        rel_vol = volume / avg_vol_20 if avg_vol_20 > 0 else 1.0
                except:
                    rel_vol = 1.0
                
                try:
                    if current['Open'] > 0:
                        run_up = ((price - current['Open']) / current['Open']) * 100
                except:
                    run_up = 0
                
                try:
                    gap = ((current['Open'] - previous['Close']) / previous['Close']) * 100
                except:
                    gap = 0
                
                try:
                    vwap = (current['High'] + current['Low'] + price) / 3
                    vwap_dist = ((price / vwap) - 1) * 100 if vwap > 0 else 0
                except:
                    vwap_dist = 0
                
                try:
                    high_today = current['High']
                    price_to_high = ((price - high_today) / high_today) * 100 if high_today > 0 else 0
                except:
                    price_to_high = 0
                
                try:
                    high_52w = info.get('fiftyTwoWeekHigh', price)
                    price_to_52w_high = ((price - high_52w) / high_52w) * 100 if high_52w > 0 else 0
                except:
                    price_to_52w_high = 0
                
                if shares_outstanding == 0:
                    try:
                        shares_outstanding = info.get('sharesOutstanding', 0)
                        if shares_outstanding == 0:
                            shares_outstanding = int(market_cap / price) if price > 0 else 0
                    except:
                        shares_outstanding = int(market_cap / price) if price > 0 else 0
                
                try:
                    float_pct = (volume / shares_outstanding * 100) if shares_outstanding > 0 else 0
                except:
                    float_pct = 0
            
            mxv = self.calculate_mxv(market_cap, price, volume)
            
            metrics = {
                'mxv': mxv,
                'price_to_52w_high': price_to_52w_high,
                'price_to_high': price_to_high,
                'rel_vol': rel_vol,
                'rsi': rsi,
                'atrx': atrx,
                'run_up': run_up,
                'float_pct': float_pct,
                'gap': gap,
                'vwap_dist': vwap_dist,
            }
            
            score = self.calculate_score(metrics)
            
            return {
                'Ticker': ticker,
                'Price': round(price, 2),
                'Change': round(change, 2),
                'Volume': int(volume),
                'MarketCap': int(market_cap),
                'SharesOutstanding': int(shares_outstanding) if shares_outstanding > 0 else 0,
                'MxV': round(mxv, 2),
                'RunUp': round(run_up, 2),
                'PriceToHigh': round(price_to_high, 2),
                'PriceTo52WHigh': round(price_to_52w_high, 2),
                'RSI': round(rsi, 2),
                'ATRX': round(atrx, 2),
                'REL_VOL': round(rel_vol, 2),
                'Gap': round(gap, 2),
                'VWAP': round(vwap_dist, 2),
                'Float%': round(float_pct, 2),
                'Score': round(score, 2),
            }
            
        except Exception as e:
            return None
    
    def analyze_ticker_complete(self, ticker, finviz_row):
        try:
            price = finviz_row.get('Price', None)
            if pd.isna(price) or price is None:
                return None
            price = float(price)
            
            if price < 2:
                return None
            
            change = finviz_row.get('Change', None)
            if pd.isna(change):
                return None
            change = float(change) * 100
            
            volume = self.parse_volume(finviz_row.get('Volume', None))
            if volume is None or volume == 0:
                return None
            
            finviz_mc = self.parse_market_cap(finviz_row.get('Market Cap', None))
            market_cap = self.get_market_cap_smart(ticker, price, finviz_mc)
            
            if market_cap is None or market_cap == 0:
                return None
            
            rsi = 50
            atr = 0
            atrx = 0
            rel_vol = 1.0
            run_up = 0
            gap = 0
            vwap_dist = 0
            price_to_high = 0
            price_to_52w_high = 0
            shares_outstanding = self.shares_cache.get(ticker, 0)
            float_pct = 0
            
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period='60d')
                
                if not hist.empty and len(hist) >= 2:
                    info = stock.info
                    current = hist.iloc[-1]
                    previous = hist.iloc[-2]
                    
                    try:
                        if len(hist) >= 14:
                            rsi_indicator = RSIIndicator(close=hist['Close'], window=14)
                            rsi_values = rsi_indicator.rsi()
                            if not rsi_values.empty and not pd.isna(rsi_values.iloc[-1]):
                                rsi = rsi_values.iloc[-1]
                    except:
                        rsi = 50
                    
                    try:
                        if len(hist) >= 14:
                            atr_indicator = AverageTrueRange(
                                high=hist['High'],
                                low=hist['Low'],
                                close=hist['Close'],
                                window=14
                            )
                            atr_values = atr_indicator.average_true_range()
                            if not atr_values.empty and not pd.isna(atr_values.iloc[-1]):
                                atr = atr_values.iloc[-1]
                            else:
                                atr = current['High'] - current['Low']
                        else:
                            atr = current['High'] - current['Low']
                        atrx = (atr / price) * 100 if price > 0 else 0
                    except:
                        atr = 0
                        atrx = 0
                    
                    try:
                        avg_volume = info.get('averageVolume', volume)
                        if avg_volume > 0:
                            rel_vol = volume / avg_volume
                        else:
                            if len(hist) >= 20:
                                avg_vol_20 = hist['Volume'].tail(20).mean()
                            else:
                                avg_vol_20 = hist['Volume'].mean()
                            rel_vol = volume / avg_vol_20 if avg_vol_20 > 0 else 1.0
                    except:
                        rel_vol = 1.0
                    
                    try:
                        if current['Open'] > 0:
                            run_up = ((price - current['Open']) / current['Open']) * 100
                    except:
                        run_up = 0
                    
                    try:
                        gap = ((current['Open'] - previous['Close']) / previous['Close']) * 100
                    except:
                        gap = 0
                    
                    try:
                        vwap = (current['High'] + current['Low'] + price) / 3
                        vwap_dist = ((price / vwap) - 1) * 100 if vwap > 0 else 0
                    except:
                        vwap_dist = 0
                    
                    try:
                        high_today = current['High']
                        price_to_high = ((price - high_today) / high_today) * 100 if high_today > 0 else 0
                    except:
                        price_to_high = 0
                    
                    try:
                        high_52w = info.get('fiftyTwoWeekHigh', price)
                        price_to_52w_high = ((price - high_52w) / high_52w) * 100 if high_52w > 0 else 0
                    except:
                        price_to_52w_high = 0
                    
                    if shares_outstanding == 0:
                        try:
                            shares_outstanding = info.get('sharesOutstanding', 0)
                            if shares_outstanding == 0:
                                shares_outstanding = int(market_cap / price) if price > 0 else 0
                        except:
                            shares_outstanding = int(market_cap / price) if price > 0 else 0
                    
                    try:
                        float_pct = (volume / shares_outstanding * 100) if shares_outstanding > 0 else 0
                    except:
                        float_pct = 0
            
            except:
                if shares_outstanding == 0:
                    shares_outstanding = int(market_cap / price) if price > 0 else 0
            
            mxv = self.calculate_mxv(market_cap, price, volume)
            
            metrics = {
                'mxv': mxv,
                'price_to_52w_high': price_to_52w_high,
                'price_to_high': price_to_high,
                'rel_vol': rel_vol,
                'rsi': rsi,
                'atrx': atrx,
                'run_up': run_up,
                'float_pct': float_pct,
                'gap': gap,
                'vwap_dist': vwap_dist,
            }
            
            score = self.calculate_score(metrics)
            
            return {
                'Ticker': ticker,
                'Price': round(price, 2),
                'Change': round(change, 2),
                'Volume': int(volume),
                'MarketCap': int(market_cap),
                'SharesOutstanding': int(shares_outstanding) if shares_outstanding > 0 else 0,
                'MxV': round(mxv, 2),
                'RunUp': round(run_up, 2),
                'PriceToHigh': round(price_to_high, 2),
                'PriceTo52WHigh': round(price_to_52w_high, 2),
                'RSI': round(rsi, 2),
                'ATRX': round(atrx, 2),
                'REL_VOL': round(rel_vol, 2),
                'Gap': round(gap, 2),
                'VWAP': round(vwap_dist, 2),
                'Float%': round(float_pct, 2),
                'Score': round(score, 2),
            }
            
        except Exception as e:
            return None
    
    def scan(self, tracked_tickers=None, progress_callback=None, skip_preload=False):
        finviz_df = self.fetch_finviz_data()
        
        if finviz_df is None or finviz_df.empty:
            return []
        
        if not skip_preload:
            self.preload_market_caps(finviz_df, progress_callback)
        
        results = []
        scanned_tickers = set()
        
        for idx, row in finviz_df.iterrows():
            ticker = row['Ticker']
            scanned_tickers.add(ticker)
            data = self.analyze_ticker_complete(ticker, row)
            if data:
                results.append(data)
            
            time.sleep(0.1)
        
        if tracked_tickers:
            missing_tickers = tracked_tickers - scanned_tickers
            
            for ticker in missing_tickers:
                data = self.analyze_ticker_from_yahoo(ticker)
                if data:
                    results.append(data)
                
                time.sleep(0.2)
        
        return sorted(results, key=lambda x: x['Score'], reverse=True)
    
    def calculate_mxv(self, market_cap, price, volume):
        try:
            if market_cap == 0:
                return 0
            return ((market_cap - (price * volume)) / market_cap) * 100
        except:
            return 0
    
    def calculate_score(self, metrics):
        score = 0
        
        try:
            if metrics['mxv'] < 0:
                mxv_ratio = min(abs(metrics['mxv']) / 50, 1)
                score += mxv_ratio * 20
        except:
            pass
        
        try:
            if metrics['price_to_52w_high'] > 0:
                p52w_ratio = min(metrics['price_to_52w_high'] / 100, 1)
                score += p52w_ratio * 10
        except:
            pass
        
        try:
            if metrics['price_to_high'] < 0:
                pth_ratio = min(abs(metrics['price_to_high']) / 10, 1)
                score += pth_ratio * 15
        except:
            pass
        
        try:
            rel_vol_ratio = min(metrics['rel_vol'] / 2, 1)
            score += rel_vol_ratio * 15
        except:
            pass
        
        try:
            if metrics['rsi'] > 80:
                score += 15
            else:
                score += (metrics['rsi'] / 80) * 15
        except:
            pass
        
        try:
            atrx_ratio = min(metrics['atrx'] / 15, 1)
            score += atrx_ratio * 10
        except:
            pass
        
        try:
            if metrics['run_up'] < 0:
                runup_ratio = min(abs(metrics['run_up']) / 5, 1)
                score += runup_ratio * 5
        except:
            pass
        
        try:
            float_ratio = min(metrics['float_pct'] / 10, 1)
            score += float_ratio * 5
        except:
            pass
        
        try:
            gap_ratio = min(abs(metrics['gap']) / 20, 1)
            score += gap_ratio * 3
        except:
            pass
        
        try:
            vwap_ratio = min(abs(metrics['vwap_dist']) / 15, 1)
            score += vwap_ratio * 2
        except:
            pass
        
        return round(score, 2)

class LiveTracker:
    
    def __init__(self):
        self.tracker_dir = os.path.expanduser("~/RidingHighPro/data/live_tracker")
        self.archive_dir = os.path.expanduser("~/RidingHighPro/data/timeline_archive")
        self.snapshot_dir = os.path.expanduser("~/RidingHighPro/data/daily_snapshots")
        
        os.makedirs(self.tracker_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        os.makedirs(self.snapshot_dir, exist_ok=True)
        
        self.today_file = os.path.join(
            self.tracker_dir,
            f"tracker_{datetime.now().strftime('%Y-%m-%d')}.csv"
        )
    
    def get_tracked_tickers(self):
        if not os.path.exists(self.today_file):
            return set()
        
        try:
            df = pd.read_csv(self.today_file, index_col=0)
            return set(df.index.tolist())
        except:
            return set()
    
    def add_minute_data(self, results, scan_time):
        if not results:
            return 0
        
        filtered = []
        for r in results:
            try:
                score = float(r['Score'])
                if not pd.isna(score):
                    filtered.append(r)
            except:
                continue
        
        if not filtered:
            return 0
        
        current_time = scan_time.strftime('%H:%M')
        
        if os.path.exists(self.today_file):
            try:
                df = pd.read_csv(self.today_file, index_col=0)
                if df.index.name == 'index':
                    df.index.name = 'Ticker'
            except:
                df = pd.DataFrame()
                df.index.name = 'Ticker'
        else:
            df = pd.DataFrame()
            df.index.name = 'Ticker'
        
        for r in filtered:
            ticker = r['Ticker']
            score = round(r['Score'], 2)
            df.loc[ticker, current_time] = score
        
        df.to_csv(self.today_file)
        
        return len(filtered)
    
    def save_daily_snapshot(self, results):
        if not results:
            return False
        
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{today}.csv")
            
            df = pd.DataFrame(results)
            df.to_csv(snapshot_file, index=False)
            save_snapshot_to_sheets(df)
            
            return True
        except:
            return False
    
    def get_today_grid(self):
        if not os.path.exists(self.today_file):
            return None
        
        try:
            df = pd.read_csv(self.today_file, index_col=0)
            
            if df.empty:
                return None
            
            if df.index.name == 'index':
                df.index.name = 'Ticker'
            
            columns = sorted(df.columns, reverse=True)
            df = df[columns]
            
            df = df.round(2)
            
            return df
        except Exception as e:
            return None
    
    def archive_today(self):
        if not os.path.exists(self.today_file):
            return False
        
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            archive_file = os.path.join(self.archive_dir, f"timeline_{today}.csv")
            
            shutil.copy2(self.today_file, archive_file)
            archive_df = pd.read_csv(self.today_file, index_col=0)
            save_timeline_to_sheets(archive_df, today)
            return True
        except:
            return False
    
    def get_archive_dates(self):
        try:
            files = os.listdir(self.archive_dir)
            dates = []
            
            for f in files:
                if f.startswith('timeline_') and f.endswith('.csv'):
                    date_str = f.replace('timeline_', '').replace('.csv', '')
                    dates.append(date_str)
            
            return sorted(dates, reverse=True)
        except:
            return []
    
    def load_archive(self, date):
        try:
            archive_file = os.path.join(self.archive_dir, f"timeline_{date}.csv")
            
            if not os.path.exists(archive_file):
                return None
            
            df = pd.read_csv(archive_file, index_col=0)
            
            if df.empty:
                return None
            
            if df.index.name == 'index':
                df.index.name = 'Ticker'
            
            columns = sorted(df.columns, reverse=True)
            df = df[columns]
            
            df = df.round(2)
            
            return df
        except:
            return None

class PortfolioTracker:
    
    def __init__(self):
        self.portfolio_dir = os.path.expanduser("~/RidingHighPro/data/portfolio")
        os.makedirs(self.portfolio_dir, exist_ok=True)
        
        self.portfolio_file = os.path.join(self.portfolio_dir, "portfolio.csv")
    
    def add_positions(self, results, date):
        """Add stocks with score >= 60 to portfolio"""
        if not results:
            return 0
        
        high_score_stocks = [r for r in results if r['Score'] >= 60]
        
        if not high_score_stocks:
            return 0
        
        if os.path.exists(self.portfolio_file):
            try:
                existing_df = pd.read_csv(self.portfolio_file)
            except:
                existing_df = pd.DataFrame()
        else:
            existing_df = pd.DataFrame()
        
        new_positions = []
        for stock in high_score_stocks:
            position_key = f"{stock['Ticker']}_{date}"
            
            if not existing_df.empty and position_key in existing_df['PositionKey'].values:
                continue
            
            new_positions.append({
                'PositionKey': position_key,
                'Date': date,
                'Ticker': stock['Ticker'],
                'Score': round(stock['Score'], 2),
                'BuyPrice': round(stock['Price'], 2),
                'Status': 'Open'
            })
        
        if new_positions:
            new_df = pd.DataFrame(new_positions)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.to_csv(self.portfolio_file, index=False)
            save_portfolio_to_sheets(combined_df)
            return len(new_positions)
        
        return 0
    
    def get_portfolio_with_current_prices(self):
        """Get portfolio with current prices"""
        if not os.path.exists(self.portfolio_file):
            return None
        
        try:
            df = pd.read_csv(self.portfolio_file)
            
            if df.empty:
                return None
            
            df['CurrentPrice'] = 0.0
            df['Change%'] = 0.0
            df['P/L'] = 0.0
            
            for idx, row in df.iterrows():
                ticker = row['Ticker']
                buy_price = row['BuyPrice']
                
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(period='1d')
                    
                    if not hist.empty:
                        current_price = hist.iloc[-1]['Close']
                        df.at[idx, 'CurrentPrice'] = round(current_price, 2)
                        
                        change_pct = ((current_price - buy_price) / buy_price) * 100
                        df.at[idx, 'Change%'] = round(change_pct, 2)
                        
                        pl = current_price - buy_price
                        df.at[idx, 'P/L'] = round(pl, 2)
                except:
                    df.at[idx, 'CurrentPrice'] = buy_price
                    df.at[idx, 'Change%'] = 0.0
                    df.at[idx, 'P/L'] = 0.0
                
                time.sleep(0.1)
            
            df = df.sort_values(by='Date', ascending=False)
            
            return df
        except:
            return None
    
    def close_position(self, position_key):
        """Close a position"""
        if not os.path.exists(self.portfolio_file):
            return False
        
        try:
            df = pd.read_csv(self.portfolio_file)
            df.loc[df['PositionKey'] == position_key, 'Status'] = 'Closed'
            df.to_csv(self.portfolio_file, index=False)
            return True
        except:
            return False
    
    def delete_position(self, position_key):
        """Delete a position"""
        if not os.path.exists(self.portfolio_file):
            return False
        
        try:
            df = pd.read_csv(self.portfolio_file)
            df = df[df['PositionKey'] != position_key]
            df.to_csv(self.portfolio_file, index=False)
            return True
        except:
            return False

def is_market_hours():
    peru_tz = pytz.timezone("America/Lima")
    now = datetime.now(peru_tz)
    market_open = dt_time(8, 30)
    market_close = dt_time(15, 0)
    current_time = now.time()
    is_weekday = now.weekday() < 5
    return is_weekday and market_open <= current_time <= market_close

def check_snapshot_time():
    peru_tz = pytz.timezone("America/Lima")
    now = datetime.now(peru_tz)
    snapshot_time = dt_time(14, 59)
    current_time = now.time()
    
    return snapshot_time <= current_time < dt_time(15, 0)

def main_page():
    st.title("🚀 RidingHigh Pro v14.1")
    st.caption("Portfolio Tracker - Auto-saves stocks with score 60+ at 14:59")
    
    if 'dashboard' not in st.session_state:
        st.session_state.dashboard = Dashboard()
    
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'last_scan' not in st.session_state:
        st.session_state.last_scan = None
    if 'force_scan' not in st.session_state:
        st.session_state.force_scan = False
    if 'preload_done' not in st.session_state:
        st.session_state.preload_done = False
    if 'snapshot_done_today' not in st.session_state:
        st.session_state.snapshot_done_today = False
    if 'portfolio_saved_today' not in st.session_state:
        st.session_state.portfolio_saved_today = False
    
    st.sidebar.header("⚙️ Settings")
    
    auto_scan = st.sidebar.checkbox("🔄 Auto-Scan (every minute)", value=is_market_hours())
    
    if st.sidebar.button("🔄 Scan Now", type="primary"):
        st.session_state.force_scan = True
    
    if st.sidebar.button("🗑️ Clear Timeline"):
        tracker = LiveTracker()
        if os.path.exists(tracker.today_file):
            os.remove(tracker.today_file)
            st.sidebar.success("✅ Cleared!")
            st.rerun()
    
    if check_snapshot_time() and not st.session_state.snapshot_done_today:
        tracker = LiveTracker()
        portfolio = PortfolioTracker()
        
        if st.session_state.results:
            if tracker.save_daily_snapshot(st.session_state.results):
                st.sidebar.success("📸 Daily snapshot saved!")
            
            if not st.session_state.portfolio_saved_today:
                today = datetime.now().strftime('%Y-%m-%d')
                added = portfolio.add_positions(st.session_state.results, today)
                if added > 0:
                    st.sidebar.success(f"💼 Added {added} stocks to portfolio!")
                    st.session_state.portfolio_saved_today = True
        
        if tracker.archive_today():
            st.sidebar.success("📦 Timeline archived!")
        
        st.session_state.snapshot_done_today = True
    
    now = datetime.now()
    if now.hour == 0 and now.minute < 5:
        st.session_state.snapshot_done_today = False
        st.session_state.portfolio_saved_today = False
    
    should_scan = False
    
    if st.session_state.force_scan:
        should_scan = True
        st.session_state.force_scan = False
    elif auto_scan:
        if st.session_state.last_scan is None:
            should_scan = True
        else:
            time_diff = (datetime.now() - st.session_state.last_scan).total_seconds()
            if time_diff >= 60:
                should_scan = True
    
    if should_scan:
        scan_start_time = datetime.now()
        
        with st.sidebar:
            progress_placeholder = st.empty()
            
            def show_progress(current, total, ticker):
                progress_placeholder.info(f"🔍 Loading {ticker} ({current}/{total})")
            
            with st.spinner("🔍 Scanning..."):
                tracker = LiveTracker()
                
                tracked_tickers = tracker.get_tracked_tickers()
                
                skip_preload = st.session_state.preload_done
                
                results = st.session_state.dashboard.scan(
                    tracked_tickers=tracked_tickers,
                    progress_callback=show_progress if not skip_preload else None,
                    skip_preload=skip_preload
                )
                
                st.session_state.results = results
                st.session_state.last_scan = scan_start_time
                st.session_state.preload_done = True
                
                progress_placeholder.empty()
                
                if results:
                    logger = DataLogger()
                    logger.save_daily_snapshot(results)
                    
                    added_count = tracker.add_minute_data(results, scan_start_time)
                    
                    st.success(f"✅ {len(results)} stocks tracked!")
                else:
                    st.warning("⚠️ No stocks")
                
                time.sleep(1)
        st.rerun()
    
    st.subheader("📊 TABLE 1: Live Scanner")
    
    if st.session_state.results:
        results = st.session_state.results
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total", len(results))
        
        with col2:
            critical = len([r for r in results if r['Score'] >= 85])
            st.metric("🔥 Critical", critical)
        
        with col3:
            high = len([r for r in results if 60 <= r['Score'] < 85])
            st.metric("⚠️ High", high)
        
        with col4:
            if st.session_state.last_scan:
                st.metric("Last Scan", st.session_state.last_scan.strftime("%H:%M:%S"))
        
        display_data = []
        for r in results:
            display_data.append({
                'Ticker': r['Ticker'],
                'Score': f"{r['Score']:.2f}",
                'Price': f"${r['Price']:.2f}",
                'Change': f"{r['Change']:+.1f}%",
                'MxV': f"{r['MxV']:.1f}%",
                'P2-52W': f"{r['PriceTo52WHigh']:+.1f}%",
                'P2High': f"{r['PriceToHigh']:.1f}%",
                'RSI': f"{r['RSI']:.1f}",
                'ATRX': f"{r['ATRX']:.1f}",
                'REL VOL': f"{r['REL_VOL']:.1f}x",
                'RunUp': f"{r['RunUp']:+.1f}%",
                'Float%': f"{r['Float%']:.2f}%",
                'Gap': f"{r['Gap']:+.1f}%",
                'VWAP': f"{r['VWAP']:+.1f}%",
            })
        
        df = pd.DataFrame(display_data)
        
        def highlight_score(row):
            score = float(row['Score'])
            if score >= 85:
                return ['background-color: #800020; color: white; font-weight: bold'] * len(row)
            elif score >= 60:
                return ['background-color: #cc0000; color: white'] * len(row)
            elif score >= 40:
                return ['background-color: #ff6600; color: white'] * len(row)
            else:
                return ['background-color: #ffcc00; color: black'] * len(row)
        
        styled_df = df.style.apply(highlight_score, axis=1)
        
        table_height = min(600, len(df) * 40 + 50)
        
        st.dataframe(styled_df, use_container_width=True, hide_index=True, height=table_height)
        
    else:
        st.info("👈 Click 'Scan Now' or enable Auto-Scan")
    
    st.markdown("---")
    st.subheader("⏱️ TABLE 2: Timeline Grid")
    
    tracker = LiveTracker()
    df_timeline = tracker.get_today_grid()
    
    if df_timeline is None or df_timeline.empty:
        st.info("📊 No timeline data yet")
    else:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Tracked", len(df_timeline))
        
        with col2:
            st.metric("Time Points", len(df_timeline.columns))
        
        with col3:
            if auto_scan and st.session_state.last_scan:
                time_since = (datetime.now() - st.session_state.last_scan).total_seconds()
                time_left = max(0, 60 - time_since)
                st.metric("Next", f"{int(time_left)}s")
        
        def color_score(val):
            try:
                score = float(val)
                if score >= 85:
                    return 'background-color: #800020; color: white; font-weight: bold'
                elif score >= 60:
                    return 'background-color: #cc0000; color: white'
                elif score >= 50:
                    return 'background-color: #ff6600; color: white'
                else:
                    return 'background-color: #ffcc00; color: black'
            except:
                return ''
        
        styled_timeline = df_timeline.style.applymap(color_score).format("{:.2f}")
        
        timeline_height = min(800, len(df_timeline) * 40 + 50)
        
        st.dataframe(styled_timeline, use_container_width=True, height=timeline_height)
        
        csv = df_timeline.to_csv()
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"timeline_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv"
        )
    
    if auto_scan:
        time.sleep(1)
        st.rerun()

def daily_summary_page():
    st.title("📅 DAILY SUMMARY")
    
    logger = DataLogger()
    dates = logger.get_all_dates()
    
    if not dates:
        st.warning("⚠️ No data")
        return
    
    selected_date = st.selectbox("📆 Date", dates, index=0)
    
    df = logger.load_date(selected_date)
    
    if df is None or df.empty:
        st.error("❌ No data")
        return
    
    table_height = min(800, len(df) * 40 + 50)
    st.dataframe(df, use_container_width=True, hide_index=True, height=table_height)
    
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download",
        data=csv,
        file_name=f"{selected_date}.csv",
        mime="text/csv"
    )

def timeline_archive_page():
    st.title("📦 TIMELINE ARCHIVE")
    
    tracker = LiveTracker()
    dates = tracker.get_archive_dates()
    
    if not dates:
        st.warning("⚠️ No archived timelines yet")
        st.info("💡 Timelines are automatically archived at 14:59")
        return
    
    selected_date = st.selectbox("📆 Select Date", dates, index=0)
    
    df = tracker.load_archive(selected_date)
    
    if df is None or df.empty:
        st.error("❌ No data for this date")
        return
    
    st.subheader(f"Timeline Grid - {selected_date}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Stocks Tracked", len(df))
    
    with col2:
        st.metric("Time Points", len(df.columns))
    
    def color_score(val):
        try:
            score = float(val)
            if score >= 85:
                return 'background-color: #800020; color: white; font-weight: bold'
            elif score >= 60:
                return 'background-color: #cc0000; color: white'
            elif score >= 50:
                return 'background-color: #ff6600; color: white'
            else:
                return 'background-color: #ffcc00; color: black'
        except:
            return ''
    
    styled_df = df.style.applymap(color_score).format("{:.2f}")
    
    table_height = min(800, len(df) * 40 + 50)
    st.dataframe(styled_df, use_container_width=True, height=table_height)
    
    csv = df.to_csv()
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"timeline_{selected_date}.csv",
        mime="text/csv"
    )

def portfolio_tracker_page():
    st.title("💼 PORTFOLIO TRACKER")
    st.caption("Tracking stocks with score 60+ from end of day")
    
    portfolio = PortfolioTracker()
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        filter_status = st.selectbox("Filter", ["All", "Open", "Closed"])
        
        if st.button("🔄 Refresh Prices"):
            st.rerun()
    
    with st.spinner("Loading portfolio..."):
        df = portfolio.get_portfolio_with_current_prices()
    
    if df is None or df.empty:
        st.info("💡 Portfolio is empty. Stocks with score 60+ will be added automatically at 14:59")
        return
    
    if filter_status != "All":
        df = df[df['Status'] == filter_status]
    
    if df.empty:
        st.info(f"📊 No {filter_status.lower()} positions")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Positions", len(df))
    
    with col2:
        open_positions = len(df[df['Status'] == 'Open'])
        st.metric("Open", open_positions)
    
    with col3:
        winners = len(df[df['Change%'] > 0])
        st.metric("Winners", winners)
    
    with col4:
        avg_pl = df['Change%'].mean()
        st.metric("Avg P/L", f"{avg_pl:+.2f}%")
    
    display_data = []
    for idx, row in df.iterrows():
        display_data.append({
            'Date': row['Date'],
            'Ticker': row['Ticker'],
            'Score': f"{row['Score']:.2f}",
            'Buy Price': f"${row['BuyPrice']:.2f}",
            'Current': f"${row['CurrentPrice']:.2f}",
            'Change%': f"{row['Change%']:+.2f}%",
            'P/L': f"${row['P/L']:+.2f}",
            'Status': row['Status'],
            'PositionKey': row['PositionKey']
        })
    
    display_df = pd.DataFrame(display_data)
    
    def highlight_pl(row):
        """צבע רק את עמודות Change% ו-P/L"""
        try:
            change_pct = float(row['Change%'].replace('%', '').replace('+', ''))
            
            # התחל עם רשימה ריקה (לבן)
            styles = [''] * len(row)
            
            # מצא את האינדקסים של Change% ו-P/L
            change_idx = list(row.index).index('Change%')
            pl_idx = list(row.index).index('P/L')
            
            if change_pct > 0:
                # ירוק
                styles[change_idx] = 'background-color: #90EE90; color: black; font-weight: bold'
                styles[pl_idx] = 'background-color: #90EE90; color: black; font-weight: bold'
            elif change_pct < 0:
                # אדום
                styles[change_idx] = 'background-color: #FFB6C1; color: black; font-weight: bold'
                styles[pl_idx] = 'background-color: #FFB6C1; color: black; font-weight: bold'
            
            return styles
        except:
            return [''] * len(row)
    
    styled_df = display_df[['Date', 'Ticker', 'Score', 'Buy Price', 'Current', 'Change%', 'P/L', 'Status']].style.apply(highlight_pl, axis=1)
    
    table_height = min(800, len(display_df) * 40 + 50)
    st.dataframe(styled_df, use_container_width=True, hide_index=True, height=table_height)
    
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Portfolio CSV",
        data=csv,
        file_name=f"portfolio_{datetime.now().strftime('%Y-%m-%d')}.csv",
        mime="text/csv"
    )

def main():
    page = st.sidebar.radio(
        "🧭 Navigation",
        ["📊 Live Tracker", "💼 Portfolio Tracker", "📅 Daily Summary", "📦 Timeline Archive"]
    )
    
    if page == "📊 Live Tracker":
        main_page()
    elif page == "💼 Portfolio Tracker":
        portfolio_tracker_page()
    elif page == "📅 Daily Summary":
        daily_summary_page()
    else:
        timeline_archive_page()

if __name__ == "__main__":
    main()
