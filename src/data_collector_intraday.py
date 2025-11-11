"""
Intraday Data Collector: IBIT ETF and BTC spot prices (15-minute bars)
Uses Alpaca API for IBIT and Binance API for BTC
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import sys
import os

# Import Alpaca config
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
sys.path.insert(0, config_path)

from alpaca_config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL


class IntradayDataCollector:
    """
    Collect intraday (15-minute) data for IBIT ETF and BTC spot
    - IBIT from Alpaca Markets API
    - BTC from Binance API (public, no key needed)
    """
    
    def __init__(self, etf_ticker='IBIT'):
        """
        Initialize the intraday data collector
        
        Parameters:
        -----------
        etf_ticker : str
            ETF ticker symbol (default: 'IBIT')
        """
        self.etf_ticker = etf_ticker.upper()
        self.alpaca_headers = {
            'APCA-API-KEY-ID': ALPACA_API_KEY,
            'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY
        }
        
        print(f"IntradayDataCollector initialized")
        print(f"  ETF: {self.etf_ticker}")
        print(f"  Timeframe: 15-minute bars")
        print(f"  Sources: Alpaca IEX (ETF) + CryptoCompare (BTC)")
    
    def get_etf_intraday_data(self, start_date, end_date):
        """
        Get 15-minute intraday data for ETF from Alpaca
        
        Parameters:
        -----------
        start_date : datetime
            Start date for data
        end_date : datetime
            End date for data
            
        Returns:
        --------
        pd.DataFrame : ETF intraday data
        """
        print(f"\nFetching {self.etf_ticker} intraday data from Alpaca...")
        print(f"  Period: {start_date.date()} to {end_date.date()}")
        
        # Format dates for Alpaca API (ISO 8601)
        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Alpaca Data API endpoint (v2)
        # Note: Use data API, not trading API
        data_url = "https://data.alpaca.markets"
        url = f"{data_url}/v2/stocks/{self.etf_ticker}/bars"
        
        params = {
            'timeframe': '15Min',
            'start': start_str,
            'end': end_str,
            'limit': 10000,  # Max bars per request
            'adjustment': 'raw',
            'feed': 'iex'  # Use IEX feed (free tier)
        }
        
        try:
            response = requests.get(url, headers=self.alpaca_headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'bars' not in data or not data['bars']:
                print(f"  Warning: No {self.etf_ticker} data returned")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data['bars'])
            
            # Rename columns
            df = df.rename(columns={
                't': 'timestamp',
                'c': f'{self.etf_ticker.lower()}_close',
                'o': f'{self.etf_ticker.lower()}_open',
                'h': f'{self.etf_ticker.lower()}_high',
                'l': f'{self.etf_ticker.lower()}_low',
                'v': f'{self.etf_ticker.lower()}_volume'
            })
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Keep only US market hours (9:30 - 16:00 ET)
            df['hour'] = df['timestamp'].dt.hour
            df['minute'] = df['timestamp'].dt.minute
            df = df[
                ((df['hour'] == 9) & (df['minute'] >= 30)) |
                ((df['hour'] >= 10) & (df['hour'] < 16)) |
                ((df['hour'] == 16) & (df['minute'] == 0))
            ]
            df = df.drop(['hour', 'minute'], axis=1)
            
            print(f"  ✓ Fetched {len(df)} bars")
            
            return df[[
                'timestamp',
                f'{self.etf_ticker.lower()}_close',
                f'{self.etf_ticker.lower()}_open',
                f'{self.etf_ticker.lower()}_high',
                f'{self.etf_ticker.lower()}_low',
                f'{self.etf_ticker.lower()}_volume'
            ]]
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching {self.etf_ticker} data: {e}")
            return pd.DataFrame()
    
    def get_btc_intraday_data(self, start_date, end_date):
        """
        Get 15-minute intraday data for BTC from CryptoCompare (no geo-blocking)
        
        Parameters:
        -----------
        start_date : datetime
            Start date for data
        end_date : datetime
            End date for data
            
        Returns:
        --------
        pd.DataFrame : BTC intraday data
        """
        print(f"\nFetching BTC intraday data from CryptoCompare...")
        print(f"  Period: {start_date.date()} to {end_date.date()}")
        
        # CryptoCompare API endpoint (public, no auth needed, no geo-blocking)
        url = "https://min-api.cryptocompare.com/data/v2/histominute"
        
        # CryptoCompare uses Unix timestamp
        end_ts = int(end_date.timestamp())
        
        # Calculate how many 15-min periods we need
        time_diff = end_date - start_date
        total_minutes = int(time_diff.total_seconds() / 60)
        total_15min_bars = total_minutes // 15
        
        # Limit to reasonable amount (max 2000 bars per request)
        limit = min(total_15min_bars, 2000)
        
        params = {
            'fsym': 'BTC',
            'tsym': 'USD',
            'limit': limit,
            'toTs': end_ts,
            'aggregate': 15  # Aggregate to 15-minute bars
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('Response') != 'Success' or not data.get('Data', {}).get('Data'):
                print("  Warning: No BTC data returned")
                return pd.DataFrame()
            
            # Extract the data
            bars = data['Data']['Data']
            
            # Convert to DataFrame
            df = pd.DataFrame(bars)
            
            # Convert timestamp and create columns
            df['timestamp'] = pd.to_datetime(df['time'], unit='s')
            df['btc_close'] = df['close'].astype(float)
            df['btc_open'] = df['open'].astype(float)
            df['btc_high'] = df['high'].astype(float)
            df['btc_low'] = df['low'].astype(float)
            df['btc_volume'] = df['volumeto'].astype(float)  # Volume in USD
            
            # Filter to requested date range
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            
            print(f"  ✓ Fetched {len(df)} bars")
            
            return df[[
                'timestamp',
                'btc_close',
                'btc_open',
                'btc_high',
                'btc_low',
                'btc_volume'
            ]]
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching BTC data: {e}")
            return pd.DataFrame()
    
    def merge_intraday_data(self, start_date, end_date):
        """
        Merge ETF and BTC intraday data
        
        Parameters:
        -----------
        start_date : datetime
            Start date
        end_date : datetime
            End date
            
        Returns:
        --------
        pd.DataFrame : Merged intraday data
        """
        print("\n" + "="*70)
        print(f"COLLECTING INTRADAY DATA FOR {self.etf_ticker}")
        print("="*70)
        
        # Fetch both datasets
        etf_df = self.get_etf_intraday_data(start_date, end_date)
        btc_df = self.get_btc_intraday_data(start_date, end_date)
        
        if etf_df.empty or btc_df.empty:
            print("\n⚠️  Could not fetch data. Check your API keys and dates.")
            return pd.DataFrame()
        
        # Ensure timestamps are timezone-naive and round to 15 minutes
        etf_df['timestamp'] = pd.to_datetime(etf_df['timestamp']).dt.tz_localize(None)
        btc_df['timestamp'] = pd.to_datetime(btc_df['timestamp']).dt.tz_localize(None)

        # Round timestamps to nearest 15 minutes for alignment
        etf_df['timestamp_rounded'] = etf_df['timestamp'].dt.floor('15min')
        btc_df['timestamp_rounded'] = btc_df['timestamp'].dt.floor('15min')
        
        # Merge on rounded timestamp
        merged = pd.merge(
            etf_df,
            btc_df,
            on='timestamp_rounded',
            how='inner',
            suffixes=('_etf', '_btc')
        )
        
        # Keep the ETF timestamp (market hours) and drop rounded column
        merged['timestamp'] = merged['timestamp_etf']
        merged = merged.drop(['timestamp_etf', 'timestamp_btc', 'timestamp_rounded'], axis=1)
        
        # Sort by timestamp
        merged = merged.sort_values('timestamp').reset_index(drop=True)
        
        print(f"\n✓ Successfully merged intraday data")
        print(f"  Total bars: {len(merged)}")
        if len(merged) > 0:
            print(f"  Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
            
            # Calculate trading days
            trading_days = merged['timestamp'].dt.date.nunique()
            print(f"  Trading days: {trading_days}")
            print(f"  Avg bars per day: {len(merged) / trading_days:.1f}")
        
        return merged
    
    def save_data(self, df, filename=None):
        """
        Save intraday data to CSV
        
        Parameters:
        -----------
        df : pd.DataFrame
            Data to save
        filename : str, optional
            Output filename
        """
        if df.empty:
            print("\nNo data to save")
            return
        
        if filename is None:
            filename = f'{self.etf_ticker.lower()}_btc_intraday_15min.csv'
        
        filepath = f'data/{filename}'
        df.to_csv(filepath, index=False)
        print(f"\n✓ Intraday data saved to {filepath}")
        print(f"  Total bars: {len(df)}")
        print(f"  Columns: {list(df.columns)}")


# TEST
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - INTRADAY DATA COLLECTOR (15min)")
    print("="*70 + "\n")
    
    # Configuration
    ETF_TICKER = 'IBIT'
    DAYS_TO_FETCH = 30  # Last 30 days 
    
    # Create collector
    collector = IntradayDataCollector(etf_ticker=ETF_TICKER)
    
    # Calculate date range (only weekdays, market hours)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_FETCH)
    
    print(f"\n📅 Fetching last {DAYS_TO_FETCH} days")
    print(f"   {start_date.date()} to {end_date.date()}")
    print(f"\n⏰ Note: Only US market hours (9:30-16:00 ET) will be collected")
    
    # Fetch and merge data
    data = collector.merge_intraday_data(start_date, end_date)
    
    # Save to file
    if not data.empty:
        collector.save_data(data)
        
        # Display preview
        print("\n" + "="*70)
        print("PREVIEW OF INTRADAY DATA:")
        print("="*70)
        print(data.head(10))
        
        print("\n" + "="*70)
        print("SAMPLE FROM DIFFERENT TIMES:")
        print("="*70)
        
        # Show opening (9:30), midday, and closing (16:00) samples
        for hour in [9, 12, 15]:
            sample = data[data['timestamp'].dt.hour == hour].head(2)
            if not sample.empty:
                print(f"\n--- {hour}:XX ---")
                print(sample[['timestamp', 'ibit_close', 'btc_close']])
    
    print("\n✓ Intraday data collection complete!")