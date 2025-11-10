"""
Data Collector Module
IBIT ETF and BTC spot prices (Yahoo Finance - change if better source)

ETFs: IBIT, FBTC, GBTC, ARKB, BITB 
Default: IBIT 
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class DataCollector:
# Collect ETFs and BTC Spot prices: IBIT (default), FBTC, GBTC, ARKB, BITB 

# ETFs info: 
    SUPPORTED_ETFS = {
        'IBIT': {
            'name': 'iShares Bitcoin Trust',
            'issuer': 'BlackRock',
            'launch_date': '2024-01-11',
            'expense_ratio': 0.0025
        },
        'FBTC': {
            'name': 'Fidelity Wise Origin Bitcoin Fund',
            'issuer': 'Fidelity',
            'launch_date': '2024-01-11',
            'expense_ratio': 0.0025
        },
        'GBTC': {
            'name': 'Grayscale Bitcoin Trust',
            'issuer': 'Grayscale',
            'launch_date': '2024-01-11',
            'expense_ratio': 0.015
        },
        'ARKB': {
            'name': 'ARK 21Shares Bitcoin ETF',
            'issuer': 'ARK Invest',
            'launch_date': '2024-01-11',
            'expense_ratio': 0.0021
        },
        'BITB': {
            'name': 'Bitwise Bitcoin ETF',
            'issuer': 'Bitwise',
            'launch_date': '2024-01-11',
            'expense_ratio': 0.0020
        }
    }
    
    def __init__(self, etf_ticker='IBIT'):
        """
        Initialize the data collector
        Parameters: 
        etf_ticker : str
            ETF ticker symbol (default: 'IBIT')
            Supported: IBIT, FBTC, GBTC, ARKB, BITB
        """ 
        self.etf_ticker = etf_ticker.upper()
        
        if self.etf_ticker in self.SUPPORTED_ETFS:
            etf_info = self.SUPPORTED_ETFS[self.etf_ticker]
            print(f"DataCollector initialized")
            print(f"  ETF: {self.etf_ticker} - {etf_info['name']}")
            print(f"  Issuer: {etf_info['issuer']}")
            print(f"  Expense Ratio: {etf_info['expense_ratio']*100}%")
        else:
            print(f"Warning: {self.etf_ticker} not in supported list")
            print(f"Supported ETFs: {', '.join(self.SUPPORTED_ETFS.keys())}")
            print(f"Proceeding anyway...")

    def get_etf_data(self, start_date, end_date):
        """ 
        Get ETF data (Yahoo Finance)

        Parameters: 
         start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection

        Returns: 
        pd.DataFrame : ETF price data with timestamp, close price, volume
        """ 
        print(f"\nFetching {self.etf_ticker} data from {start_date} to {end_date}...")

        try: 
            etf = yf.Ticker(self.etf_ticker)
            df = etf.history(start=start_date, end=end_date, interval="1d")
            
            if df.empty:
                print(f"Warning: No {self.etf_ticker} data found for this period")
                return pd.DataFrame()
            
            df = df.reset_index()
            
            # Flexible column naming
            df = df.rename(columns={
                'Date': 'timestamp',
                'Close': f'{self.etf_ticker.lower()}_close',
                'Volume': f'{self.etf_ticker.lower()}_volume'
            })
            
            print(f"✓ Fetched {len(df)} {self.etf_ticker} data points")
            return df[['timestamp', f'{self.etf_ticker.lower()}_close', f'{self.etf_ticker.lower()}_volume']]

        except Exception as e:
            print(f"Error fetching {self.etf_ticker} data: {e}")
            return pd.DataFrame()
        
    
    def get_btc_spot_data(self, start_date, end_date):
        """ 
        Get BTC Spot price (Yahoo Finance): 

        Parameters:
        start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection

        Returns: 
        pd.DataFrame : BTC price data with timestamp, close price, volume
        """ 
        print(f"Fetching BTC spot data from {start_date} to {end_date}...")

        try: 
            btc = yf.Ticker("BTC-USD")
            df = btc.history(start=start_date, end=end_date, interval="1d")
            
            if df.empty:
                print("Warning: No BTC data found for this period")
                return pd.DataFrame()
            
            df = df.reset_index()
            df = df.rename(columns={
                'Date': 'timestamp',
                'Close': 'btc_close',
                'Volume': 'btc_volume'
            })
            
            print(f"✓ Fetched {len(df)} BTC data points")
            return df[['timestamp', 'btc_close', 'btc_volume']]
        
        except Exception as e: 
            print(f"Error fetching BTC data: {e}")
            return pd.DataFrame()

    
    def merge_data(self, start_date, end_date):
        """ 
        Merge ETF and BTC data on timestamp 

        Parameters: 
        start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection

        Returns:
        pd.DataFrame : Merged dataset with ETF and BTC prices
        """ 

        print("\n" + "="*60)
        print(f"Starting data collection for {self.etf_ticker}...")
        print("="*60)

        # Datasets 
        etf_df = self.get_etf_data(start_date, end_date)
        btc_df = self.get_btc_spot_data(start_date, end_date)

        if etf_df.empty or btc_df.empty:
            print("Could not fetch data. Check dates and try again.")
            return pd.DataFrame()

        etf_df['date'] = pd.to_datetime(etf_df['timestamp']).dt.date
        btc_df['date'] = pd.to_datetime(btc_df['timestamp']).dt.date

        etf_close_col = f'{self.etf_ticker.lower()}_close'
        etf_volume_col = f'{self.etf_ticker.lower()}_volume'

        merged = pd.merge(
            etf_df[['date', etf_close_col, etf_volume_col]], 
            btc_df[['date', 'btc_close', 'btc_volume']], 
            on='date', 
            how='inner'
        )

        merged = merged.rename(columns={'date': 'timestamp'})

        print(f"\n✓ Successfully merged data: {len(merged)} rows")
        if len(merged) > 0:
            print(f"Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
        
        return merged

    
    def save_data(self, df, filename=None):
        """ 
        Save data 
        Parameters: 
        df : pd.DataFrame
            Data to save
        filename : str, optional
            Output filename. If None, auto-generates based on ETF ticker
        """ 

        if df.empty:
            print("No data to save")
            return
        
        if filename is None:
            filename = f'{self.etf_ticker.lower()}_btc_data.csv'

        filepath = f'data/{filename}'
        df.to_csv(filepath, index=False)
        print(f"\n✓ Data saved to {filepath}")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")


# TEST: 
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - DATA COLLECTOR (Multi-ETF Support)")
    print("="*70 + "\n")
    
    # Default: IBIT (or can be FBTC, GBTC, ARKB, BITB ) -- CHANGE HERE FOR OTHER ETFs
    ETF_TO_ANALYZE = 'IBIT' 

    print(f"Analyzing: {ETF_TO_ANALYZE}")
    print("-" * 70 + "\n")

    # Create collector for ETF 
    collector = DataCollector(etf_ticker=ETF_TO_ANALYZE)

    # Get data (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    print(f"Date range: {start_date.date()} to {end_date.date()}\n")

    data = collector.merge_data(start_date, end_date)

    # Save 
    if not data.empty:
        collector.save_data(data)
        
        # Display first few rows
        print("\n" + "="*70)
        print("PREVIEW OF DATA:")
        print("="*70)
        print(data.head())
        
        print("\n" + "="*70)
        print("DATA STATISTICS:")
        print("="*70)
        print(data.describe())
    
    print("\n✓ Data collection complete!")
