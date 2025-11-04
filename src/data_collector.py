"""
Data Collector Module
Fetches IBIT ETF and BTC spot prices from Yahoo Finance
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class DataCollector:
    """Collect IBIT and BTC spot prices"""
    
    def __init__(self):
        """Initialize the data collector"""
        print("DataCollector initialized")
        
    def get_ibit_data(self, start_date, end_date):
        """
        Get IBIT historical data from Yahoo Finance
        
        Parameters:
        -----------
        start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection
            
        Returns:
        --------
        pd.DataFrame : IBIT price data with timestamp, close price, volume
        """
        print(f"Fetching IBIT data from {start_date} to {end_date}...")
        
        try:
            ibit = yf.Ticker("IBIT")
            # Get 1-day interval data (1-minute not available for long periods)
            df = ibit.history(start=start_date, end=end_date, interval="1d")
            
            if df.empty:
                print("Warning: No IBIT data found for this period")
                return pd.DataFrame()
            
            df = df.reset_index()
            df = df.rename(columns={
                'Date': 'timestamp',
                'Close': 'ibit_close',
                'Volume': 'ibit_volume'
            })
            
            print(f"✓ Fetched {len(df)} IBIT data points")
            return df[['timestamp', 'ibit_close', 'ibit_volume']]
            
        except Exception as e:
            print(f"Error fetching IBIT data: {e}")
            return pd.DataFrame()
    
    def get_btc_spot_data(self, start_date, end_date):
        """
        Get BTC spot price from Yahoo Finance
        
        Parameters:
        -----------
        start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection
            
        Returns:
        --------
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
        Merge IBIT and BTC data on timestamp
    
        Parameters:
        -----------
        start_date : str or datetime
            Start date for data collection
        end_date : str or datetime
            End date for data collection
        
        Returns:
        --------
        pd.DataFrame : Merged dataset with IBIT and BTC prices
        """
        print("\n" + "="*50)
        print("Starting data collection...")
        print("="*50 + "\n")
        
        # Get both datasets
        ibit_df = self.get_ibit_data(start_date, end_date)
        btc_df = self.get_btc_spot_data(start_date, end_date)
        
        # Check if data was fetched
        if ibit_df.empty or btc_df.empty:
            print("\n⚠️  Could not fetch data. Check dates and try again.")
            return pd.DataFrame()
        
        # Convert timestap to date only (remove time component)
        ibit_df['date'] = pd.to_datetime(ibit_df['timestamp']).dt.date
        btc_df['date'] = pd.to_datetime(btc_df['timestamp']).dt.date

        # Merge on date instead of exact timestamp
        merged = pd.merge(
            ibit_df[['date', 'ibit_close', 'ibit_volume']], 
            btc_df[['date', 'btc_close', 'btc_volume']], 
            on='date', 
            how='inner'
        )

        # Rename date back to timestamp for consistency
        merged = merged.rename(columns={'date': 'timestamp'})

        print(f"\n✓ Successfully merged data: {len(merged)} rows")
        if len(merged) > 0:
            print(f"Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
    
        return merged
    
    def save_data(self, df, filename='ibit_btc_data.csv'):
        """
        Save data to CSV file
        
        Parameters:
        -----------
        df : pd.DataFrame
            Data to save
        filename : str
            Output filename
        """
        if df.empty:
            print("No data to save")
            return
        
        filepath = f'data/{filename}'
        df.to_csv(filepath, index=False)
        print(f"\n✓ Data saved to {filepath}")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")


# Test the module
if __name__ == "__main__":
    print("="*60)
    print("BITCOIN ETF-SPOT ARBITRAGE - DATA COLLECTOR")
    print("="*60 + "\n")
    
    # Create collector
    collector = DataCollector()
    
    # Get data from last 3 months (IBIT launched in Jan 2024)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"Collecting data from {start_date.date()} to {end_date.date()}\n")
    
    # Fetch and merge data
    data = collector.merge_data(start_date, end_date)
    
    # Save to file
    if not data.empty:
        collector.save_data(data)
        
        # Display first few rows
        print("\n" + "="*60)
        print("PREVIEW OF DATA:")
        print("="*60)
        print(data.head())
        
        print("\n" + "="*60)
        print("DATA STATISTICS:")
        print("="*60)
        print(data.describe())
    
    print("\n✓ Data collection complete!")
