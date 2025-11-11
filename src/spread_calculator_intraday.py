"""
Intraday Spread Calculator (15-minute bars)
Calculates spreads between Bitcoin ETF and BTC spot prices on intraday data
Identifies arbitrage opportunities throughout the trading day
"""

import pandas as pd
import numpy as np

class IntradaySpreadCalculator:
    """
    Calculate spreads between ETF and BTC spot prices on 15-minute bars
    Generate trading signals based on net spread after costs
    """
    
    def __init__(self, etf_ticker='IBIT', threshold_bps=15):
        """
        Initialize the intraday spread calculator
        
        Parameters:
        -----------
        etf_ticker : str
            ETF ticker symbol (default: 'IBIT')
        threshold_bps : float
            Minimum net spread in basis points to trigger a signal (default: 15 bps)
            Lower threshold for intraday (vs 20 bps for daily) due to tighter spreads
        """
        self.etf_ticker = etf_ticker.upper()
        self.threshold_bps = threshold_bps
        
        print(f"IntradaySpreadCalculator initialized")
        print(f"  ETF: {self.etf_ticker}")
        print(f"  Signal threshold: {self.threshold_bps} bps")
        print(f"  Timeframe: 15-minute bars")
    
    def calculate_raw_spread(self, df):
        """
        Calculate raw spread between ETF and BTC spot
        
        Spread (bps) = ((ETF_price_normalized - BTC_price) / BTC_price) * 10,000
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with ETF and BTC intraday prices
            
        Returns:
        --------
        pd.DataFrame : DataFrame with spread_bps column added
        """
        etf_col = f'{self.etf_ticker.lower()}_close'
        
        if etf_col not in df.columns:
            raise ValueError(f"Column '{etf_col}' not found. Available: {df.columns.tolist()}")
        
        # Calculate the average ratio to normalize ETF price to BTC
        # (e.g., if IBIT = $60 and BTC = $60,000, ratio = 0.001)
        avg_ratio = (df[etf_col] / df['btc_close']).mean()
        
        print(f"\nCalculating intraday spreads...")
        print(f"  Average {self.etf_ticker}/BTC ratio: {avg_ratio:.6f}")
        
        # Normalize ETF price to BTC equivalent
        df['etf_btc_equivalent'] = df[etf_col] / avg_ratio
        
        # Calculate spread in basis points
        df['spread_bps'] = ((df['etf_btc_equivalent'] - df['btc_close']) / df['btc_close']) * 10000
        
        print(f"✓ Raw spreads calculated")
        print(f"  Mean spread: {df['spread_bps'].mean():.2f} bps")
        print(f"  Std dev: {df['spread_bps'].std():.2f} bps")
        print(f"  Range: [{df['spread_bps'].min():.2f}, {df['spread_bps'].max():.2f}] bps")
        
        return df
    
    def calculate_trading_costs(self):
        """
        Estimate trading costs for intraday arbitrage (round-trip)
        
        Costs include:
        - ETF trading: commission + bid-ask spread
        - BTC spot: exchange fees + bid-ask spread
        
        Returns:
        --------
        float : Total round-trip cost in basis points
        """
        # ETF costs (intraday typically lower than daily)
        etf_commission = 0.3      # Commission per side (bps)
        etf_spread = 0.5          # Bid-ask spread (bps)
        
        # BTC spot costs
        btc_fees = 1.5            # Exchange fees (0.15% = 15 bps, but round-trip)
        btc_spread = 2.0          # Bid-ask spread (bps)
        
        # Total round-trip cost
        total_cost = etf_commission + etf_spread + btc_fees + btc_spread
        
        return total_cost
    
    def calculate_net_spread(self, df):
        """
        Calculate net spread after trading costs
        
        Net Spread = Raw Spread - Trading Costs
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with spread_bps column
            
        Returns:
        --------
        pd.DataFrame : DataFrame with net_spread_bps and costs_bps columns added
        """
        costs = self.calculate_trading_costs()
        
        df['costs_bps'] = costs
        df['net_spread_bps'] = df['spread_bps'] - costs
        
        print(f"\n✓ Net spreads calculated")
        print(f"  Trading costs: {costs:.2f} bps per round-trip")
        print(f"  Mean net spread: {df['net_spread_bps'].mean():.2f} bps")
        
        return df
    
    def generate_signals(self, df):
        """
        Generate trading signals based on net spread
        
        Signals:
        - LONG_BTC_SHORT_ETF: net spread > +threshold (ETF overpriced)
        - SHORT_BTC_LONG_ETF: net spread < -threshold (ETF underpriced)
        - HOLD: |net spread| < threshold (no opportunity)
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with net_spread_bps column
            
        Returns:
        --------
        pd.DataFrame : DataFrame with signal column added
        """
        print(f"\nGenerating signals (threshold: {self.threshold_bps} bps)...")
        
        # Initialize all as HOLD
        df['signal'] = 'HOLD'
        
        # ETF overpriced relative to BTC → Long BTC + Short ETF
        df.loc[df['net_spread_bps'] > self.threshold_bps, 'signal'] = 'LONG_BTC_SHORT_ETF'
        
        # ETF underpriced relative to BTC → Short BTC + Long ETF
        df.loc[df['net_spread_bps'] < -self.threshold_bps, 'signal'] = 'SHORT_BTC_LONG_ETF'
        
        # Count signals
        n_long_btc = len(df[df['signal'] == 'LONG_BTC_SHORT_ETF'])
        n_short_btc = len(df[df['signal'] == 'SHORT_BTC_LONG_ETF'])
        n_hold = len(df[df['signal'] == 'HOLD'])
        
        print(f"✓ Signals generated:")
        print(f"  LONG_BTC_SHORT_ETF: {n_long_btc} ({n_long_btc/len(df)*100:.1f}%)")
        print(f"  SHORT_BTC_LONG_ETF: {n_short_btc} ({n_short_btc/len(df)*100:.1f}%)")
        print(f"  HOLD: {n_hold} ({n_hold/len(df)*100:.1f}%)")
        
        return df
    
    def add_time_features(self, df):
        """
        Add time-based features for intraday analysis
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with timestamp column
            
        Returns:
        --------
        pd.DataFrame : DataFrame with time features added
        """
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        df['minute'] = pd.to_datetime(df['timestamp']).dt.minute
        df['time_of_day'] = df['hour'] + df['minute']/60
        
        # Market session labels
        df['session'] = 'midday'
        df.loc[df['hour'] == 9, 'session'] = 'open'
        df.loc[df['hour'] >= 15, 'session'] = 'close'
        
        return df
    
    def analyze_spreads(self, df):
        """
        Perform statistical analysis on spreads
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with spread data
            
        Returns:
        --------
        dict : Performance statistics
        """
        stats = {
            'total_bars': len(df),
            'mean_spread_bps': df['spread_bps'].mean(),
            'std_spread_bps': df['spread_bps'].std(),
            'max_spread_bps': df['spread_bps'].max(),
            'min_spread_bps': df['spread_bps'].min(),
            'mean_net_spread_bps': df['net_spread_bps'].mean(),
            'std_net_spread_bps': df['net_spread_bps'].std(),
            'opportunities': len(df[df['signal'] != 'HOLD']),
            'opportunity_rate': len(df[df['signal'] != 'HOLD']) / len(df) * 100,
            'avg_opportunity_spread': df[df['signal'] != 'HOLD']['net_spread_bps'].abs().mean() if len(df[df['signal'] != 'HOLD']) > 0 else 0
        }
        
        # Analysis by session
        if 'session' in df.columns:
            stats['opportunities_by_session'] = df[df['signal'] != 'HOLD'].groupby('session').size().to_dict()
        
        return stats
    
    def process_data(self, input_file='data/ibit_btc_intraday_15min.csv', 
                     output_file='data/analyzed_intraday_data.csv'):
        """
        Complete processing pipeline: load data, calculate spreads, generate signals
        
        Parameters:
        -----------
        input_file : str
            Path to input CSV file with intraday data
        output_file : str
            Path to output CSV file for analyzed data
            
        Returns:
        --------
        tuple : (DataFrame with analysis, statistics dict)
        """
        print("\n" + "="*70)
        print(f"INTRADAY SPREAD ANALYSIS - {self.etf_ticker}")
        print("="*70)
        
        # Load data
        print(f"\nLoading data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"✓ Loaded {len(df)} bars")
        
        # Add time features
        df = self.add_time_features(df)
        
        # Calculate spreads
        df = self.calculate_raw_spread(df)
        df = self.calculate_net_spread(df)
        
        # Generate signals
        df = self.generate_signals(df)
        
        # Analyze
        stats = self.analyze_spreads(df)
        
        # Save results
        df.to_csv(output_file, index=False)
        print(f"\n✓ Analyzed data saved to {output_file}")
        print(f"  Columns added: spread_bps, net_spread_bps, costs_bps, signal, hour, minute, session")
        
        return df, stats


# Test the module
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - INTRADAY SPREAD CALCULATOR")
    print("="*70 + "\n")
    
    # Configuration
    ETF_TICKER = 'IBIT'
    THRESHOLD_BPS = 15  # Lower threshold for intraday (tighter spreads)
    
    # Create calculator
    calculator = IntradaySpreadCalculator(
        etf_ticker=ETF_TICKER, 
        threshold_bps=THRESHOLD_BPS
    )
    
    # Process data
    df, stats = calculator.process_data(
        input_file='data/ibit_btc_intraday_15min.csv',
        output_file='data/analyzed_intraday_data.csv'
    )
    
    # Display results
    print("\n" + "="*70)
    print("INTRADAY SPREAD STATISTICS:")
    print("="*70)
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    # Show opportunities
    opportunities = df[df['signal'] != 'HOLD']
    if len(opportunities) > 0:
        print("\n" + "="*70)
        print("SAMPLE INTRADAY ARBITRAGE OPPORTUNITIES:")
        print("="*70)
        print(opportunities[['timestamp', 'hour', 'minute', 'spread_bps', 'net_spread_bps', 'signal']].head(10))
    else:
        print("\n⚠️  No arbitrage opportunities found with current threshold")
        print(f"   Try lowering threshold (currently {THRESHOLD_BPS} bps)")
    
    # Preview analyzed data
    print("\n" + "="*70)
    print("PREVIEW OF ANALYZED INTRADAY DATA:")
    print("="*70)
    cols_to_show = ['timestamp', 'ibit_close', 'btc_close', 'spread_bps', 'net_spread_bps', 'signal']
    print(df[cols_to_show].head())
    
    print("\n✓ Intraday spread calculation complete!")
    