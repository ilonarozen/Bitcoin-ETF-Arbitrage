"""
Spread Calculator 
Calculates the spread between Bitcoin ETF and BTC spot prices
Identifies arbitrage opportunities
"""

import pandas as pd
import numpy as np

class SpreadCalculator:
    # Calculate spreads between ETF and BTC spot prices. Trading signals: spread after costs
    
    def __init__(self, etf_ticker='IBIT', threshold_bps=20):
        # Spread calculator: 
        # Parameters --> etf_ticker : str (Default 'IBIT'), threshold_bps : float (min spread to trigger a signal)

        self.etf_ticker = etf_ticker.upper()
        self.threshold_bps = threshold_bps
        
        print(f"SpreadCalculator initialized")
        print(f"  ETF: {self.etf_ticker}")
        print(f"  Signal threshold: {self.threshold_bps} bps")
    
    def calculate_raw_spread(self, df):
        # Calculate raw spread between ETF and BTC spot: ((ETF_price - BTC_price) / BTC_price) * 10,000
        # Parameters: DataFrame with ETF and BTC

        etf_col = f'{self.etf_ticker.lower()}_close'
        
        if etf_col not in df.columns:
            raise ValueError(f"Column '{etf_col}' not found. Available: {df.columns.tolist()}")

        # Calculate the BTC/share 
        # Calculate the ratio to normalize ETF price to BTC-equivalent
        avg_ratio = (df[etf_col] / df['btc_close']).mean()
        
        print(f"\nCalculating spreads...")
        print(f"  Average {self.etf_ticker}/BTC ratio: {avg_ratio:.6f}")
        
        # Normalize ETF price to BTC-equivalent
        df['etf_btc_equivalent'] = df[etf_col] / avg_ratio
        
        # Calculate spread
        df['spread_bps'] = ((df['etf_btc_equivalent'] - df['btc_close']) / df['btc_close']) * 10000
        
        print(f"✓ Raw spreads calculated")
        print(f"  Mean spread: {df['spread_bps'].mean():.2f} bps")
        print(f"  Std dev: {df['spread_bps'].std():.2f} bps")
        print(f"  Range: [{df['spread_bps'].min():.2f}, {df['spread_bps'].max():.2f}] bps")
        
        return df
    
    def calculate_trading_costs(self):
        # Costs (ETF, BTC) = commission / exchange fees + spread
    
        # ETF costs
        etf_commission = 0.5      
        etf_spread = 1.0          
        
        # BTC spot costs
        btc_fees = 2.0            
        btc_spread = 3.0          
        
        total_cost = etf_commission + etf_spread + btc_fees + btc_spread
        
        return total_cost
    
    def calculate_net_spread(self, df):
        # Net Spread = raw spread - trading costs 

        costs = self.calculate_trading_costs()
        
        df['costs_bps'] = costs
        df['net_spread_bps'] = df['spread_bps'] - costs
        
        print(f"\n✓ Net spreads calculated")
        print(f"  Trading costs: {costs:.2f} bps per round-trip")
        print(f"  Mean net spread: {df['net_spread_bps'].mean():.2f} bps")
        
        return df
    
    def generate_signals(self, df):
        # Generate trading signals: 
            # Long BTC + Short ETF: net spread > +threshold (ETF overpriced)
            # Short BTC + Long ETF: net spread < -threshold (ETF underpriced)
            # Hold: |net spread| < threshold (no opportunity)

        print(f"\nGenerating signals (threshold: {self.threshold_bps} bps)...")
        
        # Initialize: all Hold (no opportunity)
        df['signal'] = 'HOLD'
        
        # ETF overpriced: BTC --> Long BTC + Short ETF
        df.loc[df['net_spread_bps'] > self.threshold_bps, 'signal'] = 'LONG_BTC_SHORT_ETF'
        
        # ETF underpriced: BTC --> Short BTC + Long ETF
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
    
    def analyze_spreads(self, df):
        # Statistical analysis on spreads 

        stats = {
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
        
        return stats
    
    def process_data(self, input_file='data/ibit_btc_data.csv', output_file='data/analyzed_data.csv'):
        # Pipeline: load data, calculate spreads and generate signals 

        print("\n" + "="*60)
        print(f"SPREAD ANALYSIS - {self.etf_ticker}")
        print("="*60)
        
        # Load data
        print(f"\nLoading data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"✓ Loaded {len(df)} rows")
        
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
        print(f"  Columns added: spread_bps, net_spread_bps, costs_bps, signal, etf_btc_equivalent")
        
        return df, stats


# TEST
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - SPREAD CALCULATOR")
    print("="*70 + "\n")
    
    # Config
    ETF_TICKER = 'IBIT'        # ETF to analyze
    THRESHOLD_BPS = 20         # Signal threshold (20 bps = 0.20%)
    
    # Create calculator
    calculator = SpreadCalculator(etf_ticker=ETF_TICKER, threshold_bps=THRESHOLD_BPS)
    
    # Process data
    df, stats = calculator.process_data(
        input_file='data/ibit_btc_data.csv',
        output_file='data/analyzed_data.csv'
    )
    
    # Results
    print("\n" + "="*70)
    print("SPREAD STATISTICS:")
    print("="*70)
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    # Opportunities
    opportunities = df[df['signal'] != 'HOLD']
    if len(opportunities) > 0:
        print("\n" + "="*70)
        print("SAMPLE ARBITRAGE OPPORTUNITIES:")
        print("="*70)
        print(opportunities[['timestamp', 'spread_bps', 'net_spread_bps', 'signal']].head(10))
    else:
        print("\n⚠️  No arbitrage opportunities found with current threshold")
        print(f"   Try lowering threshold (currently {THRESHOLD_BPS} bps)")
    
    print("\n" + "="*70)
    print("PREVIEW OF ANALYZED DATA:")
    print("="*70)
    print(df[['timestamp', 'ibit_close', 'btc_close', 'spread_bps', 'net_spread_bps', 'signal']].head())
    
    print("\n✓ Spread calculation complete!")

    