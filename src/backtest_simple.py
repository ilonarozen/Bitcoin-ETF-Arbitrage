"""
Simple Backtest Module
Simulates the arbitrage trading strategy on historical data
Calculates PnL, win rate, and performance metrics
"""

import pandas as pd
import numpy as np
from datetime import datetime

class SimpleBacktester:
    """
    Backtest the ETF-spot arbitrage strategy
    
    Strategy:
    - Enter position when signal is triggered (net spread > threshold)
    - Hold for fixed period OR until spread converges
    - Exit and calculate profit/loss
    """
    
    def __init__(self, initial_capital=1000000, position_size=0.1):
        """
        Initialize the backtester
        
        Parameters:
        -----------
        initial_capital : float
            Starting capital in USD (default: $100,000)
        position_size : float
            Fraction of capital to use per trade (default: 0.5 = 50%)
        """
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.position_size = position_size
        self.trades = []
        self.positions = []
        
        print(f"SimpleBacktester initialized")
        print(f"  Initial capital: ${self.initial_capital:,.0f}")
        print(f"  Position size: {self.position_size*100}% of capital")
    
    def enter_position(self, row, index):
        """
        Enter a new trading position
        
        Parameters:
        -----------
        row : pd.Series
            Data row with signal and prices
        index : int
            Row index in dataframe
            
        Returns:
        --------
        dict : Position details
        """
        trade_amount = self.capital * self.position_size
        
        position = {
            'entry_idx': index,
            'entry_date': row['timestamp'],
            'signal': row['signal'],
            'entry_spread_bps': row['net_spread_bps'],
            'ibit_entry': row['ibit_close'],
            'btc_entry': row['btc_close'],
            'trade_amount': trade_amount,
            'status': 'open'
        }
        
        return position
    
    def exit_position(self, position, row, index):
        """
        Exit a trading position and calculate PnL
        
        Parameters:
        -----------
        position : dict
            Open position to exit
        row : pd.Series
            Current data row with prices
        index : int
            Current row index
            
        Returns:
        --------
        dict : Completed trade with PnL
        """
        # Calculate returns based on strategy
        if position['signal'] == 'LONG_BTC_SHORT_ETF':
            # We bought BTC and sold ETF
            # PnL = (BTC price change) - (ETF price change)
            btc_return = (row['btc_close'] - position['btc_entry']) / position['btc_entry']
            etf_return = (row['ibit_close'] - position['ibit_entry']) / position['ibit_entry']
            strategy_return = btc_return - etf_return
            
        elif position['signal'] == 'SHORT_BTC_LONG_ETF':
            # We sold BTC and bought ETF
            # PnL = (ETF price change) - (BTC price change)
            etf_return = (row['ibit_close'] - position['ibit_entry']) / position['ibit_entry']
            btc_return = (row['btc_close'] - position['btc_entry']) / position['btc_entry']
            strategy_return = etf_return - btc_return
        else:
            strategy_return = 0
        
        # Calculate PnL in dollars
        pnl = position['trade_amount'] * strategy_return
        
        # Create completed trade record
        trade = {
            'entry_date': position['entry_date'],
            'exit_date': row['timestamp'],
            'signal': position['signal'],
            'entry_spread_bps': position['entry_spread_bps'],
            'exit_spread_bps': row['net_spread_bps'],
            'spread_change_bps': position['entry_spread_bps'] - row['net_spread_bps'],
            'ibit_entry': position['ibit_entry'],
            'ibit_exit': row['ibit_close'],
            'btc_entry': position['btc_entry'],
            'btc_exit': row['btc_close'],
            'trade_amount': position['trade_amount'],
            'return_pct': strategy_return * 100,
            'pnl': pnl,
            'holding_days': index - position['entry_idx']
        }
        
        # Update capital
        self.capital += pnl
        
        return trade
    
    def should_exit_position(self, position, row, index):
        """
        Determine if position should be exited
        
        Exit conditions:
        1. Spread has converged (abs(spread) < 10 bps)
        2. Held for 5+ days (simplified - in reality would be intraday)
        3. Spread reversed significantly (moved against us)
        
        Parameters:
        -----------
        position : dict
            Current open position
        row : pd.Series
            Current data row
        index : int
            Current row index
            
        Returns:
        --------
        bool : True if should exit
        """
        # Condition 1: Spread converged
        if abs(row['net_spread_bps']) < 10:
            return True
        
        # Condition 2: Held too long
        holding_days = index - position['entry_idx']
        if holding_days >= 5:
            return True
        
        # Condition 3: Spread reversed significantly
        spread_change = position['entry_spread_bps'] - row['net_spread_bps']
        if position['signal'] == 'LONG_BTC_SHORT_ETF':
            # We want spread to decrease (converge from positive)
            if spread_change < -50:  # Spread widened by 50+ bps
                return True
        elif position['signal'] == 'SHORT_BTC_LONG_ETF':
            # We want spread to increase (converge from negative)
            if spread_change > 50:  # Spread widened by 50+ bps
                return True
        
        return False
    
    def run_backtest(self, df):
        """
        Run the backtest on historical data
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with signals and prices
            
        Returns:
        --------
        dict : Performance metrics
        pd.DataFrame : Trade history
        """
        print("\n" + "="*60)
        print("RUNNING BACKTEST")
        print("="*60 + "\n")
        
        current_position = None
        
        for idx, row in df.iterrows():
            # Check if we should exit current position
            if current_position is not None:
                if self.should_exit_position(current_position, row, idx):
                    trade = self.exit_position(current_position, row, idx)
                    self.trades.append(trade)
                    current_position = None
                    
                    # Print trade result
                    profit_str = f"+${trade['pnl']:,.2f}" if trade['pnl'] > 0 else f"-${abs(trade['pnl']):,.2f}"
                    print(f"Trade closed: {trade['signal'][:12]}... {profit_str} ({trade['return_pct']:.2f}%)")
            
            # Check if we should enter a new position (only if not already in one)
            if current_position is None and row['signal'] != 'HOLD':
                current_position = self.enter_position(row, idx)
                print(f"\nOpened position: {row['signal']} on {row['timestamp']} (spread: {row['net_spread_bps']:.2f} bps)")
        
        # Close any remaining open position at end
        if current_position is not None:
            last_row = df.iloc[-1]
            trade = self.exit_position(current_position, last_row, len(df)-1)
            self.trades.append(trade)
            print(f"\nClosed final position: {trade['signal'][:12]}... PnL: ${trade['pnl']:,.2f}")
        
        # Calculate metrics
        metrics = self.calculate_metrics()
        
        return metrics, pd.DataFrame(self.trades)
    
    def calculate_metrics(self):
        """
        Calculate performance metrics
        
        Returns:
        --------
        dict : Performance metrics
        """
        if not self.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'total_return_pct': 0,
                'avg_pnl_per_trade': 0,
                'avg_return_per_trade': 0,
                'max_win': 0,
                'max_loss': 0,
                'sharpe_ratio': 0,
                'final_capital': self.capital
            }
        
        trades_df = pd.DataFrame(self.trades)
        
        winning_trades = trades_df[trades_df['pnl'] > 0]
        losing_trades = trades_df[trades_df['pnl'] <= 0]
        
        total_pnl = trades_df['pnl'].sum()
        total_return_pct = (self.capital - self.initial_capital) / self.initial_capital * 100
        
        # Sharpe ratio (annualized, assuming daily returns)
        if len(trades_df) > 1:
            returns = trades_df['return_pct'] / 100
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
        else:
            sharpe_ratio = 0
        
        metrics = {
            'total_trades': len(trades_df),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': len(winning_trades) / len(trades_df) * 100,
            'total_pnl': total_pnl,
            'total_return_pct': total_return_pct,
            'avg_pnl_per_trade': trades_df['pnl'].mean(),
            'avg_return_per_trade': trades_df['return_pct'].mean(),
            'max_win': trades_df['pnl'].max(),
            'max_loss': trades_df['pnl'].min(),
            'sharpe_ratio': sharpe_ratio,
            'final_capital': self.capital
        }
        
        return metrics


# Test the module
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - BACKTEST")
    print("="*70 + "\n")
    
    # Configuration
    INITIAL_CAPITAL = 1000000  # $1M
    POSITION_SIZE = 0.001       # 0.1% of capital per trade
    
    # Create backtester
    backtester = SimpleBacktester(
        initial_capital=INITIAL_CAPITAL,
        position_size=POSITION_SIZE
    )
    
    # Load analyzed data
    print("\nLoading analyzed data...")
    df = pd.read_csv('data/analyzed_data.csv')
    print(f"✓ Loaded {len(df)} days of data")
    print(f"  Signals: {len(df[df['signal'] != 'HOLD'])} opportunities")
    
    # Run backtest
    metrics, trades_df = backtester.run_backtest(df)
    
    # Display results
    print("\n" + "="*70)
    print("BACKTEST RESULTS")
    print("="*70 + "\n")
    
    print(f"Initial Capital:     ${metrics['final_capital'] - metrics['total_pnl']:,.2f}")
    print(f"Final Capital:       ${metrics['final_capital']:,.2f}")
    print(f"Total PnL:           ${metrics['total_pnl']:,.2f}")
    print(f"Total Return:        {metrics['total_return_pct']:.2f}%")
    print(f"\nTotal Trades:        {metrics['total_trades']}")
    print(f"Winning Trades:      {metrics['winning_trades']} ({metrics['win_rate']:.1f}%)")
    print(f"Losing Trades:       {metrics['losing_trades']}")
    print(f"\nAvg PnL per Trade:   ${metrics['avg_pnl_per_trade']:,.2f}")
    print(f"Avg Return per Trade: {metrics['avg_return_per_trade']:.2f}%")
    print(f"Max Win:             ${metrics['max_win']:,.2f}")
    print(f"Max Loss:            ${metrics['max_loss']:,.2f}")
    print(f"\nSharpe Ratio:        {metrics['sharpe_ratio']:.2f}")
    
    # Save trades to file
    if not trades_df.empty:
        trades_df.to_csv('results/trades.csv', index=False)
        print(f"\n✓ Trade history saved to results/trades.csv")
        
        # Show sample trades
        print("\n" + "="*70)
        print("SAMPLE TRADES:")
        print("="*70)
        print(trades_df[['entry_date', 'exit_date', 'signal', 'return_pct', 'pnl']].head(10))
    
    print("\n✓ Backtest complete!")