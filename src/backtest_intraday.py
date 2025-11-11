"""
Intraday Backtest Module (15-minute bars)
Simulates the arbitrage trading strategy on intraday data
Calculates PnL, win rate, and performance metrics for intraday trading
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class IntradayBacktester:
    """
    Backtest the ETF-spot arbitrage strategy on 15-minute intraday data
    
    Strategy:
    - Enter position when signal is triggered (net spread > threshold)
    - Hold for short duration (15-60 minutes) OR until spread converges
    - Exit and calculate profit/loss
    - Can take multiple positions per day
    """
    
    def __init__(self, initial_capital=1000000, position_size=0.1, max_holding_bars=4):
        """
        Initialize the intraday backtester
        
        Parameters:
        -----------
        initial_capital : float
            Starting capital in USD (default: $1M)
        position_size : float
            Fraction of capital to use per trade (default: 0.1 = 10%)
        max_holding_bars : int
            Maximum number of 15-min bars to hold a position (default: 4 = 1 hour)
        """
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.position_size = position_size
        self.max_holding_bars = max_holding_bars
        self.trades = []
        self.current_position = None
        
        print(f"IntradayBacktester initialized")
        print(f"  Initial capital: ${self.initial_capital:,.0f}")
        print(f"  Position size: {self.position_size*100}% of capital")
        print(f"  Max holding time: {self.max_holding_bars} bars ({self.max_holding_bars * 15} minutes)")
    
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
            'entry_time': row['timestamp'],
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
            # We bought BTC and sold ETF (betting spread will narrow)
            btc_return = (row['btc_close'] - position['btc_entry']) / position['btc_entry']
            etf_return = (row['ibit_close'] - position['ibit_entry']) / position['ibit_entry']
            strategy_return = btc_return - etf_return
            
        elif position['signal'] == 'SHORT_BTC_LONG_ETF':
            # We sold BTC and bought ETF (betting spread will narrow from negative)
            etf_return = (row['ibit_close'] - position['ibit_entry']) / position['ibit_entry']
            btc_return = (row['btc_close'] - position['btc_entry']) / position['btc_entry']
            strategy_return = etf_return - btc_return
        else:
            strategy_return = 0
        
        # Calculate PnL in dollars
        pnl = position['trade_amount'] * strategy_return
        
        # Calculate holding time
        holding_bars = index - position['entry_idx']
        holding_minutes = holding_bars * 15
        
        # Create completed trade record
        trade = {
            'entry_time': position['entry_time'],
            'exit_time': row['timestamp'],
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
            'holding_bars': holding_bars,
            'holding_minutes': holding_minutes
        }
        
        # Update capital
        self.capital += pnl
        
        return trade
    
    def should_exit_position(self, position, row, index):
        """
        Determine if position should be exited
        
        Exit conditions for intraday:
        1. Spread has converged (abs(spread) < 5 bps)
        2. Held for max duration (e.g., 4 bars = 1 hour)
        3. Spread reversed significantly (moved against us by 20+ bps)
        4. End of day approaching (after 15:45)
        
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
        # Condition 1: Spread converged (good exit)
        if abs(row['net_spread_bps']) < 5:
            return True
        
        # Condition 2: Held for max duration
        holding_bars = index - position['entry_idx']
        if holding_bars >= self.max_holding_bars:
            return True
        
        # Condition 3: Spread reversed significantly (stop loss)
        spread_change = position['entry_spread_bps'] - row['net_spread_bps']
        if position['signal'] == 'LONG_BTC_SHORT_ETF':
            # We want spread to decrease (converge from positive)
            if spread_change < -20:  # Spread widened by 20+ bps
                return True
        elif position['signal'] == 'SHORT_BTC_LONG_ETF':
            # We want spread to increase (converge from negative)
            if spread_change > 20:  # Spread widened by 20+ bps
                return True
        
        # Condition 4: End of day approaching (force exit)
        if 'hour' in row and row['hour'] >= 15 and row['minute'] >= 45:
            return True
        
        return False
    
    def run_backtest(self, df):
        """
        Run the backtest on intraday data
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with signals and prices (15-min bars)
            
        Returns:
        --------
        dict : Performance metrics
        pd.DataFrame : Trade history
        """
        print("\n" + "="*70)
        print("RUNNING INTRADAY BACKTEST")
        print("="*70 + "\n")
        
        self.current_position = None
        
        for idx, row in df.iterrows():
            # Check if we should exit current position
            if self.current_position is not None:
                if self.should_exit_position(self.current_position, row, idx):
                    trade = self.exit_position(self.current_position, row, idx)
                    self.trades.append(trade)
                    self.current_position = None
                    
                    # Print trade result
                    profit_str = f"+${trade['pnl']:,.2f}" if trade['pnl'] > 0 else f"-${abs(trade['pnl']):,.2f}"
                    print(f"✓ Trade closed: {trade['signal'][:15]} | {trade['holding_minutes']}min | {profit_str} ({trade['return_pct']:.3f}%)")
            
            # Check if we should enter a new position (only if not already in one)
            if self.current_position is None and row['signal'] != 'HOLD':
                self.current_position = self.enter_position(row, idx)
                print(f"\n→ Opened: {row['signal']} at {row['timestamp']} | Spread: {row['net_spread_bps']:.2f} bps")
        
        # Close any remaining open position at end
        if self.current_position is not None:
            last_row = df.iloc[-1]
            trade = self.exit_position(self.current_position, last_row, len(df)-1)
            self.trades.append(trade)
            print(f"\n✓ Closed final position at EOD: PnL ${trade['pnl']:,.2f}")
        
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
                'avg_holding_minutes': 0,
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
        
        # Sharpe ratio (annualized for intraday)
        if len(trades_df) > 1:
            returns = trades_df['return_pct'] / 100
            # Assuming ~26 bars per day, 252 trading days
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(26 * 252) if returns.std() > 0 else 0
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
            'avg_holding_minutes': trades_df['holding_minutes'].mean(),
            'max_win': trades_df['pnl'].max(),
            'max_loss': trades_df['pnl'].min(),
            'sharpe_ratio': sharpe_ratio,
            'final_capital': self.capital
        }
        
        return metrics


# Test the module
if __name__ == "__main__":
    print("="*70)
    print("BITCOIN ETF-SPOT ARBITRAGE - INTRADAY BACKTEST")
    print("="*70 + "\n")
    
    # Configuration
    INITIAL_CAPITAL = 1000000   # $1M
    POSITION_SIZE = 0.1          # 10% of capital per trade
    MAX_HOLDING_BARS = 4         # Max 1 hour (4 x 15min bars)
    
    # Create backtester
    backtester = IntradayBacktester(
        initial_capital=INITIAL_CAPITAL,
        position_size=POSITION_SIZE,
        max_holding_bars=MAX_HOLDING_BARS
    )
    
    # Load analyzed data
    print("\nLoading analyzed intraday data...")
    df = pd.read_csv('data/analyzed_intraday_data.csv')
    print(f"✓ Loaded {len(df)} bars")
    print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"  Signals: {len(df[df['signal'] != 'HOLD'])} opportunities")
    
    # Run backtest
    metrics, trades_df = backtester.run_backtest(df)
    
    # Display results
    print("\n" + "="*70)
    print("INTRADAY BACKTEST RESULTS")
    print("="*70 + "\n")
    
    print(f"Initial Capital:        ${metrics['final_capital'] - metrics['total_pnl']:,.2f}")
    print(f"Final Capital:          ${metrics['final_capital']:,.2f}")
    print(f"Total PnL:              ${metrics['total_pnl']:,.2f}")
    print(f"Total Return:           {metrics['total_return_pct']:.3f}%")
    print(f"\nTotal Trades:           {metrics['total_trades']}")
    print(f"Winning Trades:         {metrics['winning_trades']} ({metrics['win_rate']:.1f}%)")
    print(f"Losing Trades:          {metrics['losing_trades']}")
    print(f"\nAvg PnL per Trade:      ${metrics['avg_pnl_per_trade']:,.2f}")
    print(f"Avg Return per Trade:   {metrics['avg_return_per_trade']:.3f}%")
    print(f"Avg Holding Time:       {metrics['avg_holding_minutes']:.0f} minutes")
    print(f"Max Win:                ${metrics['max_win']:,.2f}")
    print(f"Max Loss:               ${metrics['max_loss']:,.2f}")
    print(f"\nSharpe Ratio:           {metrics['sharpe_ratio']:.2f}")
    
    # Save trades to file
    if not trades_df.empty:
        trades_df.to_csv('results/intraday_trades.csv', index=False)
        print(f"\n✓ Trade history saved to results/intraday_trades.csv")
        
        # Show sample trades
        print("\n" + "="*70)
        print("SAMPLE INTRADAY TRADES:")
        print("="*70)
        cols_to_show = ['entry_time', 'exit_time', 'signal', 'holding_minutes', 'return_pct', 'pnl']
        print(trades_df[cols_to_show].head(10))
    
    print("\n✓ Intraday backtest complete!")
    