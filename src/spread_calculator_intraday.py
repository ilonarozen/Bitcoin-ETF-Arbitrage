# Intraday Spread Calculator (15 min)
# Calculates spread between ETF and BTC Spot and identify arbitrage opportunities 

import pandas as pd
import numpy as np


class IntradaySpreadCalculator:
    # Calculate spreads beween ETF and BTC (15 min) and generate trading signals 

    def __init__(self, etf_ticker="IBIT", threshold_bps=15):
        # Initialization
        self.etf_ticker = etf_ticker.upper()
        self.threshold_bps = threshold_bps

        print("IntradaySpreadCalculator initialized")
        print(f"  ETF: {self.etf_ticker}")
        print(f"  Signal threshold: {self.threshold_bps} bps")
        print("  Timeframe: 15-minute bars")

    # SPREAD CALCULATIONS

    def calculate_raw_spread(self, df):
        # Spread = ((ETF_price_normalized - BTC_price) / BTC_price) * 10,000
        
        etf_col = f"{self.etf_ticker.lower()}_close"

        if etf_col not in df.columns:
            raise ValueError(
                f"Column '{etf_col}' not found. Available: {df.columns.tolist()}"
            )

        # Average ratio ETF/BTC 
        avg_ratio = (df[etf_col] / df["btc_close"]).mean()

        print("\nCalculating spreads...")
        print(f"  Average {self.etf_ticker}/BTC ratio: {avg_ratio:.6f}")

        # Normalize ETF price to BTC equivalent
        df["etf_btc_equivalent"] = df[etf_col] / avg_ratio

        # Spread
        df["spread_bps"] = (
            (df["etf_btc_equivalent"] - df["btc_close"]) / df["btc_close"]
        ) * 10000

        print("✓ Raw spreads calculated")
        print(f"  Mean spread: {df['spread_bps'].mean():.2f} bps")
        print(f"  Std dev: {df['spread_bps'].std():.2f} bps")
        print(
            f"  Range: [{df['spread_bps'].min():.2f}, {df['spread_bps'].max():.2f}] bps"
        )

        return df

    def calculate_trading_costs(self):
        # trading costs for intraday arbitrage, total costs (bps)
        
        # ETF costs
        etf_commission = 0.3  
        etf_spread = 0.5  

        # BTC spot costs
        btc_fees = 1.5  
        btc_spread = 2.0  

        total_cost = etf_commission + etf_spread + btc_fees + btc_spread
        return total_cost

    def calculate_net_spread(self, df):
        # Net Spread = Raw Spread - Trading Costs

        costs = self.calculate_trading_costs()

        df["costs_bps"] = costs
        df["net_spread_bps"] = df["spread_bps"] - costs

        print("\n✓ Net spreads calculated")
        print(f"  Trading costs: {costs:.2f} bps per round-trip")
        print(f"  Mean net spread: {df['net_spread_bps'].mean():.2f} bps")

        return df


    # SIGNALS

    def generate_signals(self, df):
        """
        Generate trading signals based on net spread
        """
        print(f"\nGenerating signals (threshold: {self.threshold_bps} bps)...")

        df["signal"] = "HOLD"

        # ETF overpriced -> Long BTC + Short ETF
        df.loc[df["net_spread_bps"] > self.threshold_bps, "signal"] = (
            "LONG_BTC_SHORT_ETF"
        )

        # ETF underpriced -> Short BTC + Long ETF
        df.loc[df["net_spread_bps"] < -self.threshold_bps, "signal"] = (
            "SHORT_BTC_LONG_ETF"
        )

        n_long_btc = len(df[df["signal"] == "LONG_BTC_SHORT_ETF"])
        n_short_btc = len(df[df["signal"] == "SHORT_BTC_LONG_ETF"])
        n_hold = len(df[df["signal"] == "HOLD"])

        print("✓ Signals generated:")
        print(f"  LONG_BTC_SHORT_ETF: {n_long_btc} ({n_long_btc/len(df)*100:.1f}%)")
        print(f"  SHORT_BTC_LONG_ETF: {n_short_btc} ({n_short_btc/len(df)*100:.1f}%)")
        print(f"  HOLD: {n_hold} ({n_hold/len(df)*100:.1f}%)")

        return df


    # FEATURES & STATS

    def add_time_features(self, df):

        # Read timestamp (UTC)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Convert to NY time
        df["timestamp"] = df["timestamp"].dt.tz_convert("America/New_York")

        df["timestamp"] = df["timestamp"].dt.tz_localize(None)

        # Extract hour, min from NY time 
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df["time_of_day"] = df["hour"] + df["minute"] / 60.0

        df["session"] = "midday"
        df.loc[df["hour"] == 9, "session"] = "open"
        df.loc[df["hour"] >= 15, "session"] = "close"

        return df

    def analyze_spreads(self, df):
        # Statistics analysis 

        stats = {
            "total_bars": len(df),
            "mean_spread_bps": df["spread_bps"].mean(),
            "std_spread_bps": df["spread_bps"].std(),
            "max_spread_bps": df["spread_bps"].max(),
            "min_spread_bps": df["spread_bps"].min(),
            "mean_net_spread_bps": df["net_spread_bps"].mean(),
            "std_net_spread_bps": df["net_spread_bps"].std(),
            "opportunities": len(df[df["signal"] != "HOLD"]),
            "opportunity_rate": len(df[df["signal"] != "HOLD"]) / len(df) * 100,
        }

        if "session" in df.columns:
            stats["opportunities_by_session"] = (
                df[df["signal"] != "HOLD"].groupby("session").size().to_dict()
            )

        return stats


    # FULL PIPELINE

    def process_data(
        self,
        input_file="data/ibit_btc_intraday_15min.csv",
        output_file="data/analyzed_intraday_data.csv",
    ):

        # Load data, calculate spreads, generate signals

        print("\n" + "=" * 70)
        print(f"INTRADAY SPREAD ANALYSIS - {self.etf_ticker}")
        print("=" * 70)

        print(f"\nLoading data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"✓ Loaded {len(df)} bars")

        df = self.add_time_features(df)
        df = self.calculate_raw_spread(df)
        df = self.calculate_net_spread(df)
        df = self.generate_signals(df)

        stats = self.analyze_spreads(df)

        df.to_csv(output_file, index=False)
        print(f"\n✓ Analyzed data saved to {output_file}")
        print(
            "  Columns added: spread_bps, net_spread_bps, costs_bps, signal, hour, minute, session"
        )

        return df, stats


# TEST 

if __name__ == "__main__":
    print("=" * 70)
    print("BITCOIN ETF-SPOT ARBITRAGE - INTRADAY SPREAD CALCULATOR")
    print("=" * 70 + "\n")

    ETF_TICKER = "IBIT" #CHOOSE ETF: IBIT 
    THRESHOLD_BPS = 15  #CHOOSE

    calculator = IntradaySpreadCalculator(
        etf_ticker=ETF_TICKER, threshold_bps=THRESHOLD_BPS
    )

    df, stats = calculator.process_data(
        input_file="data/ibit_btc_intraday_15min.csv",
        output_file="data/analyzed_intraday_data.csv",
    )

    print("\n" + "=" * 70)
    print("INTRADAY SPREAD STATISTICS:")
    print("=" * 70)
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")

    opportunities = df[df["signal"] != "HOLD"]
    if len(opportunities) > 0:
        print("\n" + "=" * 70)
        print("ALL INTRADAY ARBITRAGE OPPORTUNITIES:")
        print("=" * 70 + "\n")

        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 150)

        print(
            opportunities[
                ["timestamp", "hour", "minute", "spread_bps", "net_spread_bps", "signal"]
            ].to_string(index=False)
        )

        pd.reset_option("display.max_rows")
        pd.reset_option("display.max_columns")
        pd.reset_option("display.width")

    else:
        print("No arbitrage opportunities found with current threshold")

    print("\n" + "=" * 70)
    print("PREVIEW OF ANALYZED INTRADAY DATA:")
    print("=" * 70)
    cols_to_show = [
        "timestamp",
        "ibit_close",
        "btc_close",
        "spread_bps",
        "net_spread_bps",
        "signal",
    ]
    print(df[cols_to_show].head())

    print("End")
    
