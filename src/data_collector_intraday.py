# Data Collector: IBIT ETF (Alpaca API) and BTC spot prices (Coinbase) - every 15 min

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
    # Data for IBIT and BTC spot (15 min)

    def __init__(self, etf_ticker="IBIT"):
        # Initialization: ETF = IBIT (default)
        self.etf_ticker = etf_ticker.upper()
        self.alpaca_headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        }

        print("IntradayDataCollector initialized")
        print(f"  ETF: {self.etf_ticker}")
        print("  Timeframe: 15-minute bars")
        print("  Sources: Alpaca IEX (ETF) + Coinbase (BTC)")

    # ------------------------------------------------------------------ #
    # ETF DATA (ALPACA)
    # ------------------------------------------------------------------ #
    def get_etf_intraday_data(self, start_date, end_date):
        """IBIT intraday data from Alpaca (15min bars, US market hours)."""

        print(f"\nFetching {self.etf_ticker} intraday data from Alpaca...")
        print(f"  Period: {start_date.date()} to {end_date.date()}")

        # Format dates for Alpaca API (ISO 8601)
        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str   = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        data_url = "https://data.alpaca.markets"
        url = f"{data_url}/v2/stocks/{self.etf_ticker}/bars"

        params = {
            "timeframe": "15Min",
            "start":     start_str,
            "end":       end_str,
            "limit":     10000,
            "adjustment": "raw",
            "feed":      "iex",
        }

        try:
            response = requests.get(url, headers=self.alpaca_headers, params=params)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching {self.etf_ticker} data from Alpaca: {e}")
            return pd.DataFrame()

        if "bars" not in data or not data["bars"]:
            print(f"  Warning: No {self.etf_ticker} data returned")
            return pd.DataFrame()

        df = pd.DataFrame(data["bars"])

        df = df.rename(
            columns={
                "t": "timestamp",
                "c": f"{self.etf_ticker.lower()}_close",
                "o": f"{self.etf_ticker.lower()}_open",
                "h": f"{self.etf_ticker.lower()}_high",
                "l": f"{self.etf_ticker.lower()}_low",
                "v": f"{self.etf_ticker.lower()}_volume",
            }
        )

        # Timestamps Alpaca -> UTC puis convertis en heure de New York
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        ts_ny = df["timestamp"].dt.tz_convert("America/New_York")

        # Filtre sur les heures de marché US en heure de New York
        df["hour"] = ts_ny.dt.hour
        df["minute"] = ts_ny.dt.minute

        df = df[
            ((df["hour"] == 9) & (df["minute"] >= 30)) |
            ((df["hour"] >= 10) & (df["hour"] < 16)) |
            ((df["hour"] == 16) & (df["minute"] == 0))
        ]

        df = df.drop(["hour", "minute"], axis=1)

        print(f"  ✓ Fetched {len(df)} {self.etf_ticker} 15min bars from Alpaca")

        return df[
            [
                "timestamp",
                f"{self.etf_ticker.lower()}_close",
                f"{self.etf_ticker.lower()}_open",
                f"{self.etf_ticker.lower()}_high",
                f"{self.etf_ticker.lower()}_low",
                f"{self.etf_ticker.lower()}_volume",
            ]
        ]


    # ------------------------------------------------------------------ #
    # BTC DATA (COINBASE)
    # ------------------------------------------------------------------ #

        # BTC DATA (COINBASE) ------------------------------------------------ #
    def get_btc_intraday_data(self, start_date, end_date):
        """
        BTC Data (Coinbase Exchange API) - 15min candles on BTC-USD.
        On utilise l'API publique :
        GET https://api.exchange.coinbase.com/products/BTC-USD/candles
        granularity = 900s (15 minutes).
        """

        print(f"\nFetching BTC intraday data from Coinbase Exchange...")
        print(f"  Period: {start_date.date()} to {end_date.date()}")

        product_id = "BTC-USD"
        url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"

        granularity = 900  # 15 minutes in seconds

        # Coinbase limite le nombre de buckets par requête (~300).
        max_buckets_per_call = 300
        window = timedelta(seconds=granularity * max_buckets_per_call)

        all_candles = []
        current_start = start_date

        # On boucle par fenêtres successives pour couvrir toute la période
        while current_start < end_date:
            current_end = min(current_start + window, end_date)

            params = {
                "granularity": granularity,
                "start": current_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

            try:
                r = requests.get(url, params=params)
                r.raise_for_status()
                candles = r.json()
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching BTC data from Coinbase: {e}")
                break

            # candles = [time, low, high, open, close, volume]
            if not candles:
                current_start = current_end
                continue

            all_candles.extend(candles)
            current_start = current_end

        if not all_candles:
            print("  Warning: No BTC data returned from Coinbase")
            return pd.DataFrame()

        df = pd.DataFrame(
            all_candles,
            columns=["time", "btc_low", "btc_high", "btc_open", "btc_close", "btc_volume"],
        )

        # Coinbase renvoie du plus récent au plus ancien → on trie
        df["timestamp"] = pd.to_datetime(df["time"], unit="s")
        df["btc_close"] = df["btc_close"].astype(float)
        df["btc_open"] = df["btc_open"].astype(float)
        df["btc_high"] = df["btc_high"].astype(float)
        df["btc_low"] = df["btc_low"].astype(float)
        df["btc_volume"] = df["btc_volume"].astype(float)

        df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]
        df = df.sort_values("timestamp").reset_index(drop=True)

        print(f"  ✓ Fetched {len(df)} BTC 15min bars from Coinbase")
        return df[
            [
                "timestamp",
                "btc_close",
                "btc_open",
                "btc_high",
                "btc_low",
                "btc_volume",
            ]
        ]

    # ------------------------------------------------------------------ #
    # MERGE + SAVE
    # ------------------------------------------------------------------ #

    def merge_intraday_data(self, start_date, end_date):
        """Merge ETF and BTC intraday data on 15min timestamps."""

        print("\n" + "=" * 70)
        print(f"COLLECTING INTRADAY DATA FOR {self.etf_ticker}")
        print("=" * 70)

        etf_df = self.get_etf_intraday_data(start_date, end_date)
        btc_df = self.get_btc_intraday_data(start_date, end_date)

        if etf_df.empty or btc_df.empty:
            print("Could not fetch data. Check APIs and dates.")
            return pd.DataFrame()

        # Nettoyage timezone
        etf_df["timestamp"] = pd.to_datetime(etf_df["timestamp"]).dt.tz_localize(None)
        btc_df["timestamp"] = pd.to_datetime(btc_df["timestamp"]).dt.tz_localize(None)

        # On arrondit les deux sur la même grille 15min
        etf_df["timestamp_rounded"] = etf_df["timestamp"].dt.floor("15min")
        btc_df["timestamp_rounded"] = btc_df["timestamp"].dt.floor("15min")

        merged = pd.merge(
            etf_df,
            btc_df,
            on="timestamp_rounded",
            how="inner",
            suffixes=("_etf", "_btc"),
        )

        # Garder le timestamp ETF (heures de marché US)
        merged["timestamp"] = merged["timestamp_etf"]
        merged = merged.drop(["timestamp_etf", "timestamp_btc", "timestamp_rounded"], axis=1)

        merged = merged.sort_values("timestamp").reset_index(drop=True)

        print(f"\n✓ Successfully merged intraday data")
        print(f"  Total bars: {len(merged)}")
        if len(merged) > 0:
            print(f"  Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
            trading_days = merged["timestamp"].dt.date.nunique()
            print(f"  Trading days: {trading_days}")
            print(f"  Avg bars per day: {len(merged) / trading_days:.1f}")

        return merged


    def save_data(self, df, filename=None):
        if df.empty:
            print("\nNo data to save")
            return

        if filename is None:
            filename = f"{self.etf_ticker.lower()}_btc_intraday_15min.csv"

        filepath = f"data/{filename}"
        df.to_csv(filepath, index=False)
        print(f"\n✓ Intraday data saved to {filepath}")
        print(f"  Total bars: {len(df)}")
        print(f"  Columns: {list(df.columns)}")


# ---------------------------------------------------------------------- #
# TEST SCRIPT
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    print("=" * 70)
    print("BITCOIN ETF-SPOT ARBITRAGE - DATA COLLECTOR (15min)")
    print("=" * 70 + "\n")

    ETF_TICKER = "IBIT"
    DAYS_TO_FETCH = 30  # Last 30 days

    collector = IntradayDataCollector(etf_ticker=ETF_TICKER)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_FETCH)

    print(f"Fetching last {DAYS_TO_FETCH} days")
    print(f"   {start_date.date()} to {end_date.date()}")
    print("Only US market hours (9:30am - 4pm)\n")

    data = collector.merge_intraday_data(start_date, end_date)

    if not data.empty:
        collector.save_data(data)

        print("\n" + "=" * 70)
        print("PREVIEW OF INTRADAY DATA:")
        print("=" * 70)
        print(data.head(10))

    print("\n✓ Intraday data collection complete!")
