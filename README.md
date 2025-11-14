# Bitcoin ETF - BTC Spot Arbitrage Strategy

> **Intraday arbitrage strategy exploiting pricing inefficiencies between Bitcoin ETF (IBIT) and BTC spot markets**

---

## Project Overview
This project implements a **systematic arbitrage trading strategy** that identifies and exploits temporary pricing discrepancies between the IBIT ETF and Bitcoin spot prices. The strategy operates on **15-minute intraday data** during US Market hours. 

### Key Features
- Real-time data from Alapaca (ETF: IBIT) and Coinbase (BTC Spot)
- Automated spread detection with transaction cost modeling
- Risk-managed signal generation with entry/exit rules
- Backtesting 

--- 

## Performance results 

### Backtest summary (last 30 days)

| Metric | Value |
|--------|-------|
| **Total Trades** | 11 |
| **Win Rate** | 100% (11/11) |
| **Total Return** | +0.002% |
| **Total PnL** | +$15.51 |
| **Sharpe Ratio** | 59.32 |
| **Max Win** | $5.63 |
| **Max Loss** | $0.02 |

### Highlights 
- **Data Points:** 576 intraday bars
- **Trading Days:** 22 days
- **Opportunity Rate:** 2.83% of all bars
- **Average holding time:** 53 minutes 

--- 

## Project Structure 

Bitcoin-ETF-Arbitrage/
├── src/
│   ├── data_collector_intraday.py      # Module 1: Data Collection
│   ├── spread_calculator_intraday.py   # Module 2: Spread Analysis
│   └── backtest_intraday.py            # Module 3: Backtesting 
├── config/
│   └── alpaca_config.py                # API
├── data/
│   ├── ibit_btc_intraday_15min.csv     # Raw collected data
│   └── analyzed_intraday_data.csv      # Data with signals
├── results/
│   └── intraday_trades.csv             
├── requirements.txt
└── README.md

--- 

## Getting started 

### Prerequisites 
- Python 3.9+
- Alpaca Markets API

### Installation 

1. **Clone the repository**
```bash
   git clone https://github.com/ilonarozen/Bitcoin-ETF-Arbitrage.git
   cd Bitcoin-ETF-Arbitrage
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API credentials**
   
   Create `config/alpaca_config.py`:
   ```python
   ALPACA_API_KEY = "your_api_key_here"
   ALPACA_SECRET_KEY = "your_secret_key_here"
   ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
   ```

### Usage: 
Run the complete pipeline:
```bash
# Step 1: Collect intraday data
python src/data_collector_intraday.py

# Step 2: Calculate spreads and generate trading signals
python src/spread_calculator_intraday.py

# Step 3: Run backtest
python src/backtest_intraday.py
```

--- 

## Methodology 

### 1. Data Collection

- **IBIT ETF:** Alpaca Markets API 
- **BTC:** Coinbase API

### 2. Spread Calculation

```
Spread (bps) = ((ETF_price_normalized - BTC_price) / BTC_price) × 10,000
Net_Spread = Raw_Spread - Trading_Costs
```
with **trading costs**: 
- ETF commission & spread: 0.8bps (to choose)
- BTC exchange fees & spread: 3.5bps (to choose)

### 3. Signal Generation 

**Entry conditions:**
- **Long BTC / Short ETF:** When `Net_Spread > +15 bps` (ETF overpriced)
- **Short BTC / Long ETF:** When `Net_Spread < -15 bps` (ETF underpriced)

**Exit conditions:**
- Spread converges
- Maximum holding time reached (1 hour)
- Stop-loss triggered (spread widens by 20+ bps)
- End of trading day

### 4. Risk Management 

- Capital: $1M (to choose)
- Position size: 0.1% of capital per trade
- No overnight position 

--- 

## Results 

### Data Collection 
- 576 IBIT bars
- 2853 BTC bars 
- Trading days: 22

### Spread Analysis 
- Signals generated: 
    ``` 
    LONG_BTC_SHORT_ETF: 2 (0.4%)
    SHORT_BTC_LONG_ETF: 14 (2.5%)
    HOLD: 549 (97.2%)
    ``` 

### Backtest 

Initial Capital:    $1,000,000
Final Capital:      $1,000,015.51
Total PnL:          $15.51
Win rate:           100%
Sharpe ratio:       59.32 

--- 

## Limitations 

This is a backtest on historical data. These results (100% win rate, 59.32 sharpe ratio) are achieved on past data so cannot be interpreted as indicative of future performance. 

### Key limitations: 
- **Backtesting bias:** strategy was developed and tested on the same historical data.
- **Costs:** may vary significantly on live markets. 
- **Sample size:** only 11 trades over 22 days (statistically insufficient for conclusion). 

### For real trading, expect:
- Lower win rate 
- Higher transaction costs 
- Execution challenges during volatile periods

**Real time adaptation:** with better data infrastructure, and low-latency execution systems, this strategy framework could be adapted for live trading.

### Data Limitations: 
This strategy uses free API which have limitations: 
- Alpaca: limited historical data 
- Coinbase: rate limits apply

### Transaction costs: 
- This model uses simplified transaction costs. Real-world costs may vary based on:
    - Market conditions and liquidity
    - Order size 
    - Exchange fees 

--- 

## What I learned 

- Building end-to-end quantitative trading systems
- Working with real-time APIs
- Data cleaning and time series alignment
- Performance metrics and backtesting methodology

--- 

## Future Improvements 

- Add 1-minute bar analysis for higher frequency
- Test on multiple ETFs (FBTC, GBTC, BITO...) 
- Add machine learning for spread prediction
- Implement real-time monitoring and alerts

--- 

## License 

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

--- 

## Author 

**Ilona Rozenberg**

- GitHub: [@ilonarozen](https://github.com/ilonarozen)
- LinkedIn: [Ilona Rozenberg](https://www.linkedin.com/in/ilona-rozenberg-05b593190/)
