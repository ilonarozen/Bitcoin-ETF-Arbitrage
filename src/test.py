import yfinance as yf

print("yfinance version:", yf.__version__)

print(yf.download("IBIT", period="5d").tail())
print(yf.download("BTC-USD", period="5d").tail())

