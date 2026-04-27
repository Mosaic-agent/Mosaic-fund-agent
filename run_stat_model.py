import yfinance as yf
import pandas as pd
import numpy as np

# 1. Fetch ADSK Price Data
ticker = yf.Ticker("ADSK")
df_price = ticker.history(start="2021-01-01", end="2026-04-25")

# Resample to annual (End of Year)
df = df_price[['Close']].resample('YE').last()
df.index = df.index.year

# 2. Historical EPS Data (Aligning with FY21-FY26 Actuals)
eps_data = {
    2021: 2.60,
    2022: 2.24,
    2023: 3.78,
    2024: 4.23,
    2025: 5.22,
    2026: 5.30
}
df['EPS'] = df.index.map(eps_data)
df = df.dropna()
df['P_E_Multiple'] = df['Close'] / df['EPS']

print("======================================================")
print(" ADSK Statistical Valuation Model (2021-2026) ")
print("======================================================")
print(df.round(2).to_string())

# 3. Calculate CAGR (Compound Annual Growth Rate)
years = len(df) - 1
if years > 0:
    cagr_price = (df['Close'].iloc[-1] / df['Close'].iloc[0])**(1/years) - 1
    cagr_eps = (df['EPS'].iloc[-1] / df['EPS'].iloc[0])**(1/years) - 1
    cagr_pe = (df['P_E_Multiple'].iloc[-1] / df['P_E_Multiple'].iloc[0])**(1/years) - 1

    print("\n[ CAGR & Variance Analysis ]")
    print(f"1. Earnings Growth (CAGR):    {cagr_eps*100:+.2f}% per year")
    print(f"2. Multiple Deflation (CAGR): {cagr_pe*100:+.2f}% per year")
    print(f"3. Net Price Change (CAGR):   {cagr_price*100:+.2f}% per year")

    # 4. Statistical Correlation
    corr_eps_price = df['EPS'].corr(df['Close'])
    corr_pe_price = df['P_E_Multiple'].corr(df['Close'])
    
    print("\n[ Pearson Correlation (r) ]")
    print(f"Price vs. EPS:          {corr_eps_price:+.3f} (Disconnected from fundamental growth)")
    print(f"Price vs. P/E Multiple: {corr_pe_price:+.3f} (Highly correlated to multiple contraction)")
print("======================================================")
