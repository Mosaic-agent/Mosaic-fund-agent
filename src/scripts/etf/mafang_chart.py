import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np

# Ensure app imports work
sys.path.insert(0, '/app/src')
sys.path.insert(0, '/app')

from db.pool import query_df
import plotext as plt

# 1. Fetch data
sql = """
SELECT 
    toDate(snapshot_at) as dt,
    argMax(snapshot_at, snapshot_at) as last_ts,
    argMax(premium_discount_pct, snapshot_at) as premium_pct,
    argMax(inav, snapshot_at) as inav,
    argMax(market_price, snapshot_at) as price,
    argMax(source, snapshot_at) as source
FROM market_data.inav_snapshots FINAL 
WHERE symbol = 'MAFANG' AND snapshot_at >= '2023-01-01'
GROUP BY dt
ORDER BY dt ASC
"""

df = query_df(sql)
df['dt'] = pd.to_datetime(df['dt'])
df['year'] = df['dt'].dt.year
df['dt_str'] = df['dt'].dt.strftime('%Y-%m-%d')

# Overall Stats
total_days = len(df)
mean_prem = df['premium_pct'].mean()
median_prem = df['premium_pct'].median()
std_prem = df['premium_pct'].std()
min_prem = df['premium_pct'].min()
max_prem = df['premium_pct'].max()
p25 = df['premium_pct'].quantile(0.25)
p75 = df['premium_pct'].quantile(0.75)

latest = df.iloc[-1]
latest_dt = latest['last_ts']
latest_price = latest['price']
latest_inav = latest['inav']
latest_prem = latest['premium_pct']
latest_source = latest['source']

# Annual Stats
annual = []
for yr, grp in df.groupby('year'):
    annual.append({
        'year': yr,
        'days': len(grp),
        'mean': grp['premium_pct'].mean(),
        'median': grp['premium_pct'].median(),
        'min': grp['premium_pct'].min(),
        'max': grp['premium_pct'].max(),
        'end_prem': grp.iloc[-1]['premium_pct'],
        'end_price': grp.iloc[-1]['price'],
        'end_inav': grp.iloc[-1]['inav']
    })

# Regime Distribution
reg_discount = (df['premium_pct'] < 0).sum()
reg_fair = ((df['premium_pct'] >= 0) & (df['premium_pct'] < 3.0)).sum()
reg_mod = ((df['premium_pct'] >= 3.0) & (df['premium_pct'] < 10.0)).sum()
reg_elev = ((df['premium_pct'] >= 10.0) & (df['premium_pct'] < 25.0)).sum()
reg_extreme = (df['premium_pct'] >= 25.0).sum()

# Build Plotext Chart
dates = df['dt_str'].tolist()
premiums = df['premium_pct'].tolist()

plt.clf()
plt.theme('dark')
plt.date_form('Y-m-d')
plt.plot(dates, premiums, color='cyan', label='MAFANG Premium %')
plt.plot(dates, [0.0]*len(dates), color='red', label='0.0% iNAV Parity')
plt.plotsize(86, 20)
plt.title('MAFANG ETF (Mirae Asset NYSE FANG+): Premium / Discount vs iNAV (%) [2023 - 2026]')
plt.xlabel('Timeline (2023 to Present)')
plt.ylabel('Premium / Discount (%)')
chart_ascii = plt.build()

print("CHART_OUTPUT_START")
print(chart_ascii)
print("CHART_OUTPUT_END")

print(f"LATEST_DATE={latest_dt}")
print(f"LATEST_PRICE={latest_price:.2f}")
print(f"LATEST_INAV={latest_inav:.4f}")
print(f"LATEST_PREM={latest_prem:+.2f}")
print(f"LATEST_SOURCE={latest_source}")

print(f"TOTAL_DAYS={total_days}")
print(f"MEAN_PREM={mean_prem:+.2f}")
print(f"MEDIAN_PREM={median_prem:+.2f}")
print(f"STD_PREM={std_prem:.2f}")
print(f"MIN_PREM={min_prem:+.2f}")
print(f"MAX_PREM={max_prem:+.2f}")
print(f"P25={p25:+.2f}")
print(f"P75={p75:+.2f}")

print("ANNUAL_DATA_START")
for row in annual:
    print(f"{row['year']}|{row['days']}|{row['mean']:+.2f}%|{row['median']:+.2f}%|{row['min']:+.2f}%|{row['max']:+.2f}%|{row['end_prem']:+.2f}%|{row['end_price']:.2f}|{row['end_inav']:.2f}")
print("ANNUAL_DATA_END")

print("REGIME_DATA_START")
print(f"DISCOUNT|{reg_discount}|{reg_discount/total_days*100:.1f}%")
print(f"FAIR|{reg_fair}|{reg_fair/total_days*100:.1f}%")
print(f"MODERATE|{reg_mod}|{reg_mod/total_days*100:.1f}%")
print(f"ELEVATED|{reg_elev}|{reg_elev/total_days*100:.1f}%")
print(f"EXTREME|{reg_extreme}|{reg_extreme/total_days*100:.1f}%")
print("REGIME_DATA_END")
