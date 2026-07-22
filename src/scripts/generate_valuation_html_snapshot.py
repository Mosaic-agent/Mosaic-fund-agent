import os
import base64
import pandas as pd
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright

# 1. Render Matplotlib Valuation & Compounding Chart (WHITE BACKGROUND)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5), facecolor='#ffffff')
for ax in (ax1, ax2):
    ax.set_facecolor('#f9fafb')
    ax.tick_params(colors='#374151', labelsize=9)
    for spine in ax.spines.values():
        spine.set_color('#d1d5db')

# Subplot 1: Annual Revenue & Net Profit Growth (FY23 - FY26)
years = ['FY23', 'FY24', 'FY25', 'FY26']
revenue = [3152.55, 3826.78, 4910.58, 8499.33] # In Cr
net_profit = [79.74, 123.24, 118.71, 351.65] # In Cr

x = range(len(years))
width = 0.35

rects1 = ax1.bar([i - width/2 for i in x], revenue, width, label='Revenue (₹ Cr)', color='#0284c7', alpha=0.85)
ax1_twin = ax1.twinx()
ax1_twin.set_facecolor('none')
ax1_twin.tick_params(colors='#374151', labelsize=9)
for spine in ax1_twin.spines.values():
    spine.set_color('#d1d5db')
rects2 = ax1_twin.bar([i + width/2 for i in x], net_profit, width, label='Net Profit (₹ Cr)', color='#16a34a', alpha=0.85)

ax1.set_ylabel('Revenue (₹ Cr)', color='#0284c7', fontsize=10, fontweight='bold')
ax1_twin.set_ylabel('Net Profit (₹ Cr)', color='#16a34a', fontsize=10, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(years, fontsize=10, fontweight='bold')
ax1.set_title('Revenue & Net Profit Compounding (FY23 - FY26)', color='#111827', fontsize=11, fontweight='bold', pad=12)
ax1.grid(True, linestyle='--', color='#e5e7eb', alpha=0.9)

# Value Labels on PAT Bars
for rect in rects2:
    height = rect.get_height()
    ax1_twin.annotate(f'₹{height:.0f}C',
                      xy=(rect.get_x() + rect.get_width() / 2, height),
                      xytext=(0, 3), textcoords="offset points",
                      ha='center', va='bottom', fontsize=8.5, fontweight='bold', color='#16a34a')

# Subplot 2: Peer Trailing P/E vs Forward P/E Comparison
peer_names = ['Thangamayil', 'Titan', 'Kalyan', 'Senco']
trailing_pe = [57.35, 82.37, 45.71, 11.17]
forward_pe = [41.86, 55.23, 31.48, 15.83]

x_p = range(len(peer_names))
ax2.bar([i - width/2 for i in x_p], trailing_pe, width, label='Trailing P/E', color='#d97706', alpha=0.85)
ax2.bar([i + width/2 for i in x_p], forward_pe, width, label='Forward P/E', color='#059669', alpha=0.85)

ax2.set_ylabel('P/E Multiple (x)', color='#111827', fontsize=10, fontweight='bold')
ax2.set_xticks(x_p)
ax2.set_xticklabels(peer_names, fontsize=10, fontweight='bold')
ax2.set_title('Jewellery Sector Peer P/E Benchmarking', color='#111827', fontsize=11, fontweight='bold', pad=12)
ax2.grid(True, linestyle='--', color='#e5e7eb', alpha=0.9)
ax2.legend(loc='upper right', facecolor='#ffffff', edgecolor='#d1d5db', labelcolor='#111827', fontsize=8.5)

# Value Labels on Thangamayil P/E
ax2.annotate('57.4x', xy=(0 - width/2, 57.35), xytext=(0, 3), textcoords="offset points", ha='center', fontsize=8.5, fontweight='bold', color='#d97706')
ax2.annotate('41.9x', xy=(0 + width/2, 41.86), xytext=(0, 3), textcoords="offset points", ha='center', fontsize=8.5, fontweight='bold', color='#059669')

plt.tight_layout()

os.makedirs('output', exist_ok=True)
chart_path = 'output/matplot_valuation_white.png'
plt.savefig(chart_path, dpi=200, facecolor='#ffffff', bbox_inches='tight')
plt.close()

# Convert chart to base64
with open(chart_path, 'rb') as f:
    chart_b64 = base64.b64encode(f.read()).decode('utf-8')

# 2. Build White Background HTML Valuation Dashboard
html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Thangamayil Valuation Check Dashboard</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background-color: #f3f4f6;
            color: #111827;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            padding: 24px;
            width: 1450px;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid #e5e7eb;
            padding-bottom: 14px;
            margin-bottom: 20px;
        }}
        .title {{ font-size: 24px; font-weight: 800; color: #b45309; letter-spacing: -0.5px; }}
        .subtitle {{ font-size: 13px; color: #4b5563; margin-top: 4px; }}
        .badge {{
            background: linear-gradient(135deg, #059669 0%, #10b981 100%);
            color: #ffffff;
            font-weight: 800;
            font-size: 15px;
            padding: 8px 16px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.2);
        }}
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .card {{
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 16px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}
        .card-full {{
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}
        .card-title {{
            font-size: 14px;
            font-weight: 700;
            color: #374151;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .chart-img {{ width: 100%; border-radius: 8px; border: 1px solid #e5e7eb; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12.5px;
        }}
        th {{
            background-color: #f9fafb;
            color: #4b5563;
            text-align: left;
            padding: 8px 10px;
            font-weight: 700;
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 0.5px;
            border-bottom: 2px solid #e5e7eb;
        }}
        td {{
            padding: 8px 10px;
            border-bottom: 1px solid #e5e7eb;
            color: #111827;
        }}
        tr:last-child td {{ border-bottom: none; }}
        .metric-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 9px 0;
            border-bottom: 1px solid #e5e7eb;
        }}
        .metric-row:last-child {{ border-bottom: none; }}
        .metric-label {{ font-size: 13px; color: #4b5563; }}
        .metric-val {{ font-size: 14.5px; font-weight: 700; color: #111827; }}
        .tag-green {{ background: #d1fae5; color: #065f46; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #6ee7b7; }}
        .tag-amber {{ background: #fef3c7; color: #92400e; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #fcd34d; }}
        .verdict-box {{
            background: #f0fdf4;
            border-left: 4px solid #16a34a;
            padding: 10px 12px;
            border-radius: 6px;
            margin-top: 10px;
            border-top: 1px solid #dcfce7;
            border-right: 1px solid #dcfce7;
            border-bottom: 1px solid #dcfce7;
        }}
        .verdict-title {{ font-size: 12px; color: #15803d; font-weight: 800; }}
        .verdict-desc {{ font-size: 12px; color: #166534; margin-top: 2px; line-height: 1.4; }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="title">THANGAMAYIL JEWELLERY — VALUATION AUDIT & PEER BENCHMARKING</div>
            <div class="subtitle">Trailing P/E · Forward P/E · PEG Ratio · EV/EBITDA · Peer Multiples</div>
        </div>
        <div class="badge">PEG RATIO: 0.29 (PEG &lt; 1.0 ATTRACTIVE)</div>
    </div>

    <!-- Valuation Chart Card -->
    <div class="card-full">
        <div class="card-title">📊 Earnings Compounding & Peer P/E Benchmarking Chart</div>
        <img class="chart-img" src="data:image/png;base64,{chart_b64}" alt="Thangamayil Valuation Chart">
    </div>

    <div class="grid-2">
        <!-- Valuation Metrics Card -->
        <div class="card">
            <div class="card-title">🔍 Thangamayil Valuation Metrics Checklist</div>
            <div class="metric-row">
                <div class="metric-label">Market Capitalisation</div>
                <div class="metric-val">₹20,128.7 Cr</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Trailing P/E Ratio</div>
                <div class="metric-val">57.35x</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Forward P/E Ratio</div>
                <div class="metric-val">41.86x</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">PEG Ratio (P/E to PAT Growth)</div>
                <div class="metric-val">0.29 <span class="tag-green">PEG &lt; 1.0</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Price-to-Book (P/B) Ratio</div>
                <div class="metric-val">14.22x</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">EV / EBITDA</div>
                <div class="metric-val">37.71x</div>
            </div>

            <div class="verdict-box">
                <div class="verdict-title">💡 Valuation Audit Verdict: High Growth Justified</div>
                <div class="verdict-desc">While Trailing P/E (57.35x) appears high, the <b>PEG ratio of 0.29</b> confirms that the valuation multiple is fully backed by hyper-earnings growth (+196% PAT in FY26). Forward P/E compresses cleanly to 41.86x.</div>
            </div>
        </div>

        <!-- Peer Valuation Comparison Table -->
        <div class="card">
            <div class="card-title">🏆 Peer Valuation Benchmarking (Jewellery Sector)</div>
            <table>
                <thead>
                    <tr>
                        <th>Company</th>
                        <th>MCap (₹ Cr)</th>
                        <th>Trailing P/E</th>
                        <th>Forward P/E</th>
                        <th>P/B</th>
                        <th>EV/EBITDA</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><b>Thangamayil</b></td>
                        <td>₹20,128.7</td>
                        <td><b>57.35x</b></td>
                        <td><b>41.86x</b></td>
                        <td>14.22x</td>
                        <td>37.71x</td>
                    </tr>
                    <tr>
                        <td><b>Titan Company</b></td>
                        <td>₹417,940.2</td>
                        <td>82.37x</td>
                        <td>55.23x</td>
                        <td>26.64x</td>
                        <td>56.18x</td>
                    </tr>
                    <tr>
                        <td><b>Kalyan Jewellers</b></td>
                        <td>₹61,561.6</td>
                        <td>45.71x</td>
                        <td>31.48x</td>
                        <td>9.75x</td>
                        <td>28.69x</td>
                    </tr>
                    <tr>
                        <td><b>Senco Gold</b></td>
                        <td>₹6,416.7</td>
                        <td>11.17x</td>
                        <td>15.83x</td>
                        <td>2.55x</td>
                        <td>8.64x</td>
                    </tr>
                </tbody>
            </table>

            <div class="verdict-box" style="background:#eff6ff; border-left-color:#0284c7; border-color:#dbeafe;">
                <div class="verdict-title" style="color:#0369a1;">🏷️ Peer Relative Valuation Position</div>
                <div class="verdict-desc" style="color:#1e40af;">Thangamayil trades at a <b>~30% P/E discount to Titan Company</b> (57.4x vs 82.4x) while growing revenues at 73% YoY, making its growth-adjusted valuation more favorable than Titan.</div>
            </div>
        </div>
    </div>
</body>
</html>
"""

html_path = 'output/thangamayil_valuation_white_dashboard.html'
with open(html_path, 'w') as f:
    f.write(html_content)

print(f"White HTML valuation dashboard generated at {html_path}")

# 3. Take Playwright Snapshot
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1500, "height": 1050})
    page.goto(f"file://{os.path.abspath(html_path)}")
    
    snapshot_path = 'output/thangamayil_valuation_white_snapshot.png'
    page.screenshot(path=snapshot_path, full_page=True)
    browser.close()

print(f"High-res white background valuation snapshot saved to {snapshot_path}")
