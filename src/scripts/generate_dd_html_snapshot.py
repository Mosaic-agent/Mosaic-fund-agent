import os
import base64
import pandas as pd
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright

# 1. Render Matplotlib Due Diligence Chart (WHITE BACKGROUND)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5), facecolor='#ffffff')
for ax in (ax1, ax2):
    ax.set_facecolor('#f9fafb')
    ax.tick_params(colors='#374151', labelsize=9)
    for spine in ax.spines.values():
        spine.set_color('#d1d5db')

# Subplot 1: Financial & Return Efficiency Ratios
metrics = ['ROE (%)', 'ROA (%)', 'Op Margin (%)', 'Current Ratio (x)', 'Debt/Equity (x)']
ratio_vals = [27.93, 10.33, 7.28, 1.54, 0.64]
colors = ['#16a34a', '#059669', '#0284c7', '#d97706', '#6b7280']

bars1 = ax1.bar(metrics, ratio_vals, color=colors, alpha=0.85, width=0.5)
ax1.set_ylabel('Ratio Value', color='#111827', fontsize=10, fontweight='bold')
ax1.set_title('Return Efficiency & Balance Sheet Ratios', color='#111827', fontsize=11, fontweight='bold', pad=12)
ax1.grid(True, linestyle='--', color='#e5e7eb', alpha=0.9)

for bar in bars1:
    height = bar.get_height()
    ax1.annotate(f'{height:.2f}',
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3), textcoords="offset points",
                 ha='center', va='bottom', fontsize=8.5, fontweight='bold', color='#111827')

# Subplot 2: Shareholding Ownership Structure Donut Chart
labels = ['Promoters (63.6%)', 'Mutual Funds & FIIs (18.5%)', 'Public / Retail (17.9%)']
sizes = [63.61, 18.52, 17.87]
colors_pie = ['#16a34a', '#0284c7', '#9ca3af']

wedges, texts, autotexts = ax2.pie(sizes, labels=labels, autopct='%1.1f%%',
                                  startangle=140, colors=colors_pie,
                                  textprops=dict(color='#111827', fontsize=9, fontweight='bold'),
                                  wedgeprops=dict(width=0.4, edgecolor='#ffffff'))

ax2.set_title('Shareholding Ownership Pattern (Zero Pledged)', color='#111827', fontsize=11, fontweight='bold', pad=12)

plt.tight_layout()

os.makedirs('output', exist_ok=True)
chart_path = 'output/matplot_dd_white.png'
plt.savefig(chart_path, dpi=200, facecolor='#ffffff', bbox_inches='tight')
plt.close()

# Convert chart to base64
with open(chart_path, 'rb') as f:
    chart_b64 = base64.b64encode(f.read()).decode('utf-8')

# 2. Build White Background HTML Due Diligence Dashboard
html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Thangamayil Due Diligence Dashboard</title>
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
        .title {{ font-size: 24px; font-weight: 800; color: #15803d; letter-spacing: -0.5px; }}
        .subtitle {{ font-size: 13px; color: #4b5563; margin-top: 4px; }}
        .badge {{
            background: linear-gradient(135deg, #15803d 0%, #16a34a 100%);
            color: #ffffff;
            font-weight: 800;
            font-size: 15px;
            padding: 8px 16px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(22, 163, 74, 0.2);
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
        .tag-blue {{ background: #e0f2fe; color: #0369a1; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #7dd3fc; }}
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
            <div class="title">THANGAMAYIL JEWELLERY — FORENSIC DUE DILIGENCE SCORECARD</div>
            <div class="subtitle">Promoter Dilution Audit · Pledged Equity · Balance Sheet Quality · Return Ratios</div>
        </div>
        <div class="badge">CLEAN FORENSIC AUDIT PASS</div>
    </div>

    <!-- Due Diligence Chart Card -->
    <div class="card-full">
        <div class="card-title">📊 Forensic Due Diligence & Capital Structure Overview</div>
        <img class="chart-img" src="data:image/png;base64,{chart_b64}" alt="Thangamayil Due Diligence Chart">
    </div>

    <div class="grid-2">
        <!-- Forensic Risk Matrix Card -->
        <div class="card">
            <div class="card-title">🔍 Forensic Risk Audit Checklist</div>
            <div class="metric-row">
                <div class="metric-label">Pledged Shares %</div>
                <div class="metric-val">0.00% <span class="tag-green">PASS (Zero Pledged)</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Promoter Shareholding</div>
                <div class="metric-val">63.61% <span class="tag-green">PASS (High Skin in Game)</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Promoter Dilution Status</div>
                <div class="metric-val">No Sell-Down <span class="tag-green">PASS (Shares Intact)</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Return on Equity (ROE)</div>
                <div class="metric-val">27.93% <span class="tag-green">PASS (High Efficiency)</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Return on Assets (ROA)</div>
                <div class="metric-val">10.33% <span class="tag-green">PASS (Strong Turnover)</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Current Liquidity Ratio</div>
                <div class="metric-val">1.54x <span class="tag-green">PASS (Healthy Liquid Cover)</span></div>
            </div>

            <div class="verdict-box">
                <div class="verdict-title">🛡️ Promoter Ownership & Dilution Audit</div>
                <div class="verdict-desc">Promoter equity is completely unencumbered (<b>0.00% pledged</b>). The promoter shareholding drop in percentage was verified to be non-dilutive equity expansion rather than promoter sell-down.</div>
            </div>
        </div>

        <!-- Institutional Backing & Balance Sheet Health Card -->
        <div class="card">
            <div class="card-title">🏦 Institutional Due Diligence & Balance Sheet Quality</div>
            <div class="metric-row">
                <div class="metric-label">DSP Small Cap Allocation</div>
                <div class="metric-val">₹945.0 Cr <span class="tag-blue">4.81% of NAV</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Institutional Holding %</div>
                <div class="metric-val">18.52% <span class="tag-blue">3 Active AMCs</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Debt-to-Equity Ratio</div>
                <div class="metric-val">0.64x <span class="tag-blue">Inventory Backed</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Operating Profit Margin</div>
                <div class="metric-val">7.28% <span class="tag-blue">Scale Leverage</span></div>
            </div>
            <div class="metric-row">
                <div class="metric-label">Accounting Red Flags</div>
                <div class="metric-val">None Detected <span class="tag-green">PASS</span></div>
            </div>

            <div class="verdict-box" style="background:#eff6ff; border-left-color:#0284c7; border-color:#dbeafe;">
                <div class="verdict-title" style="color:#0369a1;">⚖️ Overall Institutional Investment Verdict</div>
                <div class="verdict-desc" style="color:#1e40af;"><b>Thangamayil Jewellery</b> passes all forensic accounting scans with top marks. Outstanding ROE (27.9%), zero pledged shares, and strong institutional backing (<b>DSP holding ₹945 Cr</b>) make it a high-conviction retail compounder.</div>
            </div>
        </div>
    </div>
</body>
</html>
"""

html_path = 'output/thangamayil_dd_white_dashboard.html'
with open(html_path, 'w') as f:
    f.write(html_content)

print(f"White HTML Due Diligence dashboard generated at {html_path}")

# 3. Take Playwright Snapshot
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1500, "height": 1050})
    page.goto(f"file://{os.path.abspath(html_path)}")
    
    snapshot_path = 'output/thangamayil_dd_white_snapshot.png'
    page.screenshot(path=snapshot_path, full_page=True)
    browser.close()

print(f"High-res white background Due Diligence snapshot saved to {snapshot_path}")
