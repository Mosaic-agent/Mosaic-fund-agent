import os
import base64
import pandas as pd
import matplotlib.pyplot as plt
from playwright.sync_api import sync_playwright

# 1. Render Matplotlib Intrinsic Value Comparison Chart (WHITE BACKGROUND)
fig, ax = plt.subplots(figsize=(10, 4.2), facecolor='#ffffff')
ax.set_facecolor('#f9fafb')
ax.tick_params(colors='#374151', labelsize=9)
for spine in ax.spines.values():
    spine.set_color('#d1d5db')

models = [
    'Current Price',
    'DCF Bullish\n(26% Growth)',
    'DCF Base\n(22% Growth)',
    'DCF Cons.\n(18% Growth)',
    'Graham Model\n(20% Growth)',
    'Peter Lynch\n(22% Growth)'
]
values = [6476.00, 7192.44, 5066.46, 3914.76, 3353.23, 2488.99]
colors = ['#b45309', '#16a34a', '#0284c7', '#d97706', '#6b7280', '#9ca3af']

bars = ax.bar(models, values, color=colors, alpha=0.85, width=0.55)
ax.axhline(y=6476.00, color='#b45309', linestyle='--', linewidth=1.5, label='Current Market Price (₹6,476)')

ax.set_ylabel('Intrinsic Value (₹ / Share)', color='#111827', fontsize=10, fontweight='bold')
ax.set_title('Thangamayil Intrinsic Value Model Comparison (₹ / Share)', color='#111827', fontsize=11, fontweight='bold', pad=12)
ax.grid(True, linestyle='--', color='#e5e7eb', alpha=0.9)
ax.legend(loc='upper right', facecolor='#ffffff', edgecolor='#d1d5db', labelcolor='#111827', fontsize=9)

# Value Labels on Bars
for bar in bars:
    height = bar.get_height()
    ax.annotate(f'₹{height:,.0f}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 4), textcoords="offset points",
                ha='center', va='bottom', fontsize=9, fontweight='bold', color='#111827')

plt.tight_layout()

os.makedirs('output', exist_ok=True)
chart_path = 'output/matplot_intrinsic_white.png'
plt.savefig(chart_path, dpi=200, facecolor='#ffffff', bbox_inches='tight')
plt.close()

# Convert chart to base64
with open(chart_path, 'rb') as f:
    chart_b64 = base64.b64encode(f.read()).decode('utf-8')

# 2. Build White Background HTML Intrinsic Value Dashboard
html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Thangamayil Intrinsic Value Dashboard</title>
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
            background: linear-gradient(135deg, #b45309 0%, #d97706 100%);
            color: #ffffff;
            font-weight: 800;
            font-size: 15px;
            padding: 8px 16px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(180, 83, 9, 0.2);
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
        .tag-green {{ background: #d1fae5; color: #065f46; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #6ee7b7; }}
        .tag-blue {{ background: #e0f2fe; color: #0369a1; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #7dd3fc; }}
        .tag-amber {{ background: #fef3c7; color: #92400e; padding: 3px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; border: 1px solid #fcd34d; }}
        .verdict-box {{
            background: #fffbeb;
            border-left: 4px solid #d97706;
            padding: 10px 12px;
            border-radius: 6px;
            margin-top: 10px;
            border-top: 1px solid #fef3c7;
            border-right: 1px solid #fef3c7;
            border-bottom: 1px solid #fef3c7;
        }}
        .verdict-title {{ font-size: 12px; color: #b45309; font-weight: 800; }}
        .verdict-desc {{ font-size: 12px; color: #92400e; margin-top: 2px; line-height: 1.4; }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="title">THANGAMAYIL JEWELLERY — INTRINSIC VALUE & DCF MODEL DASHBOARD</div>
            <div class="subtitle">2-Stage DCF · Benjamin Graham Formula · Peter Lynch Growth Multiplier · WACC 11.5%</div>
        </div>
        <div class="badge">BULLISH DCF TARGET: ₹7,192 (+11.1% UPSIDE)</div>
    </div>

    <!-- Intrinsic Value Chart Card -->
    <div class="card-full">
        <div class="card-title">📊 Intrinsic Value Model Comparison Chart (₹ / Share)</div>
        <img class="chart-img" src="data:image/png;base64,{chart_b64}" alt="Thangamayil Intrinsic Value Chart">
    </div>

    <div class="grid-2">
        <!-- DCF Valuation Scenarios Table -->
        <div class="card">
            <div class="card-title">📥 2-Stage Discounted Cash Flow (DCF) Scenarios</div>
            <table>
                <thead>
                    <tr>
                        <th>DCF Scenario</th>
                        <th>Y1-5 Growth</th>
                        <th>Y6-10 Growth</th>
                        <th>Intrinsic Price</th>
                        <th>Implied Margin</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><b>Bullish Scenario</b></td>
                        <td>26.0% CAGR</td>
                        <td>18.0% CAGR</td>
                        <td><b>₹7,192.44</b></td>
                        <td><span class="tag-green">+11.1% Upside</span></td>
                    </tr>
                    <tr>
                        <td><b>Base Case Scenario</b></td>
                        <td>22.0% CAGR</td>
                        <td>15.0% CAGR</td>
                        <td><b>₹5,066.46</b></td>
                        <td><span class="tag-blue">-21.8% Growth Prem.</span></td>
                    </tr>
                    <tr>
                        <td><b>Conservative Scenario</b></td>
                        <td>18.0% CAGR</td>
                        <td>12.0% CAGR</td>
                        <td><b>₹3,914.76</b></td>
                        <td><span class="tag-amber">-39.5% Baseline</span></td>
                    </tr>
                </tbody>
            </table>

            <div class="verdict-box">
                <div class="verdict-title">💡 DCF Valuation Insight</div>
                <div class="verdict-desc">The current market price of <b>₹6,476.00</b> is pricing in a <b>25%+ earnings compounding trajectory</b> over the next 5-10 years (Bullish DCF Target: <b>₹7,192</b>), fully supported by FY26's +196% PAT surge.</div>
            </div>
        </div>

        <!-- Graham & Lynch Alternative Models Table -->
        <div class="card">
            <div class="card-title">🏛️ Graham & Peter Lynch Valuation Models</div>
            <table>
                <thead>
                    <tr>
                        <th>Valuation Model</th>
                        <th>Growth Input ($g$)</th>
                        <th>Calculated Value</th>
                        <th>Model Focus</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><b>Graham Model (25% Growth)</b></td>
                        <td>25.0% CAGR</td>
                        <td><b>₹4,044.62</b></td>
                        <td>Asset + Growth Formula</td>
                    </tr>
                    <tr>
                        <td><b>Graham Model (20% Growth)</b></td>
                        <td>20.0% CAGR</td>
                        <td><b>₹3,353.23</b></td>
                        <td>Standard Graham Value</td>
                    </tr>
                    <tr>
                        <td><b>Graham Model (15% Growth)</b></td>
                        <td>15.0% CAGR</td>
                        <td><b>₹2,661.84</b></td>
                        <td>Conservative Graham</td>
                    </tr>
                    <tr>
                        <td><b>Peter Lynch Fair Value</b></td>
                        <td>22.0% CAGR</td>
                        <td><b>₹2,488.99</b></td>
                        <td>Fair PE = Growth Rate</td>
                    </tr>
                </tbody>
            </table>

            <div class="verdict-box" style="background:#eff6ff; border-left-color:#0284c7; border-color:#dbeafe;">
                <div class="verdict-title" style="color:#0369a1;">🏷️ Institutional Scarcity Premium</div>
                <div class="verdict-desc" style="color:#1e40af;">Organized retail leaders with <b>60%+ PAT CAGR</b> and active fund backing (<b>DSP Small Cap holding ₹945 Cr / 4.81% NAV</b>) trade at a structural scarcity premium (40x-55x P/E) over traditional asset-backed Graham models.</div>
            </div>
        </div>
    </div>
</body>
</html>
"""

html_path = 'output/thangamayil_intrinsic_white_dashboard.html'
with open(html_path, 'w') as f:
    f.write(html_content)

print(f"White HTML intrinsic value dashboard generated at {html_path}")

# 3. Take Playwright Snapshot
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1500, "height": 1050})
    page.goto(f"file://{os.path.abspath(html_path)}")
    
    snapshot_path = 'output/thangamayil_intrinsic_white_snapshot.png'
    page.screenshot(path=snapshot_path, full_page=True)
    browser.close()

print(f"High-res white background intrinsic value snapshot saved to {snapshot_path}")
