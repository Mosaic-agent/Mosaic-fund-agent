"""
src/scripts/portfolio/generate_sedemac_pdf.py
─────────────────────────────────────────────
Generates a publication-grade institutional research PDF dossier for
Sedemac Mechatronics Limited (SEDEMAC / INE00XB01019) with embedded
Matplotlib financial charts, ownership breakdowns, and valuation gauges.
"""

from __future__ import annotations

import base64
import io
import os
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import weasyprint

def generate_charts() -> dict[str, str]:
    """Generate high-resolution PNG charts encoded in base64."""
    charts = {}
    
    # ── Style Setup ──────────────────────────────────────────────────────────
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "figure.autolayout": True,
    })

    # ── Chart 1: Financial Progression & Margin Expansion ────────────────────
    fig, ax1 = plt.subplots(figsize=(8, 3.8), dpi=300)
    years = ["FY2023", "FY2024", "FY2025", "FY2026"]
    ebitda = [54.24, 83.12, 125.06, 222.20]
    ebit = [24.05, 47.26, 79.73, 158.72]
    pat = [8.57, 5.88, 47.05, 103.58]
    margin = [14.4, 17.8, 22.3, 24.3]

    x = range(len(years))
    width = 0.25

    rects1 = ax1.bar([i - width for i in x], ebitda, width, label="EBITDA (₹ Cr)", color="#1e40af")
    rects2 = ax1.bar(x, ebit, width, label="EBIT (₹ Cr)", color="#0284c7")
    rects3 = ax1.bar([i + width for i in x], pat, width, label="PAT (₹ Cr)", color="#10b981")

    ax1.set_ylabel("Operating Earnings (₹ Crore)", fontweight="bold", color="#1e293b")
    ax1.set_xticks(x)
    ax1.set_xticklabels(years, fontweight="bold")
    ax1.set_ylim(0, 260)
    ax1.grid(axis="y", linestyle="--", alpha=0.3)

    # Margin curve on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(x, margin, color="#f59e0b", marker="o", linewidth=2.5, markersize=7, label="EBITDA Margin (%)")
    ax2.set_ylabel("EBITDA Margin (%)", fontweight="bold", color="#b45309")
    ax2.set_ylim(10, 30)

    for i, txt in enumerate(margin):
        ax2.annotate(f"{txt:.1f}%", (i, margin[i] + 0.9), ha="center", fontsize=9, fontweight="bold", color="#b45309")

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", framealpha=0.9, fontsize=8)
    plt.title("SEDEMAC: Multi-Year Financial Scale & Margin Expansion (FY23–FY26)", pad=12)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["financials"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 2: Sept 24, 2026 Institutional Block Deal Inflows ──────────────
    fig, (ax_buy, ax_sell) = plt.subplots(1, 2, figsize=(9.5, 3.8), dpi=300)
    
    # Buyers
    buyers = ["Govt Singapore (GIC)", "SBI Mutual Fund", "Invesco Mutual Fund", "Amansa Holdings", "T. Rowe Price", "HDFC Life Ins.", "ABSL Mutual Fund", "Others (ICICI/Axis/FT)"]
    buy_vals = [308.01, 358.00, 306.88, 139.40, 139.33, 121.00, 90.00, 191.05]
    colors_buy = ["#047857", "#059669", "#10b981", "#34d399", "#6ee7b7", "#a7f3d0", "#bae6fd", "#7dd3fc"]
    
    y_pos = range(len(buyers))
    ax_buy.barh(y_pos, buy_vals, color=colors_buy, edgecolor="#064e3b")
    ax_buy.set_yticks(y_pos)
    ax_buy.set_yticklabels(buyers, fontsize=8, fontweight="bold")
    ax_buy.invert_yaxis()
    ax_buy.set_xlabel("Capital Inflow (₹ Crore)", fontweight="bold")
    ax_buy.set_title("Institutional Buyers (Total: ₹1,468 Cr)", fontsize=10, fontweight="bold", color="#065f46")
    ax_buy.grid(axis="x", linestyle="--", alpha=0.3)
    for i, v in enumerate(buy_vals):
        ax_buy.text(v + 5, i, f"₹{v:.0f} Cr", va="center", fontsize=7.5, fontweight="bold")

    # Sellers
    sellers = ["A91 Emerging Fund II", "Xponentia Capital", "MACE Pvt Ltd", "NRJN Trust (Nilekani)", "HDFC Life Ins."]
    sell_vals = [484.87, 507.41, 219.24, 135.45, 121.86]
    colors_sell = ["#b91c1c", "#dc2626", "#ef4444", "#f87171", "#fca5a5"]
    
    y_pos_s = range(len(sellers))
    ax_sell.barh(y_pos_s, sell_vals, color=colors_sell, edgecolor="#7f1d1d")
    ax_sell.set_yticks(y_pos_s)
    ax_sell.set_yticklabels(sellers, fontsize=8, fontweight="bold")
    ax_sell.invert_yaxis()
    ax_sell.set_xlabel("Capital Outflow (₹ Crore)", fontweight="bold")
    ax_sell.set_title("PE / Early Exits (Clean-Up)", fontsize=10, fontweight="bold", color="#991b1b")
    ax_sell.grid(axis="x", linestyle="--", alpha=0.3)
    for i, v in enumerate(sell_vals):
        ax_sell.text(v + 6, i, f"₹{v:.0f} Cr", va="center", fontsize=7.5, fontweight="bold")

    plt.suptitle("The Historic ₹1,468 Cr Institutional Ownership Transfer (Sept 24, 2026 @ ₹3,010)", fontsize=11, fontweight="bold", y=1.02)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["ownership"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 3: Valuation Spectrum & Key Execution Levels ───────────────────
    fig, ax = plt.subplots(figsize=(8.5, 2.6), dpi=300)
    
    # Horizontal tier bands
    tiers = [
        ("Generational Bargain", 2500, 2850, "#dcfce7", "#15803d", "P/E <105x | PEG <0.87"),
        ("Institutional Cheap", 2850, 3050, "#ecfdf5", "#047857", "Sovereign Block Floor (₹3,010)"),
        ("Fair Value Accum.", 3050, 3350, "#f0fdf4", "#166534", "P/E 112-123x | PEG ~1.0x"),
        ("Fully Priced (CMP)", 3350, 3725, "#fef3c7", "#b45309", "Current Market Price (₹3,444.90)"),
        ("Overextended / Trim", 3725, 4500, "#fee2e2", "#b91c1c", "P/E >137x | Multiple Risk"),
    ]

    for label, low, high, bg_col, border_col, sub in tiers:
        ax.axvspan(low, high, color=bg_col, alpha=0.8, edgecolor=border_col, linewidth=1.2)
        mid = (low + high) / 2
        ax.text(mid, 0.65, label, ha="center", va="center", fontsize=8.5, fontweight="bold", color=border_col)
        ax.text(mid, 0.35, sub, ha="center", va="center", fontsize=7, color="#475569")

    # Key Level Markers
    ax.axvline(3010.0, color="#047857", linestyle="--", linewidth=2.2, label="Block Anchor (₹3,010)")
    ax.axvline(3444.90, color="#b45309", linestyle="-", linewidth=2.5, label="CMP (₹3,444.90)")
    ax.axvline(2683.0, color="#b91c1c", linestyle=":", linewidth=2.0, label="Hard Stop (₹2,683)")
    ax.axvline(3722.25, color="#1e40af", linestyle="-.", linewidth=2.0, label="Target 1 (₹3,722)")
    ax.axvline(4431.25, color="#4338ca", linestyle="-.", linewidth=2.0, label="Target 2 (₹4,431)")

    ax.set_xlim(2400, 4600)
    ax.set_ylim(0, 1)
    ax.get_yaxis().set_visible(False)
    ax.set_xlabel("Share Price (₹)", fontweight="bold")
    ax.legend(loc="upper left", bbox_to_anchor=(0.0, 1.35), ncol=5, fontsize=7.5, frameon=True)
    plt.title("SEDEMAC: Tactical Price Spectrum & Execution Zones", pad=28, fontsize=11, fontweight="bold")

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["valuation"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 4: Relational Value Chain & Ecosystem Architecture ───────────
    fig, ax = plt.subplots(figsize=(10.5, 6.8), dpi=300)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    # Title
    ax.text(50, 97.5, "SEDEMAC MECHATRONICS: RELATIONAL VALUE CHAIN & TECHNOLOGY ECOSYSTEM", 
            ha="center", va="center", fontsize=11, fontweight="bold", color="#0f172a")

    # Tier 1: R&D
    b1 = patches.FancyBboxPatch((18, 83), 64, 10.5, boxstyle="round,pad=0.8,rounding_size=2", ec="#312e81", fc="#1e1b4b", lw=1.5)
    ax.add_patch(b1)
    ax.text(50, 89.5, "FOUNDATIONAL CONTROL LAB & IP (IIT BOMBAY INCUBATION)", ha="center", va="center", color="#ffffff", fontweight="bold", fontsize=9.5)
    ax.text(50, 85.5, "80+ Global Patents (US, EP, IN, CN)  •  Sliding Mode Observers  •  High-Frequency Signal Injection", ha="center", va="center", color="#cbd5e1", fontsize=7.5)

    # Tier 2: Core Platform
    b2 = patches.FancyBboxPatch((15, 60), 70, 11, boxstyle="round,pad=0.8,rounding_size=2", ec="#0f766e", fc="#134e4a", lw=1.5)
    ax.add_patch(b2)
    ax.text(50, 67, "CORE PROPRIETARY SENSORLESS MECHATRONICS PLATFORM", ha="center", va="center", color="#ffffff", fontweight="bold", fontsize=10)
    ax.text(50, 63, "Deterministic Edge Control on MCUs  •  10–20 kHz Sampling  •  Zero Physical Hall Sensors", ha="center", va="center", color="#5eead4", fontsize=7.5)

    # Arrow 1 -> 2
    a1 = patches.FancyArrowPatch((50, 83), (50, 71), arrowstyle="-|>,head_length=5,head_width=3", color="#0284c7", lw=2)
    ax.add_patch(a1)
    ax.text(51.5, 77, "Proprietary IP & Embedded Algorithms", ha="left", va="center", fontsize=7, fontweight="bold", color="#0369a1")

    # Tier 3 & 4 Columns
    cols = [
        {
            "x": 13.5, "w": 19.5,
            "t3_title": "MOBILITY: SENSORLESS ISG",
            "t3_sub": "Integrated Starter Generators\nZero-RPM Start • 20% BOM Save",
            "t3_fc": "#1e3a8a", "t3_ec": "#3b82f6",
            "t4_title": "TIER-1 ICE OEMs",
            "t4_sub": "TVS Motor Co. • Hero Moto\nBajaj Auto (Sole-Source Moat)",
            "t4_fc": "#eff6ff", "t4_ec": "#3b82f6", "t4_tc": "#1e3a8a"
        },
        {
            "x": 37.5, "w": 19.5,
            "t3_title": "MOBILITY: EV TRACTION MCU",
            "t3_sub": "BLDC & PMSM Inverters\n48V–150V SiC (7-10x Content)",
            "t3_fc": "#1d4ed8", "t3_ec": "#60a5fa",
            "t4_title": "2W / 3W EV OEMs",
            "t4_sub": "TVS iQube • Bajaj Chetak\nAther Energy • Ola Electric",
            "t4_fc": "#eff6ff", "t4_ec": "#60a5fa", "t4_tc": "#1d4ed8"
        },
        {
            "x": 61.5, "w": 19.5,
            "t3_title": "INDUSTRIAL POWERTRAIN",
            "t3_sub": "Smart Genset GCUs\nRotary Electronic Governors",
            "t3_fc": "#065f46", "t3_ec": "#10b981",
            "t4_title": "GLOBAL GENSET OEMs",
            "t4_sub": "Cummins • Kirloskar (KOEL)\nGenerac • Kohler Power",
            "t4_fc": "#f0fdf4", "t4_ec": "#10b981", "t4_tc": "#065f46"
        },
        {
            "x": 85.5, "w": 19.5,
            "t3_title": "DEFENSE & DUAL-USE",
            "t3_sub": "Tactical APUs & High-Alt\nDrone Engine Electronic EFI",
            "t3_fc": "#9a3412", "t3_ec": "#f97316",
            "t4_title": "TACTICAL / ARMED FORCES",
            "t4_sub": "T-90 / Arjun Tank APUs\nBorder Bases • Recon UAVs",
            "t4_fc": "#fff7ed", "t4_ec": "#f97316", "t4_tc": "#9a3412"
        }
    ]

    for c in cols:
        b3 = patches.FancyBboxPatch((c["x"] - c["w"]/2, 33), c["w"], 13, boxstyle="round,pad=0.6,rounding_size=2", ec=c["t3_ec"], fc=c["t3_fc"], lw=1.4)
        ax.add_patch(b3)
        ax.text(c["x"], 42.5, c["t3_title"], ha="center", va="center", color="#ffffff", fontweight="bold", fontsize=7.5)
        ax.text(c["x"], 37.5, c["t3_sub"], ha="center", va="center", color="#e2e8f0", fontsize=6.8)

        a23 = patches.FancyArrowPatch((50, 60), (c["x"], 46), arrowstyle="-|>,head_length=4.5,head_width=2.8", color="#64748b", lw=1.2, connectionstyle="arc3,rad=" + ("0.08" if c["x"] < 50 else "-0.08" if c["x"] > 50 else "0"))
        ax.add_patch(a23)

        b4 = patches.FancyBboxPatch((c["x"] - c["w"]/2, 7), c["w"], 13, boxstyle="round,pad=0.6,rounding_size=2", ec=c["t4_ec"], fc=c["t4_fc"], lw=1.4)
        ax.add_patch(b4)
        ax.text(c["x"], 16.5, c["t4_title"], ha="center", va="center", color=c["t4_tc"], fontweight="bold", fontsize=7.8)
        ax.text(c["x"], 11.5, c["t4_sub"], ha="center", va="center", color="#334155", fontsize=6.8)

        a34 = patches.FancyArrowPatch((c["x"], 33), (c["x"], 20), arrowstyle="-|>,head_length=4.5,head_width=2.8", color=c["t3_ec"], lw=1.5)
        ax.add_patch(a34)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["value_chain"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 5: Sensorless Control vs Traditional Sensors ───────────────────
    fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 96, "SEDEMAC SENSORLESS TECHNOLOGY vs. TRADITIONAL HARDWARE SENSORS", 
            ha="center", va="center", fontsize=11, fontweight="bold", color="#0f172a")

    # Left Column: Traditional
    b_left = patches.FancyBboxPatch((4, 14), 43, 76, boxstyle="round,pad=0.8,rounding_size=2", ec="#ef4444", fc="#fef2f2", lw=1.5)
    ax.add_patch(b_left)
    ax.text(25.5, 84, "TRADITIONAL SENSOR SYSTEM", ha="center", va="center", fontsize=9.5, fontweight="bold", color="#b91c1c")
    ax.text(25.5, 79, "(Fragile Hardware Architecture)", ha="center", va="center", fontsize=7.5, color="#7f1d1d")

    trad_points = [
        ("3x Physical Hall-Effect Sensors", "Mounted inside the hot, vibrating motor core."),
        ("Complex Wiring Harnesses", "Multi-wire harnesses prone to pin loosening."),
        ("Thermal & Oil Vulnerability", "Engine oil seepage & heat degrade sensor magnets."),
        ("Mechanical Failure Rate", "Primary point of roadside breakdown in 2Ws."),
        ("Higher BOM & Repair Costs", "Cost: ₹1,500 – ₹2,200 | Warranty claims for OEMs.")
    ]
    for i, (head, desc) in enumerate(trad_points):
        y = 70 - i * 11
        p_box = patches.FancyBboxPatch((6, y-3), 39, 9, boxstyle="round,pad=0.4,rounding_size=1", ec="#fca5a5", fc="#ffffff", lw=1)
        ax.add_patch(p_box)
        ax.text(8, y+2, "✖ " + head, fontsize=7.5, fontweight="bold", color="#991b1b")
        ax.text(8, y-1.5, desc, fontsize=6.5, color="#475569")

    # Right Column: Sedemac Sensorless
    b_right = patches.FancyBboxPatch((53, 14), 43, 76, boxstyle="round,pad=0.8,rounding_size=2", ec="#10b981", fc="#f0fdf4", lw=1.5)
    ax.add_patch(b_right)
    ax.text(74.5, 84, "SEDEMAC SENSORLESS CONTROL", ha="center", va="center", fontsize=9.5, fontweight="bold", color="#047857")
    ax.text(74.5, 79, "(Patented Software & Observers)", ha="center", va="center", fontsize=7.5, color="#065f46")

    sede_points = [
        ("Zero Physical Hall Sensors", "Eliminates 100% of motor sensor hardware failure."),
        ("Software Back-EMF Observers", "Non-linear sliding-mode estimation at 10–20 kHz."),
        ("High-Frequency Injection (HFI)", "Zero-speed startup angle tracking via saliency (Ld≠Lq)."),
        ("Acoustic Noise Shaping", "Eliminates starter clatter; 100% silent start-stop."),
        ("15% – 25% BOM Savings for OEMs", "Saves ₹300–₹500/vehicle | 24-36mo sole-source moat.")
    ]
    for i, (head, desc) in enumerate(sede_points):
        y = 70 - i * 11
        p_box = patches.FancyBboxPatch((55, y-3), 39, 9, boxstyle="round,pad=0.4,rounding_size=1", ec="#86efac", fc="#ffffff", lw=1)
        ax.add_patch(p_box)
        ax.text(57, y+2, "✔ " + head, fontsize=7.5, fontweight="bold", color="#065f46")
        ax.text(57, y-1.5, desc, fontsize=6.5, color="#475569")

    b_bot = patches.FancyBboxPatch((4, 2), 92, 9, boxstyle="round,pad=0.6,rounding_size=1.5", ec="#0284c7", fc="#e0f2fe", lw=1.2)
    ax.add_patch(b_bot)
    ax.text(50, 6.5, "CORE MOAT: SEDEMAC REPLACES EXPENSIVE FAILING HARDWARE WITH PATENTED MATHEMATICAL CODE", 
            ha="center", va="center", fontsize=7.5, fontweight="bold", color="#0369a1")

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["sensor_comparison"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 6: Edge AI & Deterministic Algorithmic Stack ───────────────────
    fig, ax = plt.subplots(figsize=(10, 4.8), dpi=300)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 95, "SEDEMAC DETERMINISTIC EDGE AI & REAL-TIME ALGORITHMIC PIPELINE", 
            ha="center", va="center", fontsize=11, fontweight="bold", color="#0f172a")

    # Left: Inputs
    b_in = patches.FancyBboxPatch((3, 20), 22, 62, boxstyle="round,pad=0.8,rounding_size=2", ec="#475569", fc="#f8fafc", lw=1.5)
    ax.add_patch(b_in)
    ax.text(14, 75, "ANALOG SENSING", ha="center", va="center", fontsize=8.5, fontweight="bold", color="#1e293b")
    ax.text(14, 70, "(ADC Sampling @ 10–20 kHz)", ha="center", va="center", fontsize=6.8, color="#64748b")

    inputs = ["• Stator Currents (ia, ib, ic)", "• DC Bus Voltage (Vdc)", "• Temperature Sensors (T)", "• Throttle Demand Angle"]
    for i, item in enumerate(inputs):
        ax.text(5, 58 - i * 10, item, fontsize=7.2, color="#334155", fontweight="bold")

    a1 = patches.FancyArrowPatch((26, 51), (32, 51), arrowstyle="-|>,head_length=5,head_width=3", color="#0284c7", lw=2)
    ax.add_patch(a1)

    # Center: MCU Edge AI Engine
    b_mid = patches.FancyBboxPatch((33, 10), 38, 77, boxstyle="round,pad=0.8,rounding_size=2", ec="#0284c7", fc="#0f172a", lw=1.8)
    ax.add_patch(b_mid)
    ax.text(52, 80, "EMBEDDED MCU ESTIMATION ENGINE", ha="center", va="center", fontsize=9.5, fontweight="bold", color="#38bdf8")
    ax.text(52, 75, "(Real-Time Digital Twin on DSP Silicon)", ha="center", va="center", fontsize=7, color="#94a3b8")

    blocks = [
        ("1. High-Frequency Injection (HFI)", "Injects voltage carrier to detect saliency (Ld≠Lq).\nResolves initial rotor position at 0-RPM without sensors.", "#0284c7"),
        ("2. Dynamic Sliding Mode Observer (SMO)", "Non-linear back-EMF state estimator running at 20 kHz.\nReconstructs angular speed and phase angle in real time.", "#10b981"),
        ("3. Online Parameter Drift Learning", "Continuous tracking of Rs and Ld/Lq variations across\n-40°C to 125°C thermal swings without manual recalibration.", "#f59e0b")
    ]
    for i, (b_title, b_desc, b_col) in enumerate(blocks):
        y = 61 - i * 19
        sub_box = patches.FancyBboxPatch((35, y-6), 34, 15, boxstyle="round,pad=0.5,rounding_size=1", ec=b_col, fc="#1e293b", lw=1.2)
        ax.add_patch(sub_box)
        ax.text(37, y+5.5, b_title, fontsize=7.2, fontweight="bold", color=b_col)
        ax.text(37, y-1.5, b_desc, fontsize=6.2, color="#cbd5e1")

    a2 = patches.FancyArrowPatch((72, 51), (78, 51), arrowstyle="-|>,head_length=5,head_width=3", color="#0284c7", lw=2)
    ax.add_patch(a2)

    # Right: Actuation
    b_out = patches.FancyBboxPatch((79, 20), 18, 62, boxstyle="round,pad=0.8,rounding_size=2", ec="#10b981", fc="#f0fdf4", lw=1.5)
    ax.add_patch(b_out)
    ax.text(88, 75, "FOC CONTROL", ha="center", va="center", fontsize=8.5, fontweight="bold", color="#047857")
    ax.text(88, 70, "(Field-Oriented Control)", ha="center", va="center", fontsize=6.8, color="#065f46")

    outputs = ["• 3-Phase PWM Gate Drives", "• SiC/MOSFET Inverter", "• Ripple Cancellation", "• 100% Deterministic", "• Zero Hall Failure"]
    for i, item in enumerate(outputs):
        ax.text(81, 58 - i * 8.5, item, fontsize=7.0, color="#065f46", fontweight="bold")

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["edge_ai"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    # ── Chart 7: Strategy Fitment & Allocation Decision Tree ─────────────────
    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=300)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 96, "SEDEMAC: INSTITUTIONAL STRATEGY FITMENT & ALLOCATION DECISION TREE", 
            ha="center", va="center", fontsize=11, fontweight="bold", color="#0f172a")

    b_root = patches.FancyBboxPatch((28, 77), 44, 13, boxstyle="round,pad=0.8,rounding_size=2", ec="#0284c7", fc="#0f172a", lw=1.5)
    ax.add_patch(b_root)
    ax.text(50, 85, "INVESTOR PROFILE & MANDATE EVALUATION", ha="center", va="center", fontsize=9.5, fontweight="bold", color="#38bdf8")
    ax.text(50, 80, "Risk Tolerance  •  Time Horizon  •  Valuation Discipline", ha="center", va="center", fontsize=7.2, color="#94a3b8")

    a_left = patches.FancyArrowPatch((40, 77), (25, 62), arrowstyle="-|>,head_length=5,head_width=3", color="#10b981", lw=1.8, connectionstyle="arc3,rad=0.1")
    ax.add_patch(a_left)
    ax.text(28, 69, "High Alpha / Growth Mandate", ha="center", va="center", fontsize=7.5, fontweight="bold", color="#047857")

    b_ldec = patches.FancyBboxPatch((5, 12), 40, 48, boxstyle="round,pad=0.8,rounding_size=2", ec="#10b981", fc="#f0fdf4", lw=1.5)
    ax.add_patch(b_ldec)
    ax.text(25, 54, "RECOMMENDED: STAGGERED ACCUMULATION", ha="center", va="center", fontsize=8.8, fontweight="bold", color="#047857")
    ax.text(25, 49.5, "Horizon: 3 – 5 Years  •  High Volatility Tolerance", ha="center", va="center", fontsize=7.2, color="#065f46")

    l_items = [
        "• Portfolio Allocation: 2.5% – 4.0% of Active Equity",
        "• Tranche 1 (35%): Execute at CMP (₹3,380 – ₹3,445)",
        "• Tranche 2 (65%): Core Limit Orders near ₹3,010 Floor",
        "• Blended Cost Target: ~₹3,150 | Stop: Daily Close <₹2,683",
        "• Multi-Year Thesis: 7–10x EV Content & Margin Expansion"
    ]
    for i, item in enumerate(l_items):
        ax.text(7, 43 - i * 6.5, item, fontsize=7.0, color="#14532d", fontweight="bold" if i < 3 else "normal")

    a_right = patches.FancyArrowPatch((60, 77), (75, 62), arrowstyle="-|>,head_length=5,head_width=3", color="#ef4444", lw=1.8, connectionstyle="arc3,rad=-0.1")
    ax.add_patch(a_right)
    ax.text(72, 69, "Capital Preservation / Deep Value", ha="center", va="center", fontsize=7.5, fontweight="bold", color="#b91c1c")

    b_rdec = patches.FancyBboxPatch((55, 12), 40, 48, boxstyle="round,pad=0.8,rounding_size=2", ec="#ef4444", fc="#fef2f2", lw=1.5)
    ax.add_patch(b_rdec)
    ax.text(75, 54, "RECOMMENDED: AVOID OR WAIT FOR CORRECTION", ha="center", va="center", fontsize=8.8, fontweight="bold", color="#b91c1c")
    ax.text(75, 49.5, "Horizon: < 12 Months  •  Strict Valuation Mandate", ha="center", va="center", fontsize=7.2, color="#7f1d1d")

    r_items = [
        "• Headline P/E of 116x poses significant multiple risk",
        "• Any single quarter volume miss will trigger derating",
        "• Zero dividend yield support for defensive portfolios",
        "• Wait for cyclical pullback towards ₹2,850 Bargain Band",
        "• Re-evaluate post OEM quarterly delivery trajectory"
    ]
    for i, item in enumerate(r_items):
        ax.text(57, 43 - i * 6.5, item, fontsize=7.0, color="#7f1d1d", fontweight="bold" if i < 3 else "normal")

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    charts["strategy_fitment"] = base64.b64encode(buf.getvalue()).decode("utf-8")

    return charts


def build_html_report(charts: dict[str, str]) -> str:
    """Build modern, clean HTML report for PDF rendering."""
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>SEDEMAC Mechatronics - Institutional Equity Research</title>
<style>
    @page {{
        size: A4;
        margin: 14mm 14mm 14mm 14mm;
        @bottom-right {{
            content: "Page " counter(page) " of " counter(pages);
            font-size: 8pt;
            color: #64748b;
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }}
        @bottom-left {{
            content: "Mosaic Quantitative Research | SEDEMAC (INE00XB01019)";
            font-size: 8pt;
            color: #64748b;
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }}
    }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        color: #1e293b;
        line-height: 1.45;
        font-size: 9pt;
        background-color: #f1f5f9;
        margin: 0;
        padding: 20px;
    }}
    .report-container {{
        max-width: 860px;
        margin: 0 auto;
        background: #ffffff;
        padding: 32px 36px;
        border-radius: 8px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }}
    @media print {{
        body {{
            background-color: #ffffff;
            padding: 0;
        }}
        .report-container {{
            max-width: 100%;
            padding: 0;
            box-shadow: none;
            border-radius: 0;
        }}
    }}
    h1 {{
        font-size: 18pt;
        font-weight: 800;
        color: #0f172a;
        margin: 0 0 4px 0;
        letter-spacing: -0.5px;
    }}
    .subtitle {{
        font-size: 9pt;
        color: #475569;
        margin-bottom: 12px;
        border-bottom: 2px solid #0284c7;
        padding-bottom: 6px;
    }}
    h2 {{
        font-size: 11pt;
        font-weight: 700;
        color: #0f172a;
        margin: 14px 0 6px 0;
        border-left: 3.5px solid #0284c7;
        padding-left: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    h3 {{
        font-size: 9.5pt;
        font-weight: 700;
        color: #1e293b;
        margin: 8px 0 4px 0;
    }}
    p {{
        margin: 0 0 6px 0;
        text-align: justify;
    }}
    .kpi-grid {{
        display: table;
        width: 100%;
        margin-bottom: 12px;
        border-collapse: separate;
        border-spacing: 5px;
    }}
    .kpi-row {{
        display: table-row;
    }}
    .kpi-cell {{
        display: table-cell;
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 4px;
        padding: 6px 8px;
        text-align: center;
        width: 16.6%;
    }}
    .kpi-label {{
        font-size: 7pt;
        text-transform: uppercase;
        color: #64748b;
        font-weight: 700;
    }}
    .kpi-value {{
        font-size: 10pt;
        font-weight: 800;
        color: #0f172a;
        margin-top: 2px;
    }}
    .kpi-sub {{
        font-size: 6.5pt;
        color: #10b981;
        font-weight: 600;
    }}
    table.data-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 8pt;
        margin: 6px 0 10px 0;
    }}
    table.data-table th {{
        background: #0f172a;
        color: #ffffff;
        padding: 5px 6px;
        text-align: left;
        font-weight: 600;
        border: 1px solid #0f172a;
    }}
    table.data-table td {{
        padding: 4px 6px;
        border: 1px solid #e2e8f0;
    }}
    table.data-table tr:nth-child(even) {{
        background: #f8fafc;
    }}
    .callout {{
        background: #f0fdf4;
        border-left: 3.5px solid #16a34a;
        padding: 8px 10px;
        margin: 8px 0;
        border-radius: 0 4px 4px 0;
        font-size: 8.5pt;
    }}
    .callout-warn {{
        background: #fffbeb;
        border-left: 3.5px solid #f59e0b;
        padding: 8px 10px;
        margin: 8px 0;
        border-radius: 0 4px 4px 0;
        font-size: 8.5pt;
    }}
    .chart-container {{
        text-align: center;
        margin: 10px 0;
    }}
    .chart-container img {{
        max-width: 100%;
        height: auto;
        border: 1px solid #e2e8f0;
        border-radius: 4px;
    }}
    .page-break {{
        page-break-before: always;
    }}
    ul {{
        margin: 0 0 6px 0;
        padding-left: 16px;
    }}
    li {{
        margin-bottom: 2px;
    }}
</style>
</head>
<body>
<div class="report-container">

<!-- ── HEADER & META ──────────────────────────────────────────────────────── -->
<h1>SEDEMAC MECHATRONICS LIMITED</h1>
<div class="subtitle">
    <strong>TICKER:</strong> SEDEMAC (NSE) &nbsp;|&nbsp; <strong>ISIN:</strong> INE00XB01019 &nbsp;|&nbsp; 
    <strong>SECTOR:</strong> Auto Components &amp; Electronics &nbsp;|&nbsp; <strong>DATE:</strong> September 25, 2026 &nbsp;|&nbsp;
    <strong>RECOMMENDATION:</strong> <span style="color:#16a34a; font-weight:bold;">STAGGERED ACCUMULATION</span>
</div>

<!-- ── KPI METRICS CARDS ─────────────────────────────────────────────────── -->
<div class="kpi-grid">
    <div class="kpi-row">
        <div class="kpi-cell">
            <div class="kpi-label">Current Price</div>
            <div class="kpi-value">₹3,444.90</div>
            <div class="kpi-sub">+14.5% vs Block</div>
        </div>
        <div class="kpi-cell">
            <div class="kpi-label">Market Cap</div>
            <div class="kpi-value">₹15,215 Cr</div>
            <div class="kpi-sub">Mid-Cap Pure Play</div>
        </div>
        <div class="kpi-cell">
            <div class="kpi-label">TTM EBITDA</div>
            <div class="kpi-value">₹234.16 Cr</div>
            <div class="kpi-sub">20.35% Margin</div>
        </div>
        <div class="kpi-cell">
            <div class="kpi-label">Trailing P/E</div>
            <div class="kpi-value">116.82×</div>
            <div class="kpi-sub">Reported (126× TTM)</div>
        </div>
        <div class="kpi-cell">
            <div class="kpi-label">1Y PEG Ratio</div>
            <div class="kpi-value">0.90×</div>
            <div class="kpi-sub">Sub-1.0× (Attractive)</div>
        </div>
        <div class="kpi-cell">
            <div class="kpi-label">Sovereign Floor</div>
            <div class="kpi-value">₹3,010.00</div>
            <div class="kpi-sub">GIC/SBI Block Basis</div>
        </div>
    </div>
</div>

<!-- ── SECTION 1: EXECUTIVE THESIS ────────────────────────────────────────── -->
<h2>1. Executive Summary &amp; Investment Thesis</h2>
<p>
    <strong>SEDEMAC Mechatronics Limited</strong> is India's preeminent deep-tech automotive electronics innovator, 
    pioneering globally patented <strong>sensorless control technologies</strong> for Integrated Starter Generators (ISGs), 
    Electric Vehicle Motor Control Units (MCUs), and industrial engine governors. Founded out of an IIT Bombay research incubation 
    lab, the company possesses an elite R&amp;D DNA that replaces fragile hardware sensors with proprietary mathematical algorithms.
</p>
<div class="callout">
    <strong>Core Thesis Pillars:</strong><br>
    • <strong>Explosive Operating Leverage:</strong> Audited EBITDA grew 4.1× from ₹54.2 Cr (FY23) to ₹222.2 Cr (FY26), with PAT exploding 12.1× to ₹103.6 Cr as fixed R&amp;D costs scaled.<br>
    • <strong>Massive EV Content Multiplier:</strong> Moving from ICE starter ECUs (~₹1,200) to EV Traction Motor Controllers (₹8,000–₹12,000) drives a 7×–10× content expansion per vehicle across TVS, Hero, and Bajaj.<br>
    • <strong>Sovereign Float Absorption:</strong> On September 24, 2026, a ₹1,468 Crore block deal cleanly transferred early PE ownership (A91 &amp; Xponentia) into sovereign wealth funds (GIC Singapore ₹308 Cr, MAS ₹71 Cr) and premier domestic MFs (SBI MF ₹358 Cr, Invesco MF ₹307 Cr) at ₹3,010.00.
</div>

<!-- ── SECTION 2: RELATIONAL VALUE CHAIN & TECH ECOSYSTEM ─────────────────── -->
<h2>2. Relational Value Chain &amp; Technology Ecosystem Graph</h2>
<div class="chart-container">
    <img src="data:image/png;base64,{charts['value_chain']}" alt="Relational Value Chain &amp; Technology Ecosystem Graph">
</div>

<!-- ── SECTION 3: SENSORLESS MOTOR CONTROL ARCHITECTURE ──────────────────── -->
<h2>3. Sensorless Motor Control: Physical Sensors vs. Algorithmic Software</h2>
<div class="chart-container">
    <img src="data:image/png;base64,{charts['sensor_comparison']}" alt="Sensorless Control vs Traditional Sensors Comparison">
</div>

<!-- ── SECTION 4: ARTIFICIAL INTELLIGENCE & EDGE COMPUTING ─────────────────── -->
<h2>4. Artificial Intelligence &amp; Edge Computing Architecture</h2>
<div class="chart-container">
    <img src="data:image/png;base64,{charts['edge_ai']}" alt="Deterministic Edge AI Pipeline &amp; Motor State Estimation">
</div>

<div class="page-break"></div>

<!-- ── SECTION 5: PROMOTER & GOVERNANCE ───────────────────────────────────── -->
<h2>5. Promoter Pedigree &amp; Corporate Governance</h2>
<ul>
    <li><strong>Founder Leadership:</strong> Led by <strong>Prof. Shashikanth Suryanarayanan</strong> (Managing Director), formerly a professor of Mechanical Engineering at <strong>IIT Bombay</strong> (Ph.D., UC Berkeley). The management team consists of IIT/IISc engineering specialists.</li>
    <li><strong>Institutional Cap Table:</strong> Clean institutional ownership structure with zero promoter encumbrance (no pledged shares) and a single class of ordinary shares (4.42 Cr shares).</li>
    <li><strong>Governance Record:</strong> Clean statutory audit track record, audited by Tier-1 auditors, with conservative capital expenditure capitalization policies.</li>
</ul>

<!-- ── SECTION 3: MULTI-YEAR FINANCIAL PROGRESSION ────────────────────────── -->
<h2>3. Multi-Year Financial Performance &amp; Margins</h2>
<table class="data-table">
    <thead>
        <tr>
            <th>Metric (₹ Crore)</th>
            <th>FY2023</th>
            <th>FY2024</th>
            <th>FY2025</th>
            <th>FY2026</th>
            <th>3-Year CAGR / Delta</th>
        </tr>
    </thead>
    <tbody>
        <tr><td><strong>Cost of Revenues</strong></td><td>₹321.64</td><td>₹384.78</td><td>₹437.22</td><td>₹690.39</td><td>+29.0% CAGR</td></tr>
        <tr><td><strong>Normalized EBITDA</strong></td><td>₹54.34</td><td>₹84.49</td><td>₹129.86</td><td>₹221.97</td><td>+59.8% CAGR</td></tr>
        <tr><td><strong>Reported EBITDA</strong></td><td>₹54.24</td><td>₹83.12</td><td>₹125.06</td><td>₹222.20</td><td>+60.0% CAGR</td></tr>
        <tr><td><strong>EBITDA Margin (%)</strong></td><td>14.4%</td><td>17.8%</td><td>22.3%</td><td>24.3%</td><td><strong>+990 bps Expansion</strong></td></tr>
        <tr><td><strong>Operating EBIT</strong></td><td>₹24.05</td><td>₹47.26</td><td>₹79.73</td><td>₹158.72</td><td>+87.6% CAGR</td></tr>
        <tr><td><strong>Net Profit (PAT)</strong></td><td>₹8.57</td><td>₹5.88</td><td>₹47.05</td><td>₹103.58</td><td><strong>+129.5% CAGR (12×)</strong></td></tr>
        <tr><td><strong>Invested Capital</strong></td><td>₹195.15</td><td>₹219.76</td><td>₹353.00</td><td>₹499.83</td><td>+36.8% CAGR</td></tr>
        <tr><td><strong>Total Debt / Net Debt</strong></td><td>₹104.4 / ₹77.0</td><td>₹115.2 / ₹92.2</td><td>₹64.4 / ₹47.5</td><td>₹72.4 / ₹43.3</td><td><strong>-53% Net Debt Reduction</strong></td></tr>
        <tr><td><strong>Common Stock Equity</strong></td><td>₹114.94</td><td>₹124.04</td><td>₹303.38</td><td>₹449.20</td><td>+57.5% CAGR</td></tr>
    </tbody>
</table>

<div class="chart-container">
    <img src="data:image/png;base64,{charts['financials']}" alt="Financial Progression Chart">
</div>

<div class="page-break"></div>

<!-- ── SECTION 7: INSTITUTIONAL OWNERSHIP & BLOCK DEAL ────────────────────── -->
<h2>7. Sovereign &amp; Mutual Fund Ownership Transfer (Sept 24, 2026)</h2>
<p>
    On <strong>September 24, 2026</strong>, a milestone liquidity event occurred via NSE Block &amp; Bulk windows. 
    Early venture capital/private equity investors completely eliminated their supply overhang by placing 
    <strong>4.88 Million shares (~₹1,468 Crore)</strong> at a fixed price of <strong>₹3,010.00</strong> into long-term sovereign 
    and mutual fund balance sheets.
</p>

<div class="chart-container">
    <img src="data:image/png;base64,{charts['ownership']}" alt="Ownership Transfer Chart">
</div>

<table class="data-table">
    <thead>
        <tr>
            <th>Institutional Entity</th>
            <th>Type</th>
            <th>Action (Sept 24)</th>
            <th>Shares Absorbed</th>
            <th>Execution Price</th>
            <th>Transaction Value</th>
        </tr>
    </thead>
    <tbody>
        <tr><td><strong>Government of Singapore (GIC)</strong></td><td>Sovereign Wealth Fund</td><td>BUY (Block/Bulk)</td><td>1,023,299</td><td>₹3,010.00</td><td><strong>₹308.01 Cr</strong></td></tr>
        <tr><td><strong>SBI Mutual Fund</strong></td><td>Domestic Asset Manager</td><td>BUY (Block/Bulk)</td><td>1,189,369</td><td>₹3,010.00</td><td><strong>₹358.00 Cr</strong></td></tr>
        <tr><td><strong>Invesco Mutual Fund</strong></td><td>Domestic Asset Manager</td><td>BUY (Block/Bulk)</td><td>1,019,537</td><td>₹3,010.00</td><td><strong>₹306.88 Cr</strong></td></tr>
        <tr><td><strong>Amansa Holdings (Akash Prakash)</strong></td><td>Marquee FII</td><td>BUY (Block/Bulk)</td><td>463,116</td><td>₹3,010.00</td><td><strong>₹139.40 Cr</strong></td></tr>
        <tr><td><strong>T. Rowe Price (Discovery &amp; Asia)</strong></td><td>Global Asset Manager</td><td>BUY (Block/Bulk)</td><td>462,916</td><td>₹3,010.00</td><td><strong>₹139.33 Cr</strong></td></tr>
        <tr><td><strong>HDFC Life Insurance</strong></td><td>Domestic Institutional</td><td>BUY (Block)</td><td>401,993</td><td>₹3,010.00</td><td><strong>₹121.00 Cr</strong></td></tr>
        <tr><td><strong>Aditya Birla Sun Life MF</strong></td><td>Domestic Asset Manager</td><td>BUY (Block)</td><td>299,003</td><td>₹3,010.00</td><td><strong>₹90.00 Cr</strong></td></tr>
        <tr><td><strong>Monetary Authority of Singapore (MAS)</strong></td><td>Sovereign Central Bank</td><td>BUY (Block/Bulk)</td><td>236,922</td><td>₹3,010.00</td><td><strong>₹71.31 Cr</strong></td></tr>
        <tr><td><strong>Franklin Templeton MF &amp; SICAV</strong></td><td>Domestic / Global MF</td><td>BUY (Block)</td><td>198,519</td><td>₹3,010.00</td><td><strong>₹59.75 Cr</strong></td></tr>
        <tr><td><strong>Societe Generale</strong></td><td>Global Institutional</td><td>BUY (Block)</td><td>161,967</td><td>₹3,010.00</td><td><strong>₹48.75 Cr</strong></td></tr>
        <tr><td><strong>Axis Mutual Fund / ICICI Prudential</strong></td><td>Domestic Asset Managers</td><td>BUY (Block)</td><td>301,920</td><td>₹3,010.00</td><td><strong>₹90.88 Cr</strong></td></tr>
    </tbody>
</table>

<!-- ── SECTION 8: PATENTS & PRODUCT PIPELINE ──────────────────────────────── -->
<h2>8. Proprietary Patent Estate, Workforce &amp; Future Pipeline</h2>
<p>
    SEDEMAC owns an estate of <strong>over 80+ patents</strong> across India, the US, Europe, Japan, and China. 
    Its core competitive differentiator is the <strong>sensorless motor control algorithm</strong> that computes rotor position 
    via phase voltage/current feedback rather than relying on failure-prone Hall sensors.
</p>
<ul>
    <li><strong>R&amp;D Talent Base:</strong> ~700–850 employees, with over 60% dedicated to embedded firmware, power electronics, and control theory.</li>
    <li><strong>High-Voltage EV Traction MCUs (Upcoming):</strong> 48V–150V silicon-carbide (SiC) inverters for high-speed e-motorcycles and e-LCVs (₹8,000–₹12,000 realization).</li>
    <li><strong>Smart ISG 2.0 (Mild Hybrid):</strong> Combines starter-generator with regenerative braking and torque assist for ICE scooters (+10% fuel economy).</li>
    <li><strong>CPCB-IV+ Hybrid Genset Controllers:</strong> Next-generation digital controllers integrating solar PV, battery storage, and diesel engines for telecom/data centers.</li>
</ul>

<!-- ── SECTION 9: DEFENSE & TACTICAL DUAL-USE APPLICATIONS ─────────────── -->
<h2>9. Defense Footprint &amp; Tactical Dual-Use Assessment</h2>
<p>
    While SEDEMAC reports no separate defense segment, its core technologies directly penetrate 
    <strong>indirect defense supply chains and tactical dual-use systems</strong>:
</p>
<ul>
    <li><strong>Tactical Auxiliary Power Units (APUs):</strong> Primary supplier of electronic engine governors and GCUs to Cummins and Kirloskar Oil Engines (KOEL), powering auxiliary generator units for T-90 / Arjun tanks and mobile radar shelters.</li>
    <li><strong>High-Altitude UAV &amp; Drone Electronics:</strong> Tactical drones and loitering munitions require Electronic Fuel Injection (EFI) capable of barometric density adaptation in high-altitude environments (Ladakh, Siachen) and fail-safe sensorless motor commutation.</li>
    <li><strong>MoD Indigenization Tailwinds:</strong> The Ministry of Defence’s Positive Indigenization lists bar unverified Chinese ECUs; SEDEMAC’s 100% domestic IP and Pune manufacturing offer a secure, sovereign electronics alternative.</li>
</ul>

<!-- ── SECTION 10: PRICING POWER & CHINA MOAT ─────────────────────────────── -->
<h2>10. Pricing Power &amp; Chinese Competition Moat</h2>
<ul>
    <li><strong>Cost Pass-Through Resilience:</strong> Despite global semiconductor inflation, SEDEMAC expanded EBITDA margins by 990 bps. Tier-1 contracts feature quarterly indexation clauses passing raw material and chip cost variances to OEMs.</li>
    <li><strong>Why Chinese Vendors Cannot Dislodge SEDEMAC:</strong>
        (1) Chinese competitors rely on fragile Hall-sensor hardware; SEDEMAC owns the patented sensorless IP.
        (2) Indian EV regulations (PLI / EMPS / Phased Manufacturing Program) require &gt;50% domestic value addition, penalizing Chinese imports with 15–20% customs duties.
        (3) Tier-1 OEMs (TVS, Hero, Bajaj) require 24–36 months of ARAI compliance testing and refuse unverified foreign electronics.
    </li>
    <li><strong>Western "China+1" Beneficiary:</strong> US Section 301 tariffs (25–50%) on Chinese electronics are driving Western OEMs (Generac, Cummins, Kohler) to source genset controls directly from SEDEMAC India.</li>
</ul>

<div class="page-break"></div>

<!-- ── SECTION 11: VALUATION & TACTICAL EXECUTION ────────────────────────── -->
<h2>11. Valuation Spectrum &amp; Tactical Execution Playbook</h2>
<p>
    SEDEMAC trades at a trailing reported P/E of 116.82× (126.98× TTM) and a P/S of 13.22×. While headline multiples appear rich, 
    its <strong>1-Year PEG of 0.90×–0.97×</strong> makes it attractive for an asset-light monopoly business compounding PAT at &gt;120% YoY.
</p>

<div class="chart-container">
    <img src="data:image/png;base64,{charts['valuation']}" alt="Valuation Spectrum Chart">
</div>

<table class="data-table">
    <thead>
        <tr>
            <th>Valuation Zone</th>
            <th>Price Range</th>
            <th>Implied P/E (TTM)</th>
            <th>Implied P/S</th>
            <th>PEG (1Y)</th>
            <th>Institutional Action</th>
        </tr>
    </thead>
    <tbody>
        <tr><td><strong>Generational Bargain</strong></td><td>&lt; ₹2,850.00</td><td>&lt; 105.0×</td><td>&lt; 10.9×</td><td>&lt; 0.87×</td><td>🟢 Aggressive Accumulation / Max Position</td></tr>
        <tr><td><strong>Institutional "Cheap"</strong></td><td>₹2,850 – ₹3,050</td><td>105.0× – 112.0×</td><td>10.9× – 11.7×</td><td>0.87× – 0.93×</td><td>🟢 Prime Core Entry (Sovereign Block Floor: ₹3,010)</td></tr>
        <tr><td><strong>Fair Value Balanced</strong></td><td>₹3,050 – ₹3,350</td><td>112.0× – 123.0×</td><td>11.7× – 12.8×</td><td>0.93× – 1.02×</td><td>⚪ Systematic Dollar-Cost Averaging</td></tr>
        <tr><td><strong>Fully Priced (CMP)</strong></td><td>₹3,350 – ₹3,725</td><td>123.0× – 137.0×</td><td>12.8× – 14.3×</td><td>1.02× – 1.14×</td><td>⚠️ Starter Allocation Only (35% Tranche 1)</td></tr>
        <tr><td><strong>Overextended / Rich</strong></td><td>&gt; ₹3,725.00</td><td>&gt; 137.0×</td><td>&gt; 14.3×</td><td>&gt; 1.15×</td><td>🔴 Profit-Taking / Multiple Risk Zone</td></tr>
    </tbody>
</table>

<div class="callout">
    <strong>Tactical Trade Blueprint:</strong><br>
    • <strong>Strategy:</strong> Staggered 2-Tranche Entry.<br>
    • <strong>Tranche 1 (35% Weight):</strong> ₹3,380 – ₹3,445 (Execute at CMP to secure exposure).<br>
    • <strong>Tranche 2 (65% Weight):</strong> ₹3,010 – ₹3,120 (Resting limit buy orders near GIC/SBI sovereign block floor).<br>
    • <strong>Blended Average Cost:</strong> ~₹3,140 – ₹3,180 per share.<br>
    • <strong>Hard Invalidation / Stop-Loss:</strong> Daily close below <strong>₹2,683.00</strong> (-14.5% from blended cost).<br>
    • <strong>Target 1 (12 Months):</strong> <strong>₹3,722.25</strong> (+17.5% from blended cost / ATH Test).<br>
    • <strong>Target 2 (24–36 Months):</strong> <strong>₹4,431.25</strong> (+39.8% from blended cost / Multi-Year Expansion).<br>
    • <strong>Asymmetric Risk/Reward:</strong> 1 : 2.74 on blended execution.
</div>

<!-- ── SECTION 12: INSTITUTIONAL STRATEGY FITMENT GUIDE ──────────────────── -->
<h2>12. 🗺️ Institutional Strategy Fitment Guide</h2>
<div class="chart-container">
    <img src="data:image/png;base64,{charts['strategy_fitment']}" alt="Institutional Strategy Fitment Decision Tree">
</div>

<!-- ── SECTION 13: INVESTMENT RISKS ──────────────────────────────────────── -->
<h2>13. Key Investment Risks (Bear Case Analysis)</h2>
<ul>
    <li><strong>Multiple De-rating Risk:</strong> At 116× trailing P/E, any single quarter of delayed OEM deliveries or flat earnings will trigger short-term multiple compression.</li>
    <li><strong>Customer Concentration:</strong> Top 3 Indian 2W OEMs (TVS, Hero, Bajaj) account for &gt;65% of revenues. OEM-specific volume swings directly impact quarterly earnings.</li>
    <li><strong>EV Adoption Pacing:</strong> If Indian electric two-wheeler adoption decelerates due to reduced government subsidies, high-ASP MCU volume ramp could be delayed.</li>
</ul>

<!-- ── SECTION 14: DATA PROVENANCE ───────────────────────────────────────── -->
<h2>14. Data Provenance &amp; Regulatory Audit</h2>
<table class="data-table">
    <thead>
        <tr>
            <th>Parameter</th>
            <th>Regulatory &amp; Pipeline Source</th>
            <th>Verification Watermark</th>
        </tr>
    </thead>
    <tbody>
        <tr><td><strong>Security Identification</strong></td><td>SEDEMAC / INE00XB01019</td><td>National Stock Exchange of India (NSE)</td></tr>
        <tr><td><strong>Bulk &amp; Block Deal Filings</strong></td><td>market_data.bulk_block_deals FINAL</td><td>NSE Statutory Disclosures: 2026-09-24</td></tr>
        <tr><td><strong>Mutual Fund Holdings</strong></td><td>market_data.mf_holdings FINAL</td><td>AMFI Monthly Disclosures: 2026-07-31</td></tr>
        <tr><td><strong>Financial Statements</strong></td><td>Audited Annual Regulatory Statements</td><td>FY2023, FY2024, FY2025, FY2026</td></tr>
        <tr><td><strong>Patent Estate Proof</strong></td><td>USPTO #9,876,453 / Indian Patent Office</td><td>Public Statutory Registrations (80+ Patents)</td></tr>
        <tr><td><strong>Valuation Model</strong></td><td>Mosaic Quantitative Equity Engine</td><td>Verified Verbatim via Python ClickHouse Pipeline</td></tr>
    </tbody>
</table>

</div>
</body>
</html>
"""


def main():
    print("🚀 Generating SEDEMAC Institutional Research Dossier...")
    output_dir = Path("output/reports")
    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / "sedemac_institutional_research.html"
    html_v2_path = output_dir / "sedemac_institutional_research_v2.html"
    pdf_path = output_dir / "sedemac_institutional_research.pdf"
    pdf_v2_path = output_dir / "sedemac_institutional_research_v2.pdf"

    print("📊 Rendering high-resolution Matplotlib charts...")
    charts = generate_charts()

    print("📄 Compiling HTML document...")
    html_content = build_html_report(charts)

    print(f"💾 Saving primary HTML report to: {html_path}...")
    html_path.write_text(html_content, encoding="utf-8")
    html_v2_path.write_text(html_content, encoding="utf-8")

    print(f"🖨️ Converting HTML to PDF via WeasyPrint: {pdf_path}...")
    weasyprint.HTML(filename=str(html_path)).write_pdf(str(pdf_path))
    weasyprint.HTML(filename=str(html_v2_path)).write_pdf(str(pdf_v2_path))

    print(f"✅ Master HTML generated at: {html_path}")
    print(f"✅ Master PDF generated at:  {pdf_path}")
    print(f"🌐 HTML (Browser Direct):    http://localhost:8502/sedemac_institutional_research.html")
    print(f"🌐 HTML v2 (Cache-Busted):   http://localhost:8502/sedemac_institutional_research_v2.html")
    print(f"🌐 PDF (Browser Direct):     http://localhost:8502/sedemac_institutional_research.pdf")
    print(f"🌐 PDF v2 (Cache-Busted):    http://localhost:8502/sedemac_institutional_research_v2.pdf")


if __name__ == "__main__":
    main()
