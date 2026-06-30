import React, { useEffect, useRef, useState, useCallback } from "react";
import { createChart, AreaSeries } from "lightweight-charts";

const RANGES = [
  { label: "1W", days: 7 },
  { label: "1M", days: 30 },
  { label: "3M", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
  { label: "ALL", days: null },
];

const COLOR_MAP = {
  cyan:    { line: "#00E5C8", top: "rgba(0,229,200,0.18)",   bottom: "rgba(0,229,200,0.0)" },
  magenta: { line: "#FF0066", top: "rgba(255,0,102,0.18)",   bottom: "rgba(255,0,102,0.0)" },
  purple:  { line: "#8B5CF6", top: "rgba(139,92,246,0.18)", bottom: "rgba(139,92,246,0.0)" },
  gold:    { line: "#F59E0B", top: "rgba(245,158,11,0.18)",  bottom: "rgba(245,158,11,0.0)" },
  green:   { line: "#10B981", top: "rgba(16,185,129,0.18)", bottom: "rgba(16,185,129,0.0)" },
};

// data: [{ time: 'YYYY-MM-DD', value: number }, ...]
export default function TradingChart({
  data = [],
  color = "cyan",
  height = 195,
  title,
  price,
  change,
  isUp,
  defaultRange = "3M",
}) {
  const containerRef = useRef(null);
  const chartRef     = useRef(null);
  const seriesRef    = useRef(null);
  const [activeRange, setActiveRange] = useState(defaultRange);
  const [hoverPrice,  setHoverPrice]  = useState(null);
  const [hoverDate,   setHoverDate]   = useState(null);

  const c = COLOR_MAP[color] || COLOR_MAP.cyan;

  // ── Build chart once ──────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background:  { color: "transparent" },
        textColor:   "#475569",
        fontSize:    11,
        fontFamily:  "'JetBrains Mono', 'Courier New', monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.03)", style: 1 },
        horzLines: { color: "rgba(255,255,255,0.04)", style: 1 },
      },
      crosshair: {
        mode: 1,
        vertLine: {
          color: c.line,
          width: 1,
          style: 2,
          labelBackgroundColor: "#0D1117",
        },
        horzLine: {
          color: c.line,
          width: 1,
          style: 2,
          labelBackgroundColor: "#0D1117",
        },
      },
      rightPriceScale: {
        borderColor:    "rgba(255,255,255,0.06)",
        textColor:      "#475569",
        scaleMargins:   { top: 0.08, bottom: 0.08 },
      },
      timeScale: {
        borderColor:      "rgba(255,255,255,0.06)",
        textColor:        "#475569",
        timeVisible:      true,
        secondsVisible:   false,
        fixRightEdge:     true,
        tickMarkFormatter: (time) => {
          // lightweight-charts v5 passes time in the same type as the data's time field.
          // We use "YYYY-MM-DD" strings → time is a string here, not a number.
          let d;
          if (typeof time === "string") {
            d = new Date(time + "T00:00:00Z");
          } else if (typeof time === "object" && time !== null && "year" in time) {
            d = new Date(Date.UTC(time.year, time.month - 1, time.day));
          } else {
            d = new Date(time * 1000); // Unix timestamp fallback
          }
          return d.toLocaleDateString("en-IN", { day: "2-digit", month: "short" });
        },
      },
      handleScroll:  { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
      handleScale:   { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor:   c.line,
      topColor:    c.top,
      bottomColor: c.bottom,
      lineWidth:   2,
      priceLineVisible:          false,
      lastValueVisible:          true,
      crosshairMarkerVisible:    true,
      crosshairMarkerRadius:     4,
      crosshairMarkerBorderColor: c.line,
      crosshairMarkerBorderWidth: 2,
      crosshairMarkerBackgroundColor: "#0D1117",
    });

    // Live tooltip via crosshair subscription
    chart.subscribeCrosshairMove((param) => {
      if (!param.time || !param.seriesData) {
        setHoverPrice(null);
        setHoverDate(null);
        return;
      }
      const d = param.seriesData.get(series);
      if (d) {
        setHoverPrice(d.value);
        const t = param.time;
        let dt;
        if (typeof t === "string") {
          dt = new Date(t + "T00:00:00Z");
        } else if (typeof t === "object" && t !== null && "year" in t) {
          dt = new Date(Date.UTC(t.year, t.month - 1, t.day));
        } else {
          dt = new Date(t * 1000);
        }
        setHoverDate(dt.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" }));
      }
    });

    chartRef.current  = chart;
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current  = null;
      seriesRef.current = null;
    };
  }, [color]); // rebuild if color prop changes

  // ── Push data into series ─────────────────────────────────────────────────
  useEffect(() => {
    if (!seriesRef.current || data.length === 0) return;
    seriesRef.current.setData(data);
    applyRange(activeRange, data);
  }, [data]);

  // ── Range application ─────────────────────────────────────────────────────
  const applyRange = useCallback((range, src) => {
    const pts = src || data;
    if (!chartRef.current || pts.length === 0) return;
    const rangeObj = RANGES.find(r => r.label === range);
    if (!rangeObj || !rangeObj.days) {
      chartRef.current.timeScale().fitContent();
      return;
    }
    const lastTime = pts[pts.length - 1].time; // "YYYY-MM-DD"
    const toDate   = new Date(lastTime);
    const fromDate = new Date(toDate);
    fromDate.setDate(fromDate.getDate() - rangeObj.days);
    chartRef.current.timeScale().setVisibleRange({
      from: fromDate.toISOString().split("T")[0],
      to:   lastTime,
    });
  }, [data]);

  const handleRangeClick = (range) => {
    setActiveRange(range);
    applyRange(range);
  };

  // ── Derived display values ────────────────────────────────────────────────
  const displayPrice  = hoverPrice != null
    ? hoverPrice.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : price;
  const displayIsUp   = hoverPrice != null
    ? (data.length > 1 ? hoverPrice >= data[data.length - 1].value : true)
    : isUp;

  return (
    <div className="trading-chart-wrapper">
      {/* ── Header ──────────────────────────────────────────────────────── */}
      <div className="tchart-header">
        <div className="tchart-left">
          <div className="chart-title">{title}</div>
          <div className="tchart-price-row">
            {displayPrice && (
              <span className="chart-price">{displayPrice}</span>
            )}
            {change && hoverPrice == null && (
              <span className={`chart-change ${displayIsUp ? "change-up" : "change-down"}`}>
                {change}
              </span>
            )}
            {hoverDate && (
              <span className="tchart-hover-date">{hoverDate}</span>
            )}
          </div>
        </div>

        {/* ── Range picker ──────────────────────────────────────────────── */}
        <div className="range-picker">
          {RANGES.map(r => (
            <button
              key={r.label}
              className={`range-btn ${activeRange === r.label ? "active" : ""}`}
              style={activeRange === r.label ? { color: c.line, borderColor: c.line, background: `${c.top}` } : {}}
              onClick={() => handleRangeClick(r.label)}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Chart canvas ─────────────────────────────────────────────────── */}
      {data.length === 0 ? (
        <div className="shimmer" style={{ width: "100%", height, borderRadius: 6 }} />
      ) : (
        <div ref={containerRef} style={{ width: "100%", height }} />
      )}
    </div>
  );
}
