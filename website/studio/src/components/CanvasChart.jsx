import React, { useRef, useEffect, useState } from "react";

export default function CanvasChart({ data = [], color = "cyan", height = 180 }) {
  const canvasRef = useRef(null);
  const [hoverIndex, setHoverIndex] = useState(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });

  // Map color names to hex codes & gradients
  const colors = {
    cyan: { stroke: "#00ffcc", fillStart: "rgba(0, 255, 204, 0.25)", fillEnd: "rgba(0, 255, 204, 0.0)" },
    magenta: { stroke: "#ff0066", fillStart: "rgba(255, 0, 102, 0.25)", fillEnd: "rgba(255, 0, 102, 0.0)" },
    purple: { stroke: "#9933ff", fillStart: "rgba(153, 51, 255, 0.25)", fillEnd: "rgba(153, 51, 255, 0.0)" },
    gold: { stroke: "#f59e0b", fillStart: "rgba(245, 158, 11, 0.25)", fillEnd: "rgba(245, 158, 11, 0.0)" },
    green: { stroke: "#10b981", fillStart: "rgba(16, 185, 129, 0.25)", fillEnd: "rgba(16, 185, 129, 0.0)" }
  };

  const activeColor = colors[color] || colors.cyan;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Handle high DPI displays (retina screens)
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);

    const w = rect.width;
    const h = height;

    // Clear canvas
    ctx.clearRect(0, 0, w, h);

    if (data.length === 0) {
      // Draw loading/empty state
      ctx.fillStyle = "#4a5568";
      ctx.font = "12px sans-serif";
      ctx.textAlign = "center";
      ctx.fillText("No data available", w / 2, h / 2);
      return;
    }

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    
    // Grid settings
    const paddingLeft = 10;
    const paddingRight = 10;
    const paddingTop = 15;
    const paddingBottom = 15;
    
    const chartW = w - paddingLeft - paddingRight;
    const chartH = h - paddingTop - paddingBottom;

    // Draw horizontal grid lines
    ctx.strokeStyle = "rgba(255, 255, 255, 0.04)";
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 0; i <= gridLines; i++) {
      const y = paddingTop + (chartH / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(paddingLeft, y);
      ctx.lineTo(w - paddingRight, y);
      ctx.stroke();
    }

    // Helper to map data index & value to X/Y canvas coordinates
    const getX = (index) => paddingLeft + (chartW / (data.length - 1)) * index;
    const getY = (value) => h - paddingBottom - (chartH * (value - min)) / range;

    // Draw Area Gradient Path
    const grad = ctx.createLinearGradient(0, paddingTop, 0, h - paddingBottom);
    grad.addColorStop(0, activeColor.fillStart);
    grad.addColorStop(1, activeColor.fillEnd);

    ctx.beginPath();
    ctx.moveTo(getX(0), h - paddingBottom);
    for (let i = 0; i < data.length; i++) {
      ctx.lineTo(getX(i), getY(data[i]));
    }
    ctx.lineTo(getX(data.length - 1), h - paddingBottom);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();

    // Draw Price Line Path
    ctx.beginPath();
    ctx.moveTo(getX(0), getY(data[0]));
    for (let i = 1; i < data.length; i++) {
      ctx.lineTo(getX(i), getY(data[i]));
    }
    ctx.strokeStyle = activeColor.stroke;
    ctx.lineWidth = 2.5;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.stroke();

    // Draw Hover Details
    if (hoverIndex !== null && hoverIndex >= 0 && hoverIndex < data.length) {
      const hx = getX(hoverIndex);
      const hy = getY(data[hoverIndex]);

      // Vertical crosshair line
      ctx.strokeStyle = "rgba(255, 255, 255, 0.15)";
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(hx, paddingTop);
      ctx.lineTo(hx, h - paddingBottom);
      ctx.stroke();
      ctx.setLineDash([]); // Reset line dash

      // Glow dot on data point
      ctx.beginPath();
      ctx.arc(hx, hy, 6, 0, 2 * Math.PI);
      ctx.fillStyle = activeColor.stroke;
      ctx.shadowBlur = 10;
      ctx.shadowColor = activeColor.stroke;
      ctx.fill();
      ctx.shadowBlur = 0; // Reset shadow

      ctx.beginPath();
      ctx.arc(hx, hy, 3, 0, 2 * Math.PI);
      ctx.fillStyle = "#ffffff";
      ctx.fill();

      // Tooltip Box
      const tooltipVal = data[hoverIndex].toFixed(2);
      ctx.font = "bold 11px JetBrains Mono, sans-serif";
      const valWidth = ctx.measureText(tooltipVal).width;
      
      const boxW = valWidth + 16;
      const boxH = 22;
      let boxX = hx - boxW / 2;
      let boxY = hy - 30;

      // Bound checking
      if (boxX < 4) boxX = 4;
      if (boxX + boxW > w - 4) boxX = w - boxW - 4;
      if (boxY < 4) boxY = hy + 12;

      ctx.fillStyle = "rgba(11, 15, 20, 0.9)";
      ctx.strokeStyle = activeColor.stroke;
      ctx.lineWidth = 1;
      
      // Draw rounded tooltip rect
      ctx.beginPath();
      ctx.roundRect(boxX, boxY, boxW, boxH, 4);
      ctx.fill();
      ctx.stroke();

      ctx.fillStyle = "#ffffff";
      ctx.textAlign = "center";
      ctx.fillText(tooltipVal, boxX + boxW / 2, boxY + 15);
    }
  }, [data, color, height, hoverIndex]);

  // Handle Mouse Hover Interactions
  const handleMouseMove = (e) => {
    const canvas = canvasRef.current;
    if (!canvas || data.length === 0) return;

    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    setMousePos({ x, y });

    const chartW = rect.width - 20; // accounting for 10px padding left/right
    const step = chartW / (data.length - 1);
    
    // Find closest index
    const index = Math.round((x - 10) / step);
    if (index >= 0 && index < data.length) {
      setHoverIndex(index);
    } else {
      setHoverIndex(null);
    }
  };

  const handleMouseLeave = () => {
    setHoverIndex(null);
  };

  return (
    <div style={{ position: "relative", width: "100%" }}>
      <canvas
        ref={canvasRef}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        style={{
          width: "100%",
          height: `${height}px`,
          display: "block",
          cursor: "crosshair"
        }}
      />
    </div>
  );
}
