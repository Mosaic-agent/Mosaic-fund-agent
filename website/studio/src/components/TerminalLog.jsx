import React, { useEffect, useRef } from "react";

export default function TerminalLog({ logs = "" }) {
  const terminalEndRef = useRef(null);

  useEffect(() => {
    if (terminalEndRef.current) {
      terminalEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  return (
    <div className="terminal-log">
      {logs || "Ready. Select pipelines and launch."}
      <div ref={terminalEndRef} />
    </div>
  );
}
