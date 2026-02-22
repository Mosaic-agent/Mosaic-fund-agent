from src.tools.comex_fetcher import get_comex_signals
import json

result = get_comex_signals(["XAU", "XAG", "HG"])
top = {k: v for k, v in result.items() if k != "commodities"}
print(json.dumps(top, indent=2))
print()
for sym, c in result.get("commodities", {}).items():
    p = c.get("change_pct")
    pstr = f"{p:+.3f}%" if p is not None else "N/A"
    print(f"{c['emoji']} {sym} {c['name']}: live=${c['live_price']:.4f}  prev=${c['prev_close'] or 0:.4f}  chg={pstr}  signal={c['signal']}")
