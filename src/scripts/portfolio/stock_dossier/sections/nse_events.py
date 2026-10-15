from __future__ import annotations

from datetime import date, timedelta

from ..base import DossierSection
from ..context import DossierContext


class NseAnnouncementsSection(DossierSection):
    key = "nse_events"
    title = "Official SEBI / NSE Corporate Events"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.nse_announcements import fetch_corporate_announcements

        today_dt = date.today()
        anns = fetch_corporate_announcements(ctx.symbol, today_dt - timedelta(days=ctx.days), today_dt)
        if not anns:
            return f"*No material NSE announcements found in the last {ctx.days} days.*"

        lines = ["| Date | Category | Summary |", "| :--- | :--- | :--- |"]
        for ann in sorted(anns, key=lambda a: a.get("published_at", ""), reverse=True)[:15]:
            date_str = str(ann.get("published_at", ""))[:10]
            cat = str(ann.get("category", "")).strip()
            desc = str(ann.get("description") or ann.get("title") or "").strip()[:120].replace("|", "–")
            lines.append(f"| {date_str} | {cat} | {desc} |")
        return "\n".join(lines)
