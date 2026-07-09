from dataclasses import dataclass
from typing import Any, Optional
from datetime import date
from src.commands.base import Command

@dataclass
class PremiumAlertsCommand(Command):
    symbols: list[str]
    lookback_days: int
    z_threshold: float
    min_snapshots: int
    log_signals: bool

    def execute(self) -> dict[str, Any]:
        from src.db.pool import get_pool
        from src.tools.premium_alerts import check_premium_alerts

        pool = get_pool()
        ch = pool.get_client()

        try:
            results = check_premium_alerts(
                ch_client=ch,
                symbols=self.symbols,
                lookback_days=self.lookback_days,
                z_threshold=self.z_threshold,
                good_entry_threshold=self.z_threshold + 0.5,
                min_snapshots=self.min_snapshots,
            )
        finally:
            ch.close()

        logged_count = 0
        if self.log_signals and results:
            today_str = date.today().isoformat()
            for r in results:
                if r.get("z_score") is None:
                    continue
                pool.execute(
                    f"INSERT INTO market_data.premium_signal_log "
                    f"(as_of, symbol, current_prem, ou_mu, half_life_days, "
                    f"expected_reversion_pct, net_pnl_stcg_pct, action, "
                    f"ou_available, is_profitable_after_costs, signal_source) VALUES "
                    f"('{today_str}', '{r['symbol']}', "
                    f"{r.get('latest_premium') or 0}, "
                    f"{r.get('ou_mu') or 0}, "
                    f"{r.get('half_life_days') or 0}, "
                    f"{r.get('expected_reversion_pct') or 0}, "
                    f"{r.get('net_pnl_stcg_pct') or 0}, "
                    f"'{r.get('action', '')}', "
                    f"{1 if r.get('ou_available') else 0}, "
                    f"{1 if r.get('is_profitable_after_costs') else 0}, "
                    f"'premium_alerts')"
                )
                logged_count += 1

        return {
            "results": results,
            "logged_count": logged_count
        }
