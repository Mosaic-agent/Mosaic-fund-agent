"""
src/data_importer/fetchers/bulk_block_deals_fetcher.py
────────────────────────────────────────────────────────
Fetches official NSE Bulk and Block deal transactions using nselib.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
import pandas as pd

log = logging.getLogger(__name__)


def _clean_float(val: Any) -> float:
    if val is None or pd.isna(val):
        return 0.0
    try:
        s = str(val).replace(',', '').strip()
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse_nse_date(val: Any) -> date | None:
    if not val or pd.isna(val):
        return None
    s = str(val).strip()
    for fmt in ('%d-%b-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def fetch_nse_bulk_and_block_deals(period: str = '1Y') -> list[dict[str, Any]]:
    try:
        from nselib import capital_market
    except ImportError:
        log.error('nselib is not installed.')
        return []

    records: list[dict[str, Any]] = []

    # 1. Bulk Deals
    try:
        df_bulk = capital_market.bulk_deal_data(period=period)
        if df_bulk is not None and not df_bulk.empty:
            df_bulk.columns = [c.strip().lstrip('﻿').strip('"') for c in df_bulk.columns]
            for _, row in df_bulk.iterrows():
                deal_date = _parse_nse_date(row.get('Date'))
                if not deal_date:
                    continue
                symbol = str(row.get('Symbol', '')).strip().upper()
                if not symbol:
                    continue
                qty = _clean_float(row.get('QuantityTraded'))
                price = _clean_float(row.get('TradePrice/Wght.Avg.Price'))
                val_cr = round((qty * price) / 1e7, 4)
                buy_sell = str(row.get('Buy/Sell', '')).strip().upper()

                records.append({
                    'deal_date': deal_date,
                    'deal_type': 'BULK',
                    'symbol': symbol,
                    'security_name': str(row.get('SecurityName', '')).strip(),
                    'client_name': str(row.get('ClientName', '')).strip(),
                    'buy_sell': buy_sell if buy_sell in ('BUY', 'SELL') else 'BUY',
                    'quantity': qty,
                    'trade_price': price,
                    'value_cr': val_cr,
                    'remarks': str(row.get('Remarks', '')).strip() if pd.notna(row.get('Remarks')) else '',
                })
    except Exception as exc:
        log.warning('NSE bulk deals fetch failed: %s', exc)

    # 2. Block Deals
    try:
        df_block = capital_market.block_deals_data(period=period)
        if df_block is not None and not df_block.empty:
            df_block.columns = [c.strip().lstrip('﻿').strip('"') for c in df_block.columns]
            for _, row in df_block.iterrows():
                deal_date = _parse_nse_date(row.get('Date'))
                if not deal_date:
                    continue
                symbol = str(row.get('Symbol', '')).strip().upper()
                if not symbol:
                    continue
                qty = _clean_float(row.get('QuantityTraded'))
                price = _clean_float(row.get('TradePrice/Wght.Avg.Price'))
                val_cr = round((qty * price) / 1e7, 4)
                buy_sell = str(row.get('Buy/Sell', '')).strip().upper()

                records.append({
                    'deal_date': deal_date,
                    'deal_type': 'BLOCK',
                    'symbol': symbol,
                    'security_name': str(row.get('SecurityName', '')).strip(),
                    'client_name': str(row.get('ClientName', '')).strip(),
                    'buy_sell': buy_sell if buy_sell in ('BUY', 'SELL') else 'BUY',
                    'quantity': qty,
                    'trade_price': price,
                    'value_cr': val_cr,
                    'remarks': str(row.get('Remarks', '')).strip() if pd.notna(row.get('Remarks')) else '',
                })
    except Exception as exc:
        log.warning('NSE block deals fetch failed: %s', exc)

    return records
