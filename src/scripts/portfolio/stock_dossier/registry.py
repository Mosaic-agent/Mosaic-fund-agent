"""
src/scripts/portfolio/stock_dossier/registry.py
──────────────────────────────────────────────────
The extension point: to add a new dossier capability, subclass
`DossierSection` in a new sections/<name>.py file and append an instance here.
Nothing else in cli.py or report.py needs to change — they iterate this list
generically (same shape as SIGNAL_SOURCES in src/agents/signal_sources.py and
FETCHER_REGISTRY in src/data_importer/fetchers/adapters.py). List order is the
report's section order; numbering is synthesised from that order, so
inserting a new section anywhere just works.
"""

from __future__ import annotations

from .base import DossierSection
from .sections.anomalies import AnomalySection
from .sections.chart import PriceChartSection
from .sections.crossover import CrossoverBacktestSection
from .sections.dcf import DcfValuationSection
from .sections.deals import BulkBlockDealsSection
from .sections.dilution import DilutionAuditSection
from .sections.financials import FinancialsSection
from .sections.momentum import MomentumSection
from .sections.news import NewsSection
from .sections.nse_events import NseAnnouncementsSection
from .sections.obi import LiveOrderBookSection
from .sections.profile import ExecutiveProfileSection
from .sections.tactical import TacticalPlaybookSection
from .sections.valuation import ValuationUniverseSection
from .sections.whale import WhaleOwnershipSection

REGISTRY: list[DossierSection] = [
    ExecutiveProfileSection(),
    PriceChartSection(),
    FinancialsSection(),
    MomentumSection(),
    WhaleOwnershipSection(),
    BulkBlockDealsSection(),
    ValuationUniverseSection(),
    AnomalySection(),
    NseAnnouncementsSection(),
    NewsSection(),
    CrossoverBacktestSection(),
    DcfValuationSection(),
    DilutionAuditSection(),
    TacticalPlaybookSection(),
    LiveOrderBookSection(),
]
