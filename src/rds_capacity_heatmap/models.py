"""Data models and threshold constants for RDS capacity heatmap."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

# RDS MySQL hard storage limit: 16 TB
RDS_MYSQL_HARD_LIMIT = 16 * 1024 * 1024 * 1024 * 1024  # 17,592,186,044,416 bytes

# Heatmap thresholds
RED_THRESHOLD = 0.75  # >= 75% of 16 TB
YELLOW_THRESHOLD = 0.60  # >= 60% of 16 TB
RED_BYTES = int(RDS_MYSQL_HARD_LIMIT * RED_THRESHOLD)  # ~12 TB
YELLOW_BYTES = int(RDS_MYSQL_HARD_LIMIT * YELLOW_THRESHOLD)  # ~9.6 TB

# Minimum number of results to display (top greens fill up if fewer flagged)
MIN_DISPLAY_RESULTS = 10

# System schemas to exclude from scanning
SYSTEM_SCHEMAS = frozenset({
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
})


class HeatmapStatus(str, Enum):
    RED = "red"
    YELLOW = "yellow"
    GREEN = "green"
    ERROR = "error"


@dataclass
class DatabaseEntry:
    name: str
    uri: str
    account_id: str
    region: str
    instance_id: str


@dataclass
class TableSizeResult:
    connection_name: str
    database: str
    table: str
    partition_name: str | None
    data_size: int
    index_size: int
    total_size: int
    table_rows: int
    status: HeatmapStatus


@dataclass
class InstanceScanError:
    connection_name: str
    error_message: str


@dataclass
class ScanQuery:
    cluster: str
    db_user: str


@dataclass
class ScanState:
    query: ScanQuery
    results: list[TableSizeResult] = field(default_factory=list)
    errors: list[InstanceScanError] = field(default_factory=list)
    total_instances: int = 0
    completed: bool = False

    @property
    def red_results(self) -> list[TableSizeResult]:
        return [r for r in self.results if r.status == HeatmapStatus.RED]

    @property
    def yellow_results(self) -> list[TableSizeResult]:
        return [r for r in self.results if r.status == HeatmapStatus.YELLOW]

    @property
    def flagged_results(self) -> list[TableSizeResult]:
        return [r for r in self.results if r.status in (HeatmapStatus.RED, HeatmapStatus.YELLOW)]

    @property
    def green_results(self) -> list[TableSizeResult]:
        return [r for r in self.results if r.status == HeatmapStatus.GREEN]

    def topup_greens(self, min_total: int = MIN_DISPLAY_RESULTS) -> list[TableSizeResult]:
        """Return the largest green results needed to reach *min_total* displayed rows."""
        flagged_count = len(self.flagged_results)
        if flagged_count >= min_total:
            return []
        needed = min_total - flagged_count
        greens = sorted(self.green_results, key=lambda r: r.total_size, reverse=True)
        return greens[:needed]

    @property
    def all_clear(self) -> bool:
        return self.completed and len(self.flagged_results) == 0 and len(self.errors) == 0
