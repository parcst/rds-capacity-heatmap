"""Core scan logic: tunnel to each RDS instance and query table sizes."""

from __future__ import annotations

import logging
import subprocess
import threading
from collections.abc import Callable

import pymysql

from .models import (
    DatabaseEntry,
    HeatmapStatus,
    InstanceScanError,
    RED_BYTES,
    ScanQuery,
    SYSTEM_SCHEMAS,
    TableSizeResult,
    YELLOW_BYTES,
)
from .teleport import (
    TeleportTunnel,
    find_tsh,
    list_mysql_databases,
    start_tunnel,
    stop_tunnel,
)

logger = logging.getLogger(__name__)

_SIZE_QUERY = """
SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TABLE_ROWS,
       COALESCE(DATA_LENGTH, 0) AS DATA_LENGTH,
       COALESCE(INDEX_LENGTH, 0) AS INDEX_LENGTH,
       (COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)) AS total_size
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA NOT IN ({placeholders})
ORDER BY total_size DESC
"""

# ---------------------------------------------------------------------------
# Tunnel registry — tracks all active tunnels for cleanup on cancel/shutdown
# ---------------------------------------------------------------------------

_registry_lock = threading.Lock()
_active_tunnels: dict[str, TeleportTunnel] = {}  # db_name -> tunnel


def _register_tunnel(tunnel: TeleportTunnel) -> None:
    with _registry_lock:
        _active_tunnels[tunnel.db_name] = tunnel


def _unregister_tunnel(db_name: str) -> None:
    with _registry_lock:
        _active_tunnels.pop(db_name, None)


def cleanup_all() -> None:
    """Force-kill all tracked tunnels and logout their db sessions.

    Safe to call from any thread. Idempotent — double-calls are harmless.
    """
    with _registry_lock:
        tunnels = list(_active_tunnels.values())
        _active_tunnels.clear()

    if not tunnels:
        return

    logger.info("Cleanup: killing %d active tunnel(s)", len(tunnels))
    try:
        tsh = find_tsh()
    except FileNotFoundError:
        # Can't find tsh — just kill the processes directly
        for tunnel in tunnels:
            try:
                tunnel.process.kill()
            except Exception:
                pass
        return

    for tunnel in tunnels:
        # Force-kill the tunnel process
        try:
            tunnel.process.kill()
            tunnel.process.wait(timeout=5)
        except Exception:
            pass

        # Logout the db session
        try:
            subprocess.run(
                [tsh, "db", "logout", tunnel.db_name],
                capture_output=True,
                timeout=10,
            )
        except Exception:
            logger.warning("Cleanup: failed to logout %s", tunnel.db_name)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _classify(total_bytes: int) -> HeatmapStatus:
    """Classify a table's total size into a heatmap status."""
    if total_bytes >= RED_BYTES:
        return HeatmapStatus.RED
    if total_bytes >= YELLOW_BYTES:
        return HeatmapStatus.YELLOW
    return HeatmapStatus.GREEN


def list_instances(cluster: str) -> list[DatabaseEntry]:
    """List all MySQL instances on *cluster* via tsh. Blocking call."""
    tsh = find_tsh()
    raw_dbs = list_mysql_databases(tsh, cluster)
    return [
        DatabaseEntry(
            name=d["name"],
            uri=d["uri"],
            account_id=d["account_id"],
            region=d["region"],
            instance_id=d["instance_id"],
        )
        for d in raw_dbs
    ]


def scan_instance(
    db_entry: DatabaseEntry,
    query: ScanQuery,
    on_step: Callable[[str], None] | None = None,
) -> list[TableSizeResult] | InstanceScanError:
    """Connect to a single RDS instance via tunnel and query all table sizes.

    Blocking call — run via run_in_executor().
    *on_step* is called with a human-readable status string at each major step.
    The tunnel is registered in the global registry so cleanup_all() can kill it.
    """
    def _step(msg: str) -> None:
        if on_step is not None:
            on_step(msg)

    tsh = find_tsh()
    tunnel = None
    try:
        _step("Authenticating and opening tunnel")
        tunnel = start_tunnel(
            tsh,
            db_entry.name,
            query.db_user,
            cluster=query.cluster,
        )
        _register_tunnel(tunnel)

        _step("Connecting to MySQL")
        conn = pymysql.connect(
            host=tunnel.host,
            port=tunnel.port,
            user=tunnel.db_user,
            database="information_schema",
            connect_timeout=10,
            read_timeout=30,
        )
        try:
            with conn.cursor() as cursor:
                _step("Querying partition sizes")
                placeholders = ", ".join(["%s"] * len(SYSTEM_SCHEMAS))
                sql = _SIZE_QUERY.format(placeholders=placeholders)
                cursor.execute(sql, tuple(SYSTEM_SCHEMAS))
                rows = cursor.fetchall()
        finally:
            _step("Closing MySQL connection")
            conn.close()

        _step(f"Processing {len(rows)} partitions")
        results: list[TableSizeResult] = []
        for row in rows:
            schema, table_name, partition_name, table_rows, data_len, index_len, total = row
            data_size = data_len or 0
            index_size = index_len or 0
            total_size = total or 0
            row_count = table_rows or 0

            results.append(
                TableSizeResult(
                    connection_name=db_entry.name,
                    database=schema,
                    table=table_name,
                    partition_name=partition_name,
                    data_size=data_size,
                    index_size=index_size,
                    total_size=total_size,
                    table_rows=row_count,
                    status=_classify(total_size),
                )
            )

        _step("Closing tunnel")
        return results

    except Exception as e:
        logger.exception("Error scanning %s", db_entry.name)
        return InstanceScanError(
            connection_name=db_entry.name,
            error_message=str(e),
        )

    finally:
        if tunnel is not None:
            _unregister_tunnel(tunnel.db_name)
            try:
                stop_tunnel(tsh, tunnel)
            except Exception:
                logger.warning("Failed to stop tunnel for %s", db_entry.name)
