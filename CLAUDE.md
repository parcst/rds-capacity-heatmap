# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Create venv (requires Python 3.12+)
python3.12 -m venv .venv

# Install (dev mode)
.venv/bin/pip install -e "."

# Run the server (auto-reload enabled)
.venv/bin/python run.py
# or
.venv/bin/python -m rds_capacity_heatmap

# Server runs at http://127.0.0.1:8001
```

## Architecture

Web tool that scans every MySQL instance in a Teleport cluster and displays a heatmap of partitions approaching the 16 TB RDS MySQL storage limit (per-partition, not per-table). Partitions at >= 60% (Yellow) or >= 75% (Red) always shown. If fewer than 10 flagged results, the largest green partitions fill up to a minimum of 10 displayed rows (`MIN_DISPLAY_RESULTS`). Results stream in real-time via SSE, sorted largest-first. No session history — standalone scans only.

**Tech stack**: Python 3.12, FastAPI, Jinja2 templates, HTMX (CDN), vanilla JS for SSE, PyMySQL, sse-starlette.

### Heatmap Thresholds

| Status | Threshold | Bytes |
|--------|-----------|-------|
| Red | >= 75% of 16 TB | ~12 TB |
| Yellow | >= 60% of 16 TB | ~9.6 TB |
| Green | < 60% | (top greens shown to reach minimum 10 rows) |

### Routes

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Main page with cluster selector and scan button |
| POST | `/api/login` | Trigger `tsh login <cluster>` for SSO |
| GET | `/api/login-status` | HTMX poll (every 2s) to detect SSO completion |
| GET | `/api/scan` | **SSE endpoint** — streams results as each instance is scanned |
| GET | `/api/export-csv` | Download CSV of red/yellow results from last scan |

### Key Files

- `app.py` — FastAPI app, routes, SSE event stream, CSV export, `format_bytes` Jinja2 filter. Controls the scan loop directly (list instances, then scan each one). Uses `run_in_executor` + `thread_queue.Queue` polling to relay live step updates from the scanner thread as `step` SSE events. Module-level `_last_scan_state` holds results for CSV export.
- `scanner.py` — Two public functions: `list_instances()` discovers MySQL DBs on a cluster, `scan_instance()` tunnels to one instance and queries all partition sizes. Both are blocking and run via `run_in_executor`/`to_thread`. Accepts `on_step` callback to report granular progress (authenticating, connecting, querying, processing, closing). Queries `information_schema.PARTITIONS` (not TABLES) so thresholds apply per-partition — non-partitioned tables have a single partition row. **Note**: `innodb_stats_on_metadata` is GLOBAL-only on RDS instances and cannot be set at SESSION level — do not attempt `SET SESSION`.
- `teleport.py` — `tsh` CLI integration: find binary, list clusters/databases, start/stop tunnels, SSO login. Originally from `datatype-inspector`, modified to use `--proxy=` instead of `--cluster=` for `tsh db ls` and `tsh db login`/`tsh proxy db` (required when the target cluster isn't the active profile). **Note**: `tsh status` returns exit code 1 even when logged in — never use `check=True` with it.
- `models.py` — Dataclasses (`TableSizeResult`, `InstanceScanError`, `ScanQuery`, `ScanState`, `DatabaseEntry`), `HeatmapStatus` enum, threshold constants (`RDS_MYSQL_HARD_LIMIT`, `RED_BYTES`, `YELLOW_BYTES`), `SYSTEM_SCHEMAS` frozenset.

### Data Flow

1. User selects cluster, clicks Login, completes SSO. DB user is resolved automatically from `tsh status --format=json` — login validation is **cluster-aware** (checks both `active` and `profiles` arrays to find the matching cluster, not just that *some* session exists).
2. User clicks "Scan Cluster" — frontend opens `EventSource` to `/api/scan?cluster=...`
3. Backend lists all MySQL DBs on the cluster via `tsh db ls --format=json`
4. For each instance sequentially: open tunnel → PyMySQL connect → query `information_schema.PARTITIONS` → close tunnel → yield SSE event
5. Frontend JS inserts only red/yellow rows in sorted order (largest first) using `data-total-size` attributes on `<tr>` elements
6. On completion: SSE `done` event with all-clear banner or error section, export button appears if flagged results exist

### SSE Event Types

- `step` — JSON: `message` (granular progress like "rds-prod-east — Querying table sizes")
- `instance_start` — JSON: `instance_name`, `current`, `total`, `progress_html` (sent before each instance scan begins)
- `instance_result` — JSON: `rows_html` (only red/yellow rows), `progress_html`, `connection_name`, `table_count`
- `instance_error` — JSON: `progress_html`, `connection_name`, `error_message` (appends to live errors panel immediately)
- `done` — JSON: `progress_html`, `all_clear_html`, `has_flagged`
- `error` — plain text error message

### Tunnel Cleanup

All tunnels are tracked in a thread-safe registry (`_active_tunnels` in scanner.py). `cleanup_all()` force-kills all registered tunnel processes and runs `tsh db logout` for each. It is called:

1. **`finally` block of the SSE generator** — fires on normal completion, cancellation (Stop Scan / page close), or error. Submitted as fire-and-forget via `run_in_executor`.
2. **`scan_instance` finally block** — each instance cleans up its own tunnel and unregisters it.
3. **FastAPI shutdown event** — catches server restart/stop.

This belt-and-suspenders approach ensures no orphaned `tsh proxy db` processes or dangling db login sessions survive.

### SQL Query (per instance)

Queries `information_schema.PARTITIONS` so the 16TB threshold is evaluated **per partition**, not per table. Non-partitioned tables have a single row with `PARTITION_NAME = NULL`. Excludes system schemas. Parameterized via PyMySQL `%s` placeholders.

```sql
SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TABLE_ROWS,
       COALESCE(DATA_LENGTH, 0), COALESCE(INDEX_LENGTH, 0),
       (COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)) AS total_size
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA NOT IN (...)
ORDER BY total_size DESC
```
