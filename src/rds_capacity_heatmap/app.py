"""FastAPI application with SSE-powered capacity scanning endpoint."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import queue as thread_queue
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

from .models import (
    HeatmapStatus,
    InstanceScanError,
    RDS_MYSQL_HARD_LIMIT,
    ScanQuery,
    ScanState,
    TableSizeResult,
)
from .scanner import cleanup_all, list_instances, scan_instance
from .teleport import find_tsh, get_clusters, get_login_status, login_to_cluster

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
STATIC_DIR = PROJECT_ROOT / "static"

app = FastAPI(title="RDS Capacity Heatmap")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("shutdown")
async def _shutdown_cleanup():
    """Kill any surviving tunnels when the server shuts down."""
    await asyncio.to_thread(cleanup_all)

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def _format_bytes(value: int) -> str:
    """Format byte count to human-readable string (auto-scaling KB/MB/GB/TB)."""
    if value < 0:
        return "0 B"
    units = [("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)]
    for suffix, threshold in units:
        if value >= threshold:
            return f"{value / threshold:.2f} {suffix}"
    return f"{value} B"


templates.env.filters["format_bytes"] = _format_bytes
templates.env.globals["rds_limit"] = RDS_MYSQL_HARD_LIMIT

# Track login state
_login_process = None
_logged_in_cluster: str = ""
_logged_in_username: str = ""

# Last scan state for CSV export
_last_scan_state: ScanState | None = None


def _resolve_username(cluster: str = "") -> str:
    """Return the logged-in Teleport username for *cluster*."""
    global _logged_in_username, _logged_in_cluster
    if _logged_in_username and _logged_in_cluster == cluster:
        return _logged_in_username
    try:
        tsh = find_tsh()
        ok, username = get_login_status(tsh, cluster or None)
        if ok:
            _logged_in_username = username
            _logged_in_cluster = cluster
            return username
    except Exception:
        pass
    return ""


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    clusters = get_clusters()
    username = _resolve_username()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "clusters": clusters,
            "username": username,
        },
    )


@app.post("/api/login", response_class=HTMLResponse)
async def api_login(request: Request):
    global _login_process
    form = await request.form()
    cluster = str(form.get("cluster", ""))
    if not cluster:
        return HTMLResponse('<div class="error">No cluster selected</div>')

    try:
        tsh = find_tsh()
        _login_process = login_to_cluster(tsh, cluster)
        return templates.TemplateResponse(
            "partials/login_status.html",
            {"request": request, "status": "pending", "cluster": cluster},
        )
    except Exception as e:
        return HTMLResponse(f'<div class="error">{e}</div>')


@app.get("/api/login-status", response_class=HTMLResponse)
async def api_login_status(request: Request):
    global _logged_in_username, _logged_in_cluster
    cluster = request.query_params.get("cluster", "")
    try:
        tsh = find_tsh()
        logged_in, username = get_login_status(tsh, cluster or None)
        if logged_in:
            _logged_in_username = username
            _logged_in_cluster = cluster
            return templates.TemplateResponse(
                "partials/login_status.html",
                {
                    "request": request,
                    "status": "success",
                    "username": username,
                    "cluster": cluster,
                },
            )
        else:
            return templates.TemplateResponse(
                "partials/login_status.html",
                {"request": request, "status": "pending", "cluster": cluster},
            )
    except Exception:
        return templates.TemplateResponse(
            "partials/login_status.html",
            {"request": request, "status": "pending", "cluster": cluster},
        )


def _render_progress(state: ScanState, current: int, total: int, *, done: bool = False) -> str:
    return templates.get_template("partials/progress.html").render(
        current=current,
        total=total,
        red_count=len(state.red_results),
        yellow_count=len(state.yellow_results),
        error_count=len(state.errors),
        done=done,
    )


@app.get("/api/scan")
async def api_scan(request: Request):
    global _last_scan_state

    cluster = request.query_params.get("cluster", "")
    if not cluster:
        async def error_stream():
            yield {"event": "error", "data": "No cluster selected"}
        return EventSourceResponse(error_stream())

    db_user = _resolve_username(cluster)
    if not db_user:
        async def error_stream():
            yield {"event": "error", "data": f"Not logged in to {cluster}. Click Login first."}
        return EventSourceResponse(error_stream())

    query = ScanQuery(cluster=cluster, db_user=db_user)
    state = ScanState(query=query)

    async def event_stream():
        global _last_scan_state
        try:
            # Step 1: discover instances
            yield _step_event("Discovering MySQL instances...")

            entries = await asyncio.to_thread(list_instances, query.cluster)
            total = len(entries)
            state.total_instances = total

            if total == 0:
                yield _step_event("No MySQL instances found on this cluster.")
                state.completed = True
                _last_scan_state = state
                yield {
                    "event": "done",
                    "data": json.dumps({
                        "progress_html": _render_progress(state, 0, 0, done=True),
                        "all_clear_html": "",
                        "error_html": "",
                        "has_flagged": False,
                    }),
                }
                return

            yield _step_event(f"Found {total} instance{'s' if total != 1 else ''}. Starting scan...")

            # Step 2: scan each instance with live step updates
            loop = asyncio.get_event_loop()

            for idx, entry in enumerate(entries):
                current = idx + 1
                step_queue: thread_queue.Queue[str] = thread_queue.Queue()

                def on_step(msg: str, _q: thread_queue.Queue[str] = step_queue) -> None:
                    _q.put(msg)

                # Notify frontend we're starting this instance
                yield {
                    "event": "instance_start",
                    "data": json.dumps({
                        "instance_name": entry.name,
                        "current": current,
                        "total": total,
                        "progress_html": _render_progress(state, idx, total),
                    }),
                }

                # Run scan in executor so we can poll the step queue
                future = loop.run_in_executor(
                    None, scan_instance, entry, query, on_step
                )

                # Poll for step updates while the scan runs
                while not future.done():
                    try:
                        msg = step_queue.get_nowait()
                        yield _step_event(f"{entry.name} — {msg}")
                    except thread_queue.Empty:
                        pass
                    await asyncio.sleep(0.15)

                # Drain any remaining messages
                while not step_queue.empty():
                    msg = step_queue.get_nowait()
                    yield _step_event(f"{entry.name} — {msg}")

                result_or_error = future.result()

                if isinstance(result_or_error, InstanceScanError):
                    state.errors.append(result_or_error)

                    yield {
                        "event": "instance_error",
                        "data": json.dumps({
                            "progress_html": _render_progress(state, current, total),
                            "connection_name": result_or_error.connection_name,
                            "error_message": result_or_error.error_message,
                        }),
                    }

                else:
                    state.results.extend(result_or_error)

                    flagged = [
                        r for r in result_or_error
                        if r.status in (HeatmapStatus.RED, HeatmapStatus.YELLOW)
                    ]

                    rows_html = ""
                    if flagged:
                        rows_html = "".join(
                            templates.get_template(
                                "partials/result_row.html"
                            ).render(result=r)
                            for r in flagged
                        )

                    yield {
                        "event": "instance_result",
                        "data": json.dumps({
                            "rows_html": rows_html,
                            "progress_html": _render_progress(state, current, total),
                            "connection_name": entry.name,
                            "table_count": len(result_or_error),
                        }),
                    }

            # Done
            state.completed = True
            _last_scan_state = state

            all_clear_html = ""
            if state.all_clear:
                all_clear_html = templates.get_template(
                    "partials/all_clear.html"
                ).render()

            # If fewer than MIN_DISPLAY_RESULTS flagged, fill with top greens
            topup = state.topup_greens()
            topup_html = ""
            if topup:
                topup_html = "".join(
                    templates.get_template(
                        "partials/result_row.html"
                    ).render(result=r)
                    for r in topup
                )

            yield {
                "event": "done",
                "data": json.dumps({
                    "progress_html": _render_progress(state, total, total, done=True),
                    "all_clear_html": all_clear_html,
                    "has_flagged": len(state.flagged_results) > 0 or len(topup) > 0,
                    "topup_rows_html": topup_html,
                }),
            }

        except asyncio.CancelledError:
            state.completed = True
            _last_scan_state = state
            raise

        except Exception as e:
            logger.exception("Scan error")
            yield {"event": "error", "data": str(e)}

        finally:
            # Kill any surviving tunnels and logout their db sessions.
            # Fire-and-forget: the thread pool will run this even if our
            # coroutine is being cancelled.
            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, cleanup_all)

    return EventSourceResponse(event_stream())


def _step_event(message: str) -> dict:
    return {
        "event": "step",
        "data": json.dumps({"message": message}),
    }


@app.get("/api/export-csv")
async def api_export_csv():
    if _last_scan_state is None:
        return HTMLResponse("No scan results to export", status_code=404)

    export_rows = _last_scan_state.flagged_results + _last_scan_state.topup_greens()
    if not export_rows:
        return HTMLResponse("No scan results to export", status_code=404)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Status",
        "Connection Name",
        "Database",
        "Table",
        "Partition",
        "Data Size (bytes)",
        "Data Size",
        "Index Size (bytes)",
        "Index Size",
        "Total Size (bytes)",
        "Total Size",
        "Pct of 16TB Limit",
        "Row Count",
    ])

    export_rows = sorted(
        export_rows,
        key=lambda r: r.total_size,
        reverse=True,
    )

    for r in export_rows:
        pct = r.total_size / RDS_MYSQL_HARD_LIMIT * 100
        writer.writerow([
            r.status.value.upper(),
            r.connection_name,
            r.database,
            r.table,
            r.partition_name or "",
            r.data_size,
            _format_bytes(r.data_size),
            r.index_size,
            _format_bytes(r.index_size),
            r.total_size,
            _format_bytes(r.total_size),
            f"{pct:.1f}%",
            r.table_rows,
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=rds-capacity-heatmap.csv"},
    )
