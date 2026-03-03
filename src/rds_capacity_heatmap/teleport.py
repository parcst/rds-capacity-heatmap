"""Teleport (tsh) integration for RDS tunnel management."""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_TELEPORT_CONNECT_TSH = (
    "/Applications/Teleport Connect.app/Contents/MacOS/tsh.app/Contents/MacOS/tsh"
)
_TSH_DIR = Path.home() / ".tsh"
_TUNNEL_READY_TIMEOUT = 15  # seconds to wait for tunnel port


@dataclass
class TeleportTunnel:
    """A running tsh proxy db tunnel."""

    process: subprocess.Popen  # type: ignore[type-arg]
    host: str
    port: int
    db_name: str
    db_user: str


# ---------------------------------------------------------------------------
# tsh discovery
# ---------------------------------------------------------------------------


def find_tsh(override: str = "") -> str:
    """Locate the tsh binary."""
    if override:
        p = Path(override)
        if p.is_file():
            return str(p)
        raise FileNotFoundError(f"Configured tsh path not found: {override}")

    which = shutil.which("tsh")
    if which:
        return which

    if Path(_TELEPORT_CONNECT_TSH).is_file():
        return _TELEPORT_CONNECT_TSH

    raise FileNotFoundError(
        "Could not find tsh binary. Install Teleport or set tsh_path."
    )


# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------


def get_clusters() -> list[str]:
    """Return cluster names from ``~/.tsh/*.yaml`` profile files."""
    profiles = sorted(_TSH_DIR.glob("*.yaml"))
    clusters = [p.stem for p in profiles]
    return clusters


def get_logged_in_user(tsh: str) -> str:
    """Return the username from ``tsh status --format=json``."""
    result = subprocess.run(
        [tsh, "status", "--format=json"],
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        raise RuntimeError("tsh status returned no output. Are you logged in?")
    status = json.loads(result.stdout)

    username: str = ""
    if isinstance(status, dict):
        username = (
            status.get("active", {}).get("username", "")
            or status.get("username", "")
        )

    if not username:
        raise RuntimeError(
            "Could not determine Teleport username from 'tsh status'. "
            "Are you logged in?"
        )
    return username


def check_login_status(tsh: str) -> tuple[bool, str]:
    """Check if the user is logged in. Returns (is_logged_in, username_or_error)."""
    try:
        username = get_logged_in_user(tsh)
        return True, username
    except Exception as e:
        return False, str(e)


def login_to_cluster(tsh: str, cluster: str) -> subprocess.Popen:  # type: ignore[type-arg]
    """Start ``tsh login <cluster>`` (opens browser for SSO). Returns the process."""
    return subprocess.Popen(
        [tsh, "login", cluster],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


# ---------------------------------------------------------------------------
# Database listing
# ---------------------------------------------------------------------------


def list_mysql_databases(tsh: str, cluster: str) -> list[dict[str, str]]:
    """List MySQL databases on *cluster* via ``tsh db ls``.

    Returns a list of dicts with keys:
    ``name``, ``uri``, ``account_id``, ``region``, ``instance_id``.
    Returns empty list if no MySQL databases found.
    """
    result = subprocess.run(
        [tsh, "db", "ls", f"--proxy={cluster}", "--format=json"],
        capture_output=True,
        text=True,
        check=True,
    )
    raw = json.loads(result.stdout)
    if not isinstance(raw, list):
        raw = [raw]

    databases: list[dict[str, str]] = []
    for entry in raw:
        spec = entry.get("spec", {})
        if spec.get("protocol") != "mysql":
            continue

        aws = spec.get("aws", {})
        rds = aws.get("rds", {})
        databases.append(
            {
                "name": entry.get("metadata", {}).get("name", ""),
                "uri": spec.get("uri", ""),
                "account_id": aws.get("account_id", ""),
                "region": aws.get("region", ""),
                "instance_id": rds.get("instance_id", ""),
            }
        )

    return databases


# ---------------------------------------------------------------------------
# Tunnel management
# ---------------------------------------------------------------------------


def start_tunnel(
    tsh: str, db_name: str, db_user: str, *, cluster: str | None = None
) -> TeleportTunnel:
    """Log in to *db_name* and start a local tunnel.

    1. ``tsh db login <db_name> --db-user=<db_user>``
    2. ``tsh proxy db --tunnel --port 0 <db_name>`` (background)
    3. Parse stdout for the listening port.
    """
    cluster_args = [f"--proxy={cluster}"] if cluster else []

    # Step 1: authenticate
    logger.info("Logging in to database %s as %s ...", db_name, db_user)
    subprocess.run(
        [tsh, "db", "login", db_name, f"--db-user={db_user}"] + cluster_args,
        check=True,
        capture_output=True,
    )

    # Step 2: start tunnel
    logger.info("Starting tunnel for %s ...", db_name)
    proc = subprocess.Popen(
        [tsh, "proxy", "db", "--tunnel", "--port", "0", db_name] + cluster_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Step 3: parse port from output like "... on 127.0.0.1:12345"
    port = _wait_for_tunnel_port(proc)
    logger.info("Tunnel ready on 127.0.0.1:%d", port)

    return TeleportTunnel(
        process=proc,
        host="127.0.0.1",
        port=port,
        db_name=db_name,
        db_user=db_user,
    )


def _wait_for_tunnel_port(proc: subprocess.Popen) -> int:  # type: ignore[type-arg]
    """Read *proc* stdout until we find the listening port or timeout."""
    pattern = re.compile(r"127\.0\.0\.1:(\d+)")
    deadline = time.monotonic() + _TUNNEL_READY_TIMEOUT

    assert proc.stdout is not None
    collected = ""
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            time.sleep(0.1)
            continue

        collected += line
        m = pattern.search(line)
        if m:
            return int(m.group(1))

    proc.kill()
    raise RuntimeError(
        f"Timed out waiting for tsh tunnel port. Output:\n{collected}"
    )


def stop_tunnel(tsh: str, tunnel: TeleportTunnel) -> None:
    """Terminate the tunnel process and log out of the database."""
    logger.info("Stopping tunnel for %s ...", tunnel.db_name)
    try:
        tunnel.process.terminate()
        tunnel.process.wait(timeout=5)
    except Exception:
        tunnel.process.kill()

    try:
        subprocess.run(
            [tsh, "db", "logout", tunnel.db_name],
            capture_output=True,
            timeout=10,
        )
    except Exception:
        logger.warning("Failed to logout from %s", tunnel.db_name, exc_info=True)
