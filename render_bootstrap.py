"""Production-friendly bootstrapper for Render and Railway deployments.

This module mirrors the ``Procfile`` command while allowing platforms that
expect a Python entry point (e.g. ``python render_bootstrap.py``) to launch the
Streamlit application. It automatically selects the service or sales
experience based on environment variables and prefers persistent storage
mounts when available.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

DEFAULT_APP_SCRIPT = "main.py"
SALES_APP_SCRIPT = "sales_app.py"


def _select_app_script() -> str:
    """Return the Streamlit script that should be executed."""

    explicit_script = os.getenv("PS_APP_SCRIPT")
    if explicit_script:
        return explicit_script

    app_flavor = os.getenv("PS_APP", "").lower()
    if app_flavor in {"sales", "sales_app"}:
        return SALES_APP_SCRIPT

    return DEFAULT_APP_SCRIPT


def _preferred_storage_dir() -> Path | None:
    """Return a writable directory for application data if one is obvious."""

    configured_dir = os.getenv("APP_STORAGE_DIR")
    if configured_dir:
        return Path(configured_dir)

    for candidate in (os.getenv("RAILWAY_VOLUME_MOUNT_PATH"), "/data", "/opt/render/project/.data"):
        if candidate and Path(candidate).exists():
            return Path(candidate)

    return None


def main() -> None:
    root_dir = Path(__file__).resolve().parent

    app_script_name = _select_app_script()
    app_script = root_dir / app_script_name
    if not app_script.exists():
        raise SystemExit(
            f"Expected application script '{app_script_name}' next to render_bootstrap.py, but it was not found."
        )

    storage_dir = _preferred_storage_dir()
    if storage_dir is not None:
        storage_dir.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("APP_STORAGE_DIR", str(storage_dir))

    os.environ.setdefault("BROWSER", "none")

    port = os.getenv("PORT", "8501")
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_script),
        "--server.port",
        str(port),
        "--server.address",
        "0.0.0.0",
        "--server.headless",
        "true",
    ]

    subprocess.run(command, check=True, cwd=root_dir)


if __name__ == "__main__":
    main()
