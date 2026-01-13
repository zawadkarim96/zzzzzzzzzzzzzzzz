"""Shared helpers for creating and tracking application backups."""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


def _load_backup_metadata(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict):
        return {str(key): str(value) for key, value in data.items()}
    return {}


def _write_backup_metadata(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _prune_backups(backup_dir: Path, keep: int, prefix: str) -> None:
    if keep <= 0:
        return
    backups = sorted(
        backup_dir.glob(f"{prefix}_*.zip"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for old_backup in backups[keep:]:
        old_backup.unlink(missing_ok=True)


def ensure_monthly_backup(
    backup_dir: Path,
    prefix: str,
    build_archive: Callable[[], bytes],
    retention: int,
    mirror_dir: Optional[Path] = None,
) -> tuple[Optional[Path], Optional[str]]:
    metadata_path = backup_dir / "backup_metadata.json"
    now = datetime.now()
    current_month = now.strftime("%Y-%m")
    metadata = _load_backup_metadata(metadata_path)
    if metadata.get("last_backup_month") == current_month:
        return None, None
    try:
        archive_bytes = build_archive()
        if not archive_bytes:
            return None, None
        backup_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{prefix}_{now:%Y_%m_%d_%H%M%S}.zip"
        destination = backup_dir / filename
        temp_path = backup_dir / f".{filename}.tmp"
        with temp_path.open("wb") as handle:
            handle.write(archive_bytes)
        temp_path.replace(destination)
        mirror_error: Optional[str] = None
        if mirror_dir is None:
            mirror_override = os.getenv("PS_BACKUP_MIRROR_DIR")
            mirror_dir = Path(mirror_override).expanduser() if mirror_override else None
        if mirror_dir is not None:
            try:
                mirror_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, mirror_dir / destination.name)
            except OSError as exc:
                mirror_error = str(exc)
        _write_backup_metadata(
            metadata_path,
            {
                "last_backup_month": current_month,
                "last_backup_at": now.isoformat(timespec="seconds"),
                "last_backup_file": destination.name,
                "mirror_dir": str(mirror_dir) if mirror_dir is not None else "",
                "mirror_error": mirror_error or "",
            },
        )
        _prune_backups(backup_dir, retention, prefix)
        if mirror_error:
            return destination, f"Mirror backup copy failed: {mirror_error}"
        return destination, None
    except Exception as exc:
        return None, str(exc)


def get_backup_status(backup_dir: Path) -> dict[str, str]:
    metadata_path = backup_dir / "backup_metadata.json"
    metadata = _load_backup_metadata(metadata_path)
    if not metadata:
        return {}
    last_backup_at = metadata.get("last_backup_at", "")
    try:
        if last_backup_at:
            last_backup_at = datetime.fromisoformat(last_backup_at).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    except ValueError:
        pass
    return {
        "last_backup_at": last_backup_at,
        "last_backup_file": metadata.get("last_backup_file", ""),
        "backup_dir": str(backup_dir),
        "mirror_dir": metadata.get("mirror_dir", ""),
        "mirror_error": metadata.get("mirror_error", ""),
    }
