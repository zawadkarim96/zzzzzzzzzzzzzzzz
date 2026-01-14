from __future__ import annotations

import importlib.util
import os
from pathlib import Path


def _load_backup_utils():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "backup_utils.py"
    spec = importlib.util.spec_from_file_location("backup_utils_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if loader is None:
        raise RuntimeError("Unable to load backup_utils module for tests")
    loader.exec_module(module)
    return module


backup_utils = _load_backup_utils()


def test_ensure_monthly_backup_creates_archive_and_metadata(tmp_path):
    backup_dir = tmp_path / "backups"
    mirror_dir = tmp_path / "mirror"

    destination, error = backup_utils.ensure_monthly_backup(
        backup_dir,
        "ps_crm_backup",
        lambda: b"backup-bytes",
        retention=3,
        mirror_dir=mirror_dir,
    )

    assert error is None
    assert destination is not None
    assert destination.exists()
    assert (backup_dir / "backup_metadata.json").exists()
    assert (mirror_dir / destination.name).exists()

    status = backup_utils.get_backup_status(backup_dir)
    assert status["last_backup_file"] == destination.name
    assert status["backup_dir"] == str(backup_dir)
    assert status["mirror_dir"] == str(mirror_dir)


def test_ensure_monthly_backup_skips_duplicate_month(tmp_path):
    backup_dir = tmp_path / "backups"
    calls = {"count": 0}

    def _builder() -> bytes:
        calls["count"] += 1
        return b"backup-bytes"

    first_path, first_error = backup_utils.ensure_monthly_backup(
        backup_dir,
        "ps_crm_backup",
        _builder,
        retention=2,
    )
    second_path, second_error = backup_utils.ensure_monthly_backup(
        backup_dir,
        "ps_crm_backup",
        _builder,
        retention=2,
    )

    assert first_error is None
    assert first_path is not None
    assert second_path is None
    assert second_error is None
    assert calls["count"] == 1


def test_prune_backups_removes_oldest(tmp_path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    files = []
    for idx in range(4):
        path = backup_dir / f"ps_crm_backup_2024_01_0{idx}_000000.zip"
        path.write_bytes(b"data")
        path.touch()
        files.append(path)

    for offset, path in enumerate(files):
        ts = 1_700_000_000 + offset
        os.utime(path, (ts, ts))

    backup_utils._prune_backups(backup_dir, keep=2, prefix="ps_crm_backup")

    remaining = sorted(backup_dir.glob("ps_crm_backup_*.zip"))
    assert len(remaining) == 2
    assert remaining[-1].name.endswith("03_000000.zip")
