from datetime import date

import pytest


def test_normalize_report_window_daily(app_module):
    key, start, end = app_module.normalize_report_window("daily", date(2024, 2, 10), None)

    assert key == "daily"
    assert start == date(2024, 2, 10)
    assert end == date(2024, 2, 10)


def test_normalize_report_window_weekly(app_module):
    key, start, end = app_module.normalize_report_window("weekly", date(2024, 2, 14), None)

    assert key == "weekly"
    assert start == date(2024, 2, 12)
    assert end == date(2024, 2, 18)


def test_normalize_report_window_monthly(app_module):
    key, start, end = app_module.normalize_report_window("monthly", date(2024, 2, 14), None)

    assert key == "monthly"
    assert start == date(2024, 2, 1)
    assert end == date(2024, 2, 29)


def test_normalize_report_window_daily_uses_start_anchor(app_module):
    key, start, end = app_module.normalize_report_window(
        "daily",
        date(2024, 3, 10),
        date(2024, 3, 1),
    )

    assert key == "daily"
    assert start == date(2024, 3, 10)
    assert end == date(2024, 3, 10)


def test_normalize_report_window_requires_anchor(app_module):
    with pytest.raises(ValueError):
        app_module.normalize_report_window("daily", None, None)
