# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import xa11y

from deadline_test_fixtures.xa11y import (
    SharedSubmitterDialog,
    find_accessibility_app,
)


def test_find_accessibility_app_prefers_matching_process_id(monkeypatch):
    wrong = SimpleNamespace(pid=1, name="Wrong")
    expected = SimpleNamespace(pid=42, name="Expected")
    monkeypatch.setattr(xa11y.App, "list", lambda: [wrong, expected])

    result = find_accessibility_app(42, timeout=0.1)

    assert result is expected


def test_find_accessibility_app_does_not_guess_an_unrelated_app(monkeypatch):
    unrelated = SimpleNamespace(pid=1, name="Unrelated Helper")
    monkeypatch.setattr(xa11y.App, "list", lambda: [unrelated])

    with pytest.raises(TimeoutError):
        find_accessibility_app(42, timeout=0.01)


def test_find_accessibility_app_filters_matching_pid_by_name_prefix(monkeypatch):
    host = SimpleNamespace(pid=42, name="Cinema 4D")
    submitter = SimpleNamespace(pid=42, name="Deadline Cloud Cinema4D Submitter 1.0")
    monkeypatch.setattr(xa11y.App, "list", lambda: [host, submitter])

    result = find_accessibility_app(
        42,
        timeout=0.1,
        name_prefix="Deadline Cloud Cinema4D Submitter",
    )

    assert result is submitter


def test_shared_submitter_dialog_accepts_an_app_root():
    app = MagicMock(spec=xa11y.App)
    button = MagicMock()
    app.locator.return_value = button
    page = SharedSubmitterDialog(app)

    result = page.button("Submit")

    assert result is button
    app.locator.assert_called_once_with('button[name="Submit"]')
