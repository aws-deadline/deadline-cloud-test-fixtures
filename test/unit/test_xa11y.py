# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import xa11y

from deadline_test_fixtures.xa11y import (
    SharedSubmitterDialog,
    dismiss_bundle_saved_popup,
    find_accessibility_app,
)
from deadline_test_fixtures.xa11y import submitter as submitter_module


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


def test_shared_submitter_dialog_saves_bundle_locally():
    root = MagicMock(spec=xa11y.Locator)
    app = MagicMock(spec=xa11y.App)
    open_button = MagicMock()
    save_dialog = MagicMock()
    local_radio = MagicMock()
    save_button = MagicMock()
    radio_state = SimpleNamespace(checked="off", selected=False)
    local_radio.element.return_value = radio_state

    def select_radio(action):
        assert action == "select"
        radio_state.checked = "on"
        radio_state.selected = True

    local_radio.perform_action.side_effect = select_radio
    root.descendant.return_value = open_button
    app.locator.return_value = save_dialog
    save_dialog.descendant.side_effect = [local_radio, save_button]

    SharedSubmitterDialog(root, app_root=app).save_bundle_locally(timeout=12.0)

    root.descendant.assert_called_once_with('button[name="Save bundle as"]')
    open_button.wait_visible.assert_called_once_with(timeout=12.0)
    open_button.wait_enabled.assert_called_once_with(timeout=12.0)
    open_button.press.assert_called_once_with()
    app.locator.assert_called_once_with(
        'dialog[name="Save bundle as"], '
        'window[name="Save bundle as"], '
        'sheet[name="Save bundle as"]'
    )
    save_dialog.wait_visible.assert_called_once_with(timeout=12.0)
    save_dialog.descendant.assert_any_call('radio_button[name="Local"]')
    local_radio.perform_action.assert_called_once_with("select")
    save_dialog.descendant.assert_any_call('button[name="Save bundle as"]')
    save_button.wait_visible.assert_called_once_with(timeout=12.0)
    save_button.wait_enabled.assert_called_once_with(timeout=12.0)
    save_button.press.assert_called_once_with()


def test_dismiss_bundle_saved_popup_uses_matching_host_process(monkeypatch):
    unrelated_app = MagicMock(pid=7)
    host_app = MagicMock(pid=42)
    ok = MagicMock()
    body = MagicMock()
    ok.exists.return_value = True
    body.exists.return_value = True
    host_app.locator.side_effect = [ok, body]
    monkeypatch.setattr(xa11y.App, "list", lambda: [unrelated_app, host_app])

    assert dismiss_bundle_saved_popup(42, timeout=1.0)

    unrelated_app.locator.assert_not_called()
    host_app.locator.assert_any_call(
        "dialog button[name='OK'], " "window button[name='OK'], " "sheet button[name='OK']"
    )
    host_app.locator.assert_any_call("static_text[name^='Bundle saved to:']")
    ok.press.assert_called_once_with()


def test_dismiss_bundle_saved_popup_returns_false_after_timeout(monkeypatch):
    clock = iter((0.0, 0.0, 1.0))
    monkeypatch.setattr(submitter_module.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(submitter_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(xa11y.App, "list", list)

    assert not dismiss_bundle_saved_popup(42, timeout=0.5)
