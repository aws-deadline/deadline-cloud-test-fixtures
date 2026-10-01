# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Minimal page object for controls shared by Deadline submitter dialogs."""

from __future__ import annotations

import time
from typing import Union

import xa11y

from .controls import is_checked

SubmitterRoot = Union[xa11y.App, xa11y.Locator]
SAVE_BUNDLE_AS = "Save bundle as"


def dismiss_bundle_saved_popup(
    pid: int,
    *,
    timeout: float = 5.0,
    poll_interval: float = 0.25,
) -> bool:
    """Dismiss the Deadline save confirmation hosted by a DCC process.

    The Qt message box may belong to the host DCC rather than the submitter
    application, and macOS may not expose its title. Match the stable message
    body within accessibility applications owned by ``pid`` instead.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            for app in xa11y.App.list():
                if app.pid != pid:
                    continue
                ok = app.locator(
                    "dialog button[name='OK'], "
                    "window button[name='OK'], "
                    "sheet button[name='OK']"
                )
                body = app.locator("static_text[name^='Bundle saved to:']")
                if body.exists() and ok.exists():
                    ok.press()
                    return True
        except Exception:  # noqa: S110
            # Accessibility providers can be transiently unavailable while the
            # host DCC creates or destroys modal windows.
            pass
        time.sleep(poll_interval)
    return False


class SharedSubmitterDialog:
    """Page object for controls supplied by the Deadline submitter UI."""

    def __init__(
        self,
        root: SubmitterRoot,
        *,
        app_root: SubmitterRoot | None = None,
    ) -> None:
        self.root = root
        self.app_root = app_root if app_root is not None else root

    @staticmethod
    def _descendant(root: SubmitterRoot, selector: str) -> xa11y.Locator:
        if isinstance(root, xa11y.App):
            return root.locator(selector)
        return root.descendant(selector)

    def button(self, name: str) -> xa11y.Locator:
        return self._descendant(self.root, f'button[name="{name}"]')

    def save_bundle_locally(self, *, timeout: float = 60.0) -> None:
        """Open Save bundle as, choose Local, and confirm the modal save."""
        open_button = self.button(SAVE_BUNDLE_AS)
        open_button.wait_visible(timeout=timeout)
        open_button.wait_enabled(timeout=timeout)
        open_button.press()

        save_dialog = self._descendant(
            self.app_root,
            f'dialog[name="{SAVE_BUNDLE_AS}"], '
            f'window[name="{SAVE_BUNDLE_AS}"], '
            f'sheet[name="{SAVE_BUNDLE_AS}"]',
        )
        save_dialog.wait_visible(timeout=timeout)
        self._select_radio(save_dialog, "Local", timeout=timeout)

        save_button = save_dialog.descendant(f'button[name="{SAVE_BUNDLE_AS}"]')
        save_button.wait_visible(timeout=timeout)
        save_button.wait_enabled(timeout=timeout)
        save_button.press()

    @staticmethod
    def _select_radio(dialog: xa11y.Locator, name: str, *, timeout: float) -> None:
        radio = dialog.descendant(f'radio_button[name="{name}"]')
        radio.wait_visible(timeout=timeout)
        if is_checked(radio) or radio.element().selected:
            return

        last_error: BaseException | None = None
        for action in ("select", "toggle", "press"):
            try:
                radio.perform_action(action)
            except xa11y.XA11yError as error:
                last_error = error
                continue
            if is_checked(radio) or radio.element().selected:
                return

        raise AssertionError(
            f"Could not select radio {name!r} via select/toggle/press"
            + (f" (last error: {last_error})" if last_error is not None else "")
        )
