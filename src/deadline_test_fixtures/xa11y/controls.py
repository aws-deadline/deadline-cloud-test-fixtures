# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Robust cross-platform interactions for common Qt accessibility widgets."""

from __future__ import annotations

import os
import time
from typing import Callable, Optional, Union

import xa11y

WIDGET_TIMEOUT = 60.0
TAB_SHARED = "Shared job settings"
TAB_JOB_SPECIFIC = "Job-specific settings"
ControlRoot = Union[xa11y.App, xa11y.Locator]


def _observe_pause() -> None:
    delay = float(os.environ.get("DIALOG_CONFIG_OBSERVE_DELAY_S", "0"))
    if delay > 0:
        time.sleep(delay)


def _descendant(root: ControlRoot, selector: str) -> xa11y.Locator:
    if isinstance(root, xa11y.App):
        return root.locator(selector)
    return root.descendant(selector)


def switch_to_tab(
    dialog: ControlRoot,
    tab_name: str,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> None:
    """Activate a Qt tab across AX, AT-SPI, and UIA role names."""
    tab = _descendant(
        dialog,
        f'tab[name="{tab_name}"], '
        f'page_tab[name="{tab_name}"], '
        f'radio_button[name="{tab_name}"]',
    )
    tab.wait_visible(timeout=timeout)
    tab.press()
    time.sleep(0.5)
    _observe_pause()


def set_text_field(
    dialog: ControlRoot,
    name: str,
    value: str,
    nth: Optional[int] = None,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> str:
    """Set a text field and return its previous value."""
    field = _descendant(dialog, f'text_field[name="{name}"]')
    if nth is not None:
        field = field.nth(nth)
    field.wait_visible(timeout=timeout)
    previous = field.element().value or ""
    field.set_value(value)
    _observe_pause()
    return previous


def _step_spin_button(spin: xa11y.Locator, target: int) -> None:
    current = int(spin.element().value or "0")
    for _ in range(10_000):
        if current == target:
            return
        spin.increment() if current < target else spin.decrement()
        _observe_pause()
        current = int(spin.element().value or "0")
    raise AssertionError(f"Spin button did not reach {target}; stopped at {current}")


def set_spin_button(
    dialog: xa11y.Locator,
    name: str,
    nth: int,
    target: int,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> None:
    spin = dialog.descendant(f'spin_button[name="{name}"]').nth(nth)
    spin.wait_visible(timeout=timeout)
    _step_spin_button(spin, target)


def set_spin_button_in_group(
    dialog: xa11y.Locator,
    group: str,
    nth: int,
    target: int,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> None:
    spin = dialog.descendant(f'group[name="{group}"]').descendant("spin_button").nth(nth)
    spin.wait_visible(timeout=timeout)
    _step_spin_button(spin, target)


def is_checked(box: xa11y.Locator) -> bool:
    value = (box.element().checked or "").lower()
    return value in ("on", "true", "1", "checked")


def toggle_checkbox(
    dialog: xa11y.Locator,
    name: str,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> None:
    box = dialog.descendant(f'check_box[name="{name}"]')
    box.wait_visible(timeout=timeout)
    box.toggle()
    _observe_pause()


def set_checkbox(
    dialog: xa11y.Locator,
    name: str,
    checked: bool,
    *,
    timeout: float = WIDGET_TIMEOUT,
) -> None:
    box = dialog.descendant(f'check_box[name="{name}"]')
    box.wait_visible(timeout=timeout)
    if is_checked(box) != checked:
        box.toggle()
        _observe_pause()


def set_job_name(dialog: ControlRoot, name: str) -> None:
    switch_to_tab(dialog, TAB_SHARED)
    set_text_field(dialog, "Name", name)


def set_priority(dialog: xa11y.Locator, value: int) -> None:
    switch_to_tab(dialog, TAB_SHARED)
    set_spin_button(dialog, "Job Properties", 1, value)


def set_max_failed_tasks(dialog: xa11y.Locator, value: int) -> None:
    switch_to_tab(dialog, TAB_SHARED)
    set_spin_button(dialog, "Job Properties", 2, value)


def set_max_retries(dialog: xa11y.Locator, value: int) -> None:
    switch_to_tab(dialog, TAB_SHARED)
    set_spin_button(dialog, "Job Properties", 3, value)


def transform_text_field(field: xa11y.Locator, transform: Callable[[str], str]) -> str:
    """Transform a field without hard-coding its environment-specific value."""
    current = field.element().value or ""
    new_value = transform(current)
    field.set_value(new_value)
    _observe_pause()
    return new_value
