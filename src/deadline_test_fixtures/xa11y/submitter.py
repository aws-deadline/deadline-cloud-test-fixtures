# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Minimal page object for controls shared by Deadline submitter dialogs."""

from __future__ import annotations

from typing import Union

import xa11y

SubmitterRoot = Union[xa11y.App, xa11y.Locator]


class SharedSubmitterDialog:
    """Page object for controls supplied by the Deadline submitter UI."""

    def __init__(self, root: SubmitterRoot) -> None:
        self.root = root

    def _descendant(self, selector: str) -> xa11y.Locator:
        if isinstance(self.root, xa11y.App):
            return self.root.locator(selector)
        return self.root.descendant(selector)

    def button(self, name: str) -> xa11y.Locator:
        return self._descendant(f'button[name="{name}"]')
