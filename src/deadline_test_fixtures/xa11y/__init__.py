# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Cross-platform accessibility helpers for subprocess UI tests."""

from .app import find_accessibility_app
from .submitter import SharedSubmitterDialog

__all__ = [
    "SharedSubmitterDialog",
    "find_accessibility_app",
]
