# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Cross-platform accessibility helpers for subprocess UI tests."""

from .app import find_accessibility_app
from .submitter import SharedSubmitterDialog, dismiss_bundle_saved_popup

__all__ = [
    "SharedSubmitterDialog",
    "dismiss_bundle_saved_popup",
    "find_accessibility_app",
]
