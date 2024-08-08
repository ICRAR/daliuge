"""
This file ensures dlg.runtime is loaded up when running pytest on a suite of tests.

This sets up the correct logging runtime and drop-tracking.
"""

import dlg.runtime
import pytest
