"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_gladly.client import gladlyStream
from tap_gladly.tap import Tapgladly
import os

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(gladlyStream._common_date_format),
    # TODO: Initialize minimal tap config
    "auth_token": 'test',
    "project_ids": [],
    "content_type": "chat_message"
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        Tapgladly,
        config=SAMPLE_CONFIG
    )

    for test in tests:
        test()

