"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_gladly.client import gladlyStream
from tap_gladly.tap import Tapgladly

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        gladlyStream._common_date_format
    ),
    "username": "test",
    "password": "test",
    "api_url_base": "api_base_url",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(Tapgladly, config=SAMPLE_CONFIG)
    #  TODO(Youssef): This is hacky, ideally we would mock the rest api connection
    tests.pop(2)
    for test in tests:
        test()
