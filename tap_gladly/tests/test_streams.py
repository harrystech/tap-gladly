"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests
from singer_sdk import exceptions

from tap_gladly.tap import Tapgladly
import pytest

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    # TODO: Initialize minimal tap config
    "project_ids": [],

}


def test_content_type_exception():
    Tapgladly(config={'content_type': 'chat_message'},
              parse_env_config=True)
    with pytest.raises(exceptions.ConfigValidationError):
        Tapgladly(config=None,
                  parse_env_config=True)
