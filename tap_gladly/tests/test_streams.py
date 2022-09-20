"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from tap_gladly.client import gladlyStream
from tap_gladly.streams import ExportFileConversationItemsChatMessage, ExportJobsStream
from tap_gladly.tap import Tapgladly

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        gladlyStream._common_date_format
    ),
    "username": "test",
    "password": "test",
    "api_base_url": "api_base_url",
}


def test_started_at():
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=3)
            ).strftime(gladlyStream._common_date_format),
        ),
        parse_env_config=False,
    )
    export_jobs_stream = ExportJobsStream(tap_gladly)
    before_row = {
        "record": "data",
        "parameters": {
            "startAt": (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=3)
            ).strftime(gladlyStream._common_date_format)
        },
    }
    after_row = {
        "record": "data",
        "parameters": {
            "startAt": datetime.datetime.now(datetime.timezone.utc).strftime(
                gladlyStream._common_date_format
            )
        },
    }
    assert not export_jobs_stream.post_process(before_row, None)
    assert export_jobs_stream.post_process(after_row, None)


def test_filter_by_content_type():
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=2)
            ).strftime(gladlyStream._common_date_format),
        ),
        parse_env_config=False,
    )
    efcis = ExportFileConversationItemsChatMessage(tap_gladly)

    good_content_row = {"record": "data", "content": {"type": efcis.content_type}}
    bad_content_row = {"record": "data", "content": {"type": "order"}}

    assert efcis.post_process(good_content_row, None)
    assert not efcis.post_process(bad_content_row, None)
