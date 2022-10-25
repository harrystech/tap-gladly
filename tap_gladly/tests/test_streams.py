"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import pendulum

from tap_gladly.streams import (
    ExportCompletedJobsStream,
    ExportFileConversationItemsChatMessage,
)
from tap_gladly.tap import Tapgladly

SAMPLE_CONFIG = {
    "start_date": pendulum.now(),
    "username": "test",
    "password": "test",
    "api_url_base": "api_base_url",
}


def test_started_at():
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(pendulum.now() - datetime.timedelta(days=2)).isoformat(),
        ),
        parse_env_config=False,
    )
    export_jobs_stream = ExportCompletedJobsStream(tap_gladly)
    before_row = {
        "record": "data",
        "parameters": {
            "endAt": (pendulum.now() - datetime.timedelta(days=3)).isoformat()
        },
    }
    after_row = {
        "record": "data",
        "parameters": {"endAt": pendulum.now().isoformat()},
    }
    assert not export_jobs_stream.post_process(before_row, None)
    assert export_jobs_stream.post_process(after_row, None)


def test_data_interval():
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(pendulum.now() - datetime.timedelta(days=2)).isoformat(),
            end_date=(pendulum.now() - datetime.timedelta(days=1)).isoformat(),
        ),
        parse_env_config=False,
    )
    export_jobs_stream = ExportCompletedJobsStream(tap_gladly)
    before_row = {
        "record": "data",
        "parameters": {
            "endAt": (pendulum.now() - datetime.timedelta(days=3)).isoformat()
        },
    }
    within_row = {
        "record": "data",
        "parameters": {
            "endAt": (pendulum.now() - datetime.timedelta(hours=30)).isoformat()
        },
    }
    after_row = {
        "record": "data",
        "parameters": {"endAt": pendulum.now().isoformat()},
    }
    assert not export_jobs_stream.post_process(before_row, None)
    assert not export_jobs_stream.post_process(after_row, None)
    assert export_jobs_stream.post_process(within_row, None)


def test_filter_by_content_type():
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(pendulum.now() - datetime.timedelta(days=2)).isoformat(),
        ),
        parse_env_config=False,
    )
    efcis = ExportFileConversationItemsChatMessage(tap_gladly)

    good_content_row = {"record": "data", "content": {"type": efcis.content_type}}
    bad_content_row = {"record": "data", "content": {"type": "order"}}

    assert efcis.post_process(good_content_row, None)
    assert not efcis.post_process(bad_content_row, None)
