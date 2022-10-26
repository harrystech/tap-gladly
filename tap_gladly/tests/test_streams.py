"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from unittest import mock

import pendulum

from tap_gladly.streams import (
    ExportCompletedJobsStream,
    ExportFileConversationItemsChatMessage,
    ExportFileTopicsStream,
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
        "updatedAt": (pendulum.now() - datetime.timedelta(days=3)).isoformat(),
    }
    after_row = {"record": "data", "updatedAt": pendulum.now().isoformat()}
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
        "updatedAt": (pendulum.now() - datetime.timedelta(days=3)).isoformat(),
    }
    within_row = {
        "record": "data",
        "updatedAt": (pendulum.now() - datetime.timedelta(hours=30)).isoformat(),
    }
    after_row = {
        "record": "data",
        "updatedAt": pendulum.now().isoformat(),
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


@mock.patch("tap_gladly.streams.gladlyStream.get_records")
def test_max_job_lookback_get_records(mocked_super_get_records):
    max_job_lookback = 5
    tap_gladly = Tapgladly(
        config=dict(
            SAMPLE_CONFIG,
            start_date=(pendulum.now() - datetime.timedelta(days=4)).isoformat(),
            max_job_lookback=5,
        ),
        parse_env_config=False,
    )

    file_stream = ExportFileTopicsStream(tap_gladly)
    mocked_super_get_records.return_value = ["records"]

    valid_job_context = {
        "job_id": "job_id",
        "updatedAt": (
            pendulum.now() - datetime.timedelta(days=max_job_lookback - 1)
        ).isoformat(),
    }

    earliest_valid_job_context = {
        "job_id": "job_id",
        "updatedAt": (
            pendulum.now() - datetime.timedelta(days=max_job_lookback)
        ).isoformat(),
    }

    ignored_job_context = {
        "job_id": "job_id_2",
        "updatedAt": (
            pendulum.now() - datetime.timedelta(days=max_job_lookback + 1)
        ).isoformat(),
    }

    assert len(file_stream.get_records(valid_job_context)) > 0
    assert len(file_stream.get_records(earliest_valid_job_context)) > 0
    assert len(file_stream.get_records(ignored_job_context)) == 0
