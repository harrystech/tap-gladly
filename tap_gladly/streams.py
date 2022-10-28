"""Stream type classes for tap-gladly."""
import abc
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pendulum
import requests
from singer_sdk import exceptions
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_gladly.client import gladlyStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ExportCompletedJobsStream(gladlyStream):
    """List export jobs stream."""

    name = "jobs"
    path = "/export/jobs?status=COMPLETED"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "export_jobs.json"

    def post_process(self, row, context):
        """Filter jobs that finished before start_date."""
        job_completion_date = pendulum.parse(row["updatedAt"])
        if pendulum.parse(self.config["start_date"]) <= job_completion_date:
            if "end_date" in self.config:
                if pendulum.parse(self.config["end_date"]) >= job_completion_date:
                    return row
            else:
                return row
        return

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"job_id": record["id"], "updatedAt": record["updatedAt"]}


class ExportFile(gladlyStream, abc.ABC):
    """Abstract class for Job File export stream."""

    def get_records(self, context: Optional[Dict[Any, Any]]):
        """Get records that exists, ignoring older jobs if max_job_lookback is setup."""
        if not context:
            logging.warning("Context is empty, nothing to do")
            return []
        period = pendulum.now().diff(pendulum.parse(context["updatedAt"])).in_days()
        if "max_job_lookback" in self.config:
            logging.info(
                f"Max job lookback is set to {self.config['max_job_lookback']}"
            )
            if period <= self.config["max_job_lookback"]:
                logging.info(
                    f"{period} <= {self.config['max_job_lookback']}, syncing ..."
                )
                return super().get_records(context)
            else:
                logging.warning(
                    f"Job id {context['job_id']} ignored because it was "
                    f"{period} > {self.config['max_job_lookback']} days ago"
                )
                return []
        else:
            return super().get_records(context)


class ExportFileTopicsStream(ExportFile):
    """Topic export conversation items stream."""

    name = "topics"
    path = "/export/jobs/{job_id}/files/topics.jsonl"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ExportCompletedJobsStream
    ignore_parent_replication_key = True
    schema_filepath = SCHEMAS_DIR / "export_topics.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for line in response.iter_lines():
            yield from extract_jsonpath(self.records_jsonpath, input=json.loads(line))


class ExportFileConversationItemsAllTypesStream(ExportFile):
    """Stream with all the conversations and content type."""

    name = "conversation_all_types"
    path = "/export/jobs/{job_id}/files/conversation_items.jsonl"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ExportCompletedJobsStream
    ignore_parent_replication_key = True
    schema_filepath = SCHEMAS_DIR / "export_conversation-all_types.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for line in response.iter_lines():
            record = json.loads(line)
            record["content"] = {"type": record["content"]["type"]}
            yield from extract_jsonpath(self.records_jsonpath, input=record)


class ExportFileConversationItemsStream(ExportFile, abc.ABC):
    """Abstract class, export conversation items stream."""

    name = "conversation_conversation_items"
    path = "/export/jobs/{job_id}/files/conversation_items.jsonl"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ExportCompletedJobsStream
    ignore_parent_replication_key = True

    @property
    def schema_filepath(self):
        """Return schema filepath by content type."""
        schemas_mapping = {
            "chat_message": "export_conversation-chat_message.json",
            "conversation_note": "export_conversation-conversation_note.json",
            "topic_change": "export_conversation-topic_change.json",
            "sms": "export_conversation-topic_change.json",
            "conversation_status_change": "export_conversation-conversation_status_change.json",  # noqa
            "phone_call": "export_conversation-phone_call.json",
            "voicemail": "export_conversation-voicemail.json",
            "customer_activity": "export_conversation-customer_activity.json",
            "facebook_message": "export_conversation-facebook_message.json",
            "twitter": "export_conversation-twitter.json",
            "instagram_direct": "export_conversation-instagram_direct.json",
            "whatsapp": "export_conversation-whatsapp.json",
            "email": "export_conversation-email.json",
        }

        try:
            return SCHEMAS_DIR / schemas_mapping[self.content_type.lower()]
        except KeyError:
            raise exceptions.ConfigValidationError(
                "content_type config parameter is required for export "
                "conversations streams"
            )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for line in response.iter_lines():
            yield from extract_jsonpath(self.records_jsonpath, input=json.loads(line))

    def post_process(self, row, context):
        """Filter rows by content type."""
        if row["content"]["type"].lower() == self.content_type.lower():
            return row


class ExportFileConversationItemsChatMessage(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is chat_message."""

    name = "conversation_chat_message"
    content_type = "chat_message"


class ExportFileConversationItemsConversationNote(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is conversation_note."""

    name = "conversation_conversation_note"
    content_type = "conversation_note"


class ExportFileConversationItemsTopicChange(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is topic_change."""

    name = "conversation_topic_change"
    content_type = "topic_change"


class ExportFileConversationItemsSms(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is sms."""

    name = "conversation_sms"
    content_type = "sms"


class ExportFileConversationItemsConversationStatusChange(
    ExportFileConversationItemsStream
):
    """Export conversation items stream.

    Where content type is conversation_status_change.
    """

    name = "conversation_conversation_status_change"
    content_type = "conversation_status_change"


class ExportFileConversationItemsPhoneCall(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is phone_call."""

    name = "conversation_phone_call"
    content_type = "phone_call"


class ExportFileConversationItemsVoiceMail(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_voicemail"
    content_type = "voicemail"


class ExportFileConversationItemsCustomerActivity(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_customer_activity"
    content_type = "customer_activity"


class ExportFileConversationItemsFacebookMessage(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_facebook_message"
    content_type = "facebook_message"


class ExportFileConversationItemsTwitter(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_twitter"
    content_type = "twitter"


class ExportFileConversationItemsInstagramDirect(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_instagram_direct"
    content_type = "instagram_direct"


class ExportFileConversationItemsWhatsapp(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_whatsapp"
    content_type = "whatsapp"


class ExportFileConversationItemsEmail(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "conversation_email"
    content_type = "email"


class ReportsConversationTimestampsReportStream(gladlyStream):
    """gladly stream class for Conversation Timestamps Report."""

    rest_method = "POST"

    name = "reports__conversation_timestamps_report"
    path = "/reports"
    schema_filepath = SCHEMAS_DIR / "reports__conversation_timestamps_report.json"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        payload: dict = {
            "metricSet": "ConversationTimestampsReport",
            "startAt": pendulum.parse(self.config["start_date"]).format(  # type: ignore
                "YYYY-MM-DD"
            ),
            "endAt": (
                pendulum.parse(self.config["end_date"])
                if "end_date" in self.config
                else pendulum.now()
            ).format(  # type: ignore
                "YYYY-MM-DD"
            ),
            "timezone": "UTC",
        }

        return payload

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        gen_decoded_response = (line.decode("utf-8") for line in response.iter_lines())
        yield from csv.DictReader(gen_decoded_response)
