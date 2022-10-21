"""Stream type classes for tap-gladly."""
import abc
import json
from pathlib import Path
from typing import Iterable, Optional

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
        job_completion_date = pendulum.parse(row["parameters"]["endAt"])
        if pendulum.parse(self.config["start_date"]) <= job_completion_date:
            if "end_date" in self.config:
                if pendulum.parse(self.config["end_date"]) >= job_completion_date:
                    return row
            else:
                return row
        return

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"job_id": record["id"]}


class ExportFileTopicsStream(gladlyStream):
    """Abstract class, export conversation items stream."""

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


class ExportFileConversationItemsStream(gladlyStream, abc.ABC):
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
