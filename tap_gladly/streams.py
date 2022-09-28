"""Stream type classes for tap-gladly."""
import abc
import json
from pathlib import Path
from typing import Iterable, Optional

import pendulum
import requests
from singer_sdk import exceptions
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.mapper import RemoveRecordTransform, StreamMap, SameRecordTransform

from tap_gladly.client import gladlyStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ExportCompletedJobsStream(gladlyStream):
    """List export jobs stream."""

    name = "export__jobs"
    path = "/export/jobs?status=COMPLETED"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "export_jobs.json"

    def post_process(self, row, context):
        """Filter jobs that finished before start_date."""
        if pendulum.parse(row["parameters"]["endAt"]) >= pendulum.parse(
                self.config["start_date"]
        ):
            return row
        return

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"job_id": record["id"]}


class ExportFileTopicsStream(gladlyStream):
    """Abstract class, export conversation items stream."""

    name = "export__topics"
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


class ExportFileConversationItemsStream(gladlyStream):
    name = "export__conversation_conversation_items"
    path = "/export/jobs/{job_id}/files/conversation_items.jsonl"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ExportCompletedJobsStream
    ignore_parent_replication_key = True
    schema_filepath = SCHEMAS_DIR / "export_conversation.json"

    # def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
    #     """Return a context dictionary for child streams."""
    #     return {"conversationId": record.get("conversationId")}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for line in response.iter_lines():
            yield from extract_jsonpath(self.records_jsonpath, input=json.loads(line))

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""

        return {"conversationId": record[
            "conversationId"]} if 'conversationId' in record else record


class ExportFileConversationItemsByTypeStream(ExportFileConversationItemsStream,
                                              abc.ABC):
    """Abstract class, export conversation items stream."""

    @property
    def schema_filepath(self):
        """Return schema filepath by content type."""
        schemas_mapping = {
            "chat_message": "export_conversation-chat_message.json",
            "conversation_note": "export_conversation-conversation_note.json",
            "topic_change": "export_conversation-topic_change.json",
            "sms": "export_conversation-topic_change.json",
            "conversation_status_change": "export_conversation-conversation_status_change.json",
            # noqa
            "phone_call": "export_conversation-phone_call.json",
            "voicemail": "export_conversation-voicemail.json"
        }

        try:
            return SCHEMAS_DIR / schemas_mapping[self.content_type.lower()]
        except KeyError:
            raise exceptions.ConfigValidationError(
                "content_type config parameter is required for export "
                "conversations streams"
            )

    def post_process(self, row, context):
        """Filter rows by content type."""
        if row["content"]["type"].lower() == self.content_type.lower():
            return row


class ExportFileConversationItemsChatMessage(ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is chat_message."""

    name = "export__conversation_chat_message"
    content_type = "chat_message"


class ExportFileConversationItemsConversationNote(
    ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is conversation_note."""

    name = "export__conversation_conversation_note"
    content_type = "conversation_note"


class ExportFileConversationItemsTopicChange(ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is topic_change."""

    name = "export__conversation_topic_change"
    content_type = "topic_change"


class ExportFileConversationItemsSms(ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is sms."""

    name = "export__conversation_sms"
    content_type = "sms"


class ExportFileConversationItemsConversationStatusChange(
    ExportFileConversationItemsByTypeStream
):
    """Export conversation items stream.

    Where content type is conversation_status_change.
    """

    name = "export__conversation_conversation_status_change"
    content_type = "conversation_status_change"


class ExportFileConversationItemsPhoneCall(ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is phone_call."""

    name = "export__conversation_phone_call"
    content_type = "phone_call"


class ExportFileConversationItemsVoiceMail(ExportFileConversationItemsByTypeStream):
    """Export conversation items stream where content type is voicemail."""

    name = "export__conversation_voicemail"
    content_type = "voicemail"


class ConversationStream(gladlyStream):
    """Get Conversations."""

    name = "conversation"
    path = "/conversations/{conversationId}"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "conversation.json"
    parent_stream_type = ExportFileConversationItemsStream
    ignore_parent_replication_key = True

    def get_records(self, context: dict):
        if 'conversationId' not in context:
            return []
        return super().get_records(context)


class InboxStream(gladlyStream):
    """Export Inboxes."""

    name = "inbox"
    path = "/inboxes"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "inbox.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for line in response.iter_lines():
            yield from extract_jsonpath(self.records_jsonpath, input=json.loads(line))
