"""Stream type classes for tap-gladly."""
import abc
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import requests
from singer_sdk import exceptions
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_gladly.client import gladlyStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ExportJobsStream(gladlyStream):
    """List export jobs stream."""

    name = "jobs"
    path = "/export/jobs"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType, description="Unique Job ID"),
        th.Property(
            "scheduleId", th.StringType, description="Schedule id the job belongs to"
        ),
        th.Property(
            "status",
            th.StringType,
            description='Current job status: "PENDING" "IN_PROGRESS" "COMPLETED"'
            ' "FAILED"',
        ),
        th.Property(
            "updatedAt",
            th.StringType,
        ),
        th.Property(
            "parameters",
            th.ObjectType(
                th.Property(
                    "type", th.StringType, description="Schedule id the job belongs to"
                ),
                th.Property(
                    "startAt",
                    th.StringType,
                    description="Start time for the export query",
                ),
                th.Property(
                    "endAt", th.StringType, description="End time for the export query"
                ),
            ),
        ),
        th.Property(
            "files",
            th.ArrayType(th.StringType),
        ),
    ).to_dict()

    # start_date
    def post_process(self, row, context):
        """As needed, append or transform raw data to match expected structure."""
        """As needed, append or transform raw data to match expected structure."""
        if "start_date" not in self.config:
            return row
        if datetime.strptime(
            row["parameters"]["startAt"], self._common_date_format
        ) > datetime.strptime(self.config["start_date"], self._common_date_format):
            return row
        return

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"job_id": record["id"]}


class ExportFileConversationItemsStream(gladlyStream, abc.ABC):
    """Abstract class, export conversation items stream."""

    name = "file_conversation_items"
    path = "/export/jobs/{job_id}/files/conversation_items.jsonl"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ExportJobsStream
    ignore_parent_replication_key = True

    @property
    def schema_filepath(self):
        """Return schema filepath by content type."""
        schemas_mapping = {
            "chat_message": "export_conversation-chat_message.json",
            "conversation_note": "export_conversation-conversation_note.json",
            "topic_change": "export_conversation-topic_change.json",
            "sms": "export_conversation-topic_change.json",
            "status_change": "export_conversation-conversation_status_change.json",
            "phone_call": "export_conversation-phone_call.json",
            "voicemail": "export_conversation-voicemail.json",
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

    name = "file_chat_message"
    content_type = "chat_message"


class ExportFileConversationItemsConversationNote(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is conversation_note."""

    name = "file_conversation_note"
    content_type = "conversation_note"


class ExportFileConversationItemsTopicChange(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is topic_change."""

    name = "file_topic_change"
    content_type = "topic_change"


class ExportFileConversationItemsSms(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is sms."""

    name = "file_sms"
    content_type = "sms"


class ExportFileConversationItemsStatusChange(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is status_change."""

    name = "file_status_change"
    content_type = "status_change"


class ExportFileConversationItemsPhoneCall(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is phone_call."""

    name = "file_phone_call"
    content_type = "phone_call"


class ExportFileConversationItemsVoiceMail(ExportFileConversationItemsStream):
    """Export conversation items stream where content type is voicemail."""

    name = "file_voicemail"
    content_type = "voicemail"
