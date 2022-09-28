"""gladly tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_gladly.streams import (
    ExportCompletedJobsStream,
    ExportFileConversationItemsChatMessage,
    ExportFileConversationItemsConversationNote,
    ExportFileConversationItemsConversationStatusChange,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsVoiceMail,
    ExportFileTopicsStream,
    InboxStream,
    ConversationStream, ExportFileConversationItemsStream
)

STREAM_TYPES = [
    ConversationStream,
    ExportCompletedJobsStream,
    ExportFileConversationItemsStream,
    ExportFileConversationItemsChatMessage,
    ExportFileConversationItemsConversationNote,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsConversationStatusChange,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsVoiceMail,
    ExportFileTopicsStream,
    InboxStream
]


class Tapgladly(Tap):
    """gladly tap class."""

    name = "tap-gladly"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The username to authenticate against the API service",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="The username to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync, format %Y-%m-%dT%H:%M:%SZ",
        ),
        th.Property(
            "api_url_base",
            th.StringType,
            required=True,
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapgladly.cli()
