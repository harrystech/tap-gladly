"""gladly tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_gladly.streams import (
    ExportFileConversationItemsChatMessage,
    ExportFileConversationItemsConversationNote,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsStatusChange,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsVoiceMail,
    ExportJobsStream,
)

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    ExportJobsStream,
    ExportFileConversationItemsChatMessage,
    ExportFileConversationItemsConversationNote,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsStatusChange,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsVoiceMail,
]


class Tapgladly(Tap):
    """gladly tap class."""

    name = "tap-gladly"

    # TODO: Update this section with the actual config values you expect:
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
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url_base",
            th.StringType,
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapgladly.cli()
