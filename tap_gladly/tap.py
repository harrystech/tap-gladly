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
    ExportFileConversationItemsCustomerActivity,
    ExportFileConversationItemsFacebookMessage,
    ExportFileConversationItemsInstagramDirect,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsTwitter,
    ExportFileConversationItemsVoiceMail,
    ExportFileConversationItemsWhatsapp,
    ExportFileTopicsStream,
)

STREAM_TYPES = [
    ExportCompletedJobsStream,
    ExportFileConversationItemsChatMessage,
    ExportFileConversationItemsConversationNote,
    ExportFileConversationItemsTopicChange,
    ExportFileConversationItemsSms,
    ExportFileConversationItemsConversationStatusChange,
    ExportFileConversationItemsPhoneCall,
    ExportFileConversationItemsVoiceMail,
    ExportFileTopicsStream,
    ExportFileConversationItemsCustomerActivity,
    ExportFileConversationItemsFacebookMessage,
    ExportFileConversationItemsTwitter,
    ExportFileConversationItemsInstagramDirect,
    ExportFileConversationItemsWhatsapp,
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
            required=True,
            description="The earliest job date to sync, format %Y-%m-%dT%H:%M:%SZ",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest job date to sync, format %Y-%m-%dT%H:%M:%SZ",
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
