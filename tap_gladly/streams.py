"""Stream type classes for tap-gladly."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_gladly.client import gladlyStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class ExportJobsStream(gladlyStream):
    """Define custom stream."""
    name = "jobs"
    path = "/export/jobs"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="Unique Job ID"
        ),
        th.Property(
            "scheduleId",
            th.StringType,
            description="Schedule id the job belongs to"
        ),
        th.Property(
            "status",
            th.StringType,
            description='Current job status: "PENDING" "IN_PROGRESS" "COMPLETED" "FAILED"'
        ),
        th.Property(
            "updatedAt",
            th.StringType,
            description='Current job status: "PENDING" "IN_PROGRESS" "COMPLETED" "FAILED"'
        ),
        th.Property(
            "parameters",
            th.ObjectType(
                th.Property(
                    "type",
                    th.StringType,
                    description="Schedule id the job belongs to"
                ),
                th.Property(
                    "startAt",
                    th.StringType,
                    description='Start time for the export query'
                ),
                th.Property(
                    "endAt",
                    th.StringType,
                    description='End time for the export query'
                ),
            ),
            description='Current job status: "PENDING" "IN_PROGRESS" "COMPLETED" "FAILED"'
        ),
        th.Property(
            "files",
            th.ArrayType(th.StringType),
            description='Current job status: "PENDING" "IN_PROGRESS" "COMPLETED" "FAILED"'
        ),

    ).to_dict()


