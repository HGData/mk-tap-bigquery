"""BigQuery tap class."""

from __future__ import annotations

from singer_sdk import SQLStream, SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_bigquery.client import BigQueryStream


class TapBigQuery(SQLTap):
    """Google BigQuery tap."""

    name = "tap-bigquery"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            description="GCP Project",
        ),
        th.Property(
            "auth_type",
            th.StringType,
            required=False,
            description=(
                "Authentication type: 'service_account' (default) or 'oauth'. "
                "For service_account, provide google_application_credentials. "
                "For oauth, provide client_id, client_secret, and refresh_token."
            ),
        ),
        th.Property(
            "google_application_credentials",
            th.OneOf(
                th.StringType,
                th.ObjectType(),
            ),
            required=False,
            secret=True,
            description="JSON content or path to service account credentials.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="Google OAuth client ID.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            secret=True,
            description="Google OAuth client secret.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            secret=True,
            description="Google OAuth refresh token.",
        ),
        th.Property(
            "google_storage_bucket",
            th.StringType,
            description="An optional Google Storage Bucket, when supplied a file based extract will be used.",
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description=(
                "If an array of schema names is provided, the tap will only process "
                "the specified BigQuery schemas (datasets) and ignore others. If left "
                " blank, the tap automatically determines ALL available schemas."
            ),
        ),
        th.Property(
            "filter_tables",
            th.ArrayType(th.StringType),
            description=(
                "If an array of table names is provided, the tap will only process "
                "the specified BigQuery tables and ignore others. If left blank, the "
                "tap automatically determines ALL available tables. Shell patterns are "
                "supported."
            ),
        ),
        th.Property(
            "replication_key_column",
            th.StringType,
            required=False,
            description=(
                "Name of a TIMESTAMP column to use as the replication key for "
                "incremental extraction (e.g. 'updated_at'). When set, the tap "
                "extracts only records where this column >= the last bookmark. "
                "If not set, the tap auto-detects from well-known column names "
                "(updated_at, modified_at, etc.) or falls back to FULL_TABLE."
            ),
        ),
    ).to_dict()

    default_stream_class: type[SQLStream] = BigQueryStream


if __name__ == "__main__":
    TapBigQuery.cli()
