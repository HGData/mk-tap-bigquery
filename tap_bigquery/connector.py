"""BigQuery connector implementation."""

from __future__ import annotations

import fnmatch
import json
import typing as t

import sqlalchemy
from google.cloud import bigquery
from google.oauth2.credentials import Credentials as OAuthCredentials
from singer_sdk import SQLConnector
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy_bigquery import (
    ARRAY,
    FLOAT,
    FLOAT64,
    INT64,
    INTEGER,
    NUMERIC,
    STRING,
    STRUCT,
)

if t.TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Supports two auth modes (controlled by auth_type config):
        - service_account (default): uses google_application_credentials
        - oauth: uses access_token passed from Argo via DAG env vars

        Returns:
            A new SQLAlchemy Engine.
        """
        auth_type = self.config.get("auth_type", "service_account")

        bq_client = self._create_bigquery_client(auth_type)
        url = self.sqlalchemy_url + "?user_supplied_client=true"
        return sqlalchemy.create_engine(
            url,
            echo=False,
            connect_args={"client": bq_client},
        )

    def _create_bigquery_client(self, auth_type: str) -> bigquery.Client:
        """Create a BigQuery client based on auth_type.

        Auth modes:
        - oauth: Uses client_id, client_secret, refresh_token to obtain
          and auto-refresh access tokens. Same pattern as mk-tap-salesforce
          and mk-tap-hubspot.
        - service_account: Uses google_application_credentials JSON.
        - Falls back to Application Default Credentials if nothing is configured.

        Returns:
            Authenticated BigQuery client.
        """
        project_id = self.config["project_id"]

        if auth_type == "oauth":
            client_id = self.config.get("client_id")
            client_secret = self.config.get("client_secret")
            refresh_token = self.config.get("refresh_token")
            if not all([client_id, client_secret, refresh_token]):
                msg = (
                    "client_id, client_secret, and refresh_token are all required "
                    "when auth_type is 'oauth'"
                )
                raise RuntimeError(msg)
            oauth_creds = OAuthCredentials(
                token=None,
                refresh_token=refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(project=project_id, credentials=oauth_creds)

        credentials: str | dict = self.config.get("google_application_credentials")

        if credentials:
            creds_dict = (
                json.loads(credentials) if isinstance(credentials, str) else credentials
            )
            creds_dict.setdefault("type", "service_account")
            creds_dict.setdefault("token_uri", "https://oauth2.googleapis.com/token")
            return bigquery.Client.from_service_account_info(
                creds_dict,
                project=project_id,
            )

        return bigquery.Client(project=project_id)

    def to_array_type(
        self,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> th.ArrayType:
        if (isinstance(sql_type, ARRAY)):
            if (isinstance(sql_type.item_type, STRING)):
                jsonschema = th.ArrayType(th.StringType)
            if (isinstance(sql_type.item_type, (INT64, INTEGER))):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, (FLOAT, FLOAT64, NUMERIC))):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, NUMERIC)):
                jsonschema = th.ArrayType(th.NumberType)
            if (isinstance(sql_type.item_type, STRUCT)):
                properties = self.struct_to_properties(sql_type.item_type)
                jsonschema = th.ArrayType(
                    th.ObjectType(
                        *properties,
                    ),
                )
            return jsonschema

    def struct_to_properties(
        self,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        properties = []
        if (isinstance(sql_type, STRUCT)):
            for name, type_ in sql_type._STRUCT_fields:
                self.logger.debug("%s: property type: %s", name, type_)
                if (isinstance(type_, STRING)):
                    properties.append(th.Property(name, th.StringType))
                if (isinstance(type_, (INT64, INTEGER))):
                    properties.append(th.Property(name, th.IntegerType))
                if (isinstance(type_, (FLOAT, FLOAT64, NUMERIC))):
                    properties.append(th.Property(name, th.NumberType))
                if (isinstance(type_, STRUCT)):
                    properties.append(th.Property(name, th.ObjectType(*self.struct_to_properties(type_))))
                if (isinstance(type_, ARRAY)):
                    properties.append(th.Property(name, self.to_array_type(type_)))
        return properties

    def to_jsonschema_type(
        self,
        column_name,
        sql_type: (
            str  # noqa: ANN401
            | sqlalchemy.types.TypeEngine
            | type[sqlalchemy.types.TypeEngine]
            | t.Any
        ),
    ) -> dict:
        self.logger.debug(
            "%s: Type %s, Array: %s, Tuple: %s",
            column_name,
            sql_type,
            sql_type._is_array,
            sql_type._is_tuple_type,
        )
        if (isinstance(sql_type, ARRAY)):
            jsonschema = self.to_array_type(sql_type)
            return jsonschema.type_dict
        if (isinstance(sql_type, STRUCT)):
            properties = self.struct_to_properties(sql_type)
            jsonschema = th.ObjectType(
                *properties,
            )
            return jsonschema.type_dict
        return super().to_jsonschema_type(sql_type)

    def discover_catalog_entries(
        self,
        *,
        exclude_schemas: t.Sequence[str] = (),
        reflect_indices: bool = True,
    ) -> list[dict]:
        """Override to avoid get_multi_columns which breaks with user_supplied_client.

        sqlalchemy-bigquery's get_multi_columns uses the schema name as
        the project in the API URL, causing 400 errors. This override
        uses per-table get_columns which resolves the project correctly.
        """
        result: list[dict] = []
        engine = self._engine
        inspected = sqlalchemy.inspect(engine)

        for schema_name in self.get_schema_names(engine, inspected):
            if schema_name in exclude_schemas:
                continue

            object_names = self.get_object_names(engine, inspected, schema_name)

            for table_name, is_view in object_names:
                catalog_entry = self.discover_catalog_entry(
                    engine, inspected, schema_name, table_name, is_view,
                )
                result.append(catalog_entry.to_dict())

        return result

    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,
        schema_name: str,
        table_name: str,
        is_view: bool,  # noqa: FBT001
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        # Initialize unique stream name
        unique_stream_id = f"{schema_name}-{table_name}"

        # Detect key properties
        possible_primary_keys: list[list[str]] = []
        pk_def = inspected.get_pk_constraint(table_name, schema=schema_name)
        if pk_def and "constrained_columns" in pk_def:
            possible_primary_keys.append(pk_def["constrained_columns"])

        # An element of the columns list is ``None`` if it's an expression and is
        # returned in the ``expressions`` list of the reflected index.
        possible_primary_keys.extend(
            index_def["column_names"]  # type: ignore[misc]
            for index_def in inspected.get_indexes(table_name, schema=schema_name)
            if index_def.get("unique", False)
        )

        key_properties = next(iter(possible_primary_keys), None)

        # Initialize columns list
        table_schema = th.PropertiesList()
        for column_def in inspected.get_columns(table_name, schema=schema_name):
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", False)
            jsonschema_type: dict = self.to_jsonschema_type(column_name, column_def["type"])
            if ("." not in column_name):
                table_schema.append(
                    th.Property(
                        name=column_name,
                        wrapped=th.CustomType(jsonschema_type),
                        nullable=is_nullable,
                        required=column_name in key_properties if key_properties else False,
                    ),
                )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"

    def get_object_names(
        self,
        engine,
        inspected,
        schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return discoverable object names."""
        # Bigquery inspections returns table names in the form
        # `schema_name.table_name` which later results in the project name
        # override due to specifics in behavior of sqlalchemy-bigquery
        #
        # Let's strip `schema_name` prefix on the inspection

        objects = [
            (table_name.split(".")[-1], is_view)
            for (table_name, is_view) in super().get_object_names(
                engine,
                inspected,
                schema_name,
            )
        ]
        if table_patterns := self.config.get("filter_tables"):
            return [
                o
                for o in objects
                if any(fnmatch.fnmatch(o[0], p) for p in table_patterns)
            ]

        return objects

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        """Return a list of schema names in DB, or overrides with user-provided values.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine

        Returns:
            List of schema names
        """
        if "filter_schemas" in self.config and len(self.config["filter_schemas"]) != 0:
            return self.config["filter_schemas"]
        return super().get_schema_names(engine, inspected)

__all__ = ["TapBigQuery", "BigQueryConnector", "BigQueryStream"]
