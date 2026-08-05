"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from __future__ import annotations

import math
import tempfile
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

import sqlalchemy
from gcsfs import GCSFileSystem
from google.cloud import bigquery
from singer_sdk import SQLStream
from singer_sdk.helpers._batch import JSONLinesEncoding

from tap_bigquery.connector import REPLICATION_KEY_TYPES, BigQueryConnector

if TYPE_CHECKING:
    from singer_sdk.helpers import types

# Name of the named query parameter carrying the incremental bookmark.
BOOKMARK_PARAMETER = "bookmark"


def quote_identifier(name: str) -> str:
    """Backtick-quote a BigQuery identifier.

    Args:
        name: Identifier to quote.

    Raises:
        ValueError: If the identifier itself contains a backtick.

    Returns:
        The quoted identifier.
    """
    if "`" in name:
        msg = f"Invalid BigQuery identifier: {name!r}"
        raise ValueError(msg)
    return f"`{name}`"


class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector

    @cached_property
    def client(self):
        """Create a BigQuery client, reusing the connector's auth logic."""
        return self.connector._create_bigquery_client(
            self.config.get("auth_type", "service_account"),
        )

    def prepare_serialisation(self, _dict, _keychain = []):
        """
        Fix 'ValueError: Out of range float values are not JSON compliant'
        Recursively delete keys with the value ``None`` in a dictionary.
        Recursively delete keys with the value ``math.inf`` in a dictionary.
        NB - This alters the input so the return is just a convenience.
        """
        for key, value in list(_dict.items()):
            if isinstance(value, dict):
                self.prepare_serialisation(value, _keychain[:] + [key])
            # elif value is None:
            #     del _dict[key]
            elif isinstance(value, float) and math.isinf(value):
                self.logger.warning(
                    "Dropping unsupported value from '%s' -> '%s'",
                    str(_keychain),
                    str(key),
                )
                del _dict[key]
            elif isinstance(value, list):
                new_array = [tup for tup in value if not isinstance(tup, float) or not math.isinf(tup)]
                if len(_dict[key]) != len(new_array):
                    self.logger.warning(
                        "Dropping %s unsupported values from '%s'",
                        len(_dict[key]) - len(new_array),
                        str(_keychain[:] + [key]),
                    )
                    _dict[key] = new_array
                for v_i in value:
                    if isinstance(v_i, dict):
                        self.prepare_serialisation(v_i, _keychain[:] + [key])
        return _dict

    def post_process(  # noqa: PLR6301
        self,
        row: types.Record,
        context: types.Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        return self.prepare_serialisation(row)

    def get_records(
        self,
        context: types.Context | None = None,
        *,
        partition: types.Context | None = None,
    ):
        """Use strict > for replication key to avoid re-pulling records at the bookmark boundary.

        Overrides singer-sdk's default >= comparison, which causes all records to be
        re-pulled every run when source rows share a uniform batch timestamp (e.g.,
        50k events all loaded with the same updated_at).

        Args:
            context: Stream partition context, passed positionally by the SDK.
            partition: Legacy alias for ``context``, kept for callers that still
                use the older keyword name.
        """
        if context is None:
            context = partition

        start_value = self.get_starting_replication_key_value(context)

        if not self.replication_key or not start_value:
            yield from super().get_records(context)
            return

        self.logger.info(
            "Incremental extract: %s > '%s' (or NULL)",
            self.replication_key,
            start_value,
        )

        query = self._build_incremental_query(start_value)

        # _connect() is what applies stream_results=True, so results are not
        # buffered in memory for large extracts.
        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed = self.post_process(dict(record), context)
                if transformed is not None:
                    yield transformed

    def _build_incremental_query(self, start_value):
        """Build the SELECT for an incremental row-by-row extract.

        Mirrors singer-sdk's ``SQLStream.get_records``: selected columns only, and
        ordered by the replication key, since ``is_sorted`` is True for INCREMENTAL
        streams and unordered rows would raise ``InvalidStreamSortException``. The
        only differences are the strict ``>`` and the NULL-key rows.

        The bookmark is bound as a parameter typed from the column itself, so a
        DATE or DATETIME key is compared against a matching literal rather than a
        TIMESTAMP, which BigQuery would reject.

        Args:
            start_value: The replication key bookmark to filter on.

        Returns:
            A SQLAlchemy select statement.
        """
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=list(self.get_selected_schema()["properties"].keys()),
        )
        replication_key_col = table.columns[self.replication_key]
        order_by = (
            sqlalchemy.nulls_first(replication_key_col.asc())
            if self.supports_nulls_first
            else replication_key_col.asc()
        )
        query = (
            table.select()
            .where(
                sqlalchemy.or_(
                    replication_key_col > start_value,
                    replication_key_col.is_(None),
                ),
            )
            .order_by(order_by)
        )

        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold, so
            # MaxRecordsLimitException is still raised by the caller.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        return query

    def get_batch_config(self, config):
        return config.get("google_storage_bucket")

    def get_batches(self, bucket: str, context):
        destination_uri = f"gs://{bucket}/{self.fully_qualified_name}-*.json.gz"

        self.logger.info(
            "Running extract job from table '%s' to bucket '%s'",
            self.fully_qualified_name,
            bucket,
        )

        # Capture the ceiling before exporting. Rows committed between this query
        # and the export still get exported, and will be exported again next run
        # because the bookmark stays behind them - at-least-once, which downstream
        # dedup absorbs. Reading the watermark afterwards could skip rows instead.
        watermark = self._get_replication_key_watermark()

        query, query_parameters = self._build_extract_query()
        self.logger.debug(query)

        extract_job = self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
        )

        try:
            extract_job.result()  # Waits for job to complete.
        except:
            if extract_job.running():
                self.logger.info("Cancelling extract job")
                extract_job.cancel()
            raise

        self.logger.info(
            "Extract job completed in %ss",
            (extract_job.ended - extract_job.started).total_seconds(),
        )

        fs = GCSFileSystem("gs", token=self.client._credentials)  # noqa: SLF001

        tempdir = Path(tempfile.mkdtemp(prefix="tap-bigquery-"))

        self.logger.info("Downloading extract job files to '%s'", tempdir)

        try:
            fs.get(destination_uri, tempdir)
        finally:
            self.logger.info("Cleaning up files in bucket")
            fs.rm(destination_uri)

        files = list(tempdir.glob("*.json.gz"))

        self.logger.info(
            "Downloaded %d file(s): %s",
            len(files),
            [str(f) for f in files],
        )

        # _sync_batches emits a STATE message after each batch it receives, but it
        # never calls _increment_stream_state, so without this the bookmark would
        # never advance and every run would re-export the same delta. Advancing
        # here means it only happens once the export and download have succeeded.
        if watermark is not None:
            self._increment_stream_state(
                {self.replication_key: watermark},
                context=context,
            )

        yield JSONLinesEncoding("gzip"), [f.as_uri() for f in files]

    def _replication_key_parameter_type(self) -> str:
        """Return the BigQuery parameter type matching the replication key column.

        BigQuery rejects ``DATE > TIMESTAMP`` and ``DATETIME > TIMESTAMP``, so the
        bound bookmark has to carry the column's own type.

        Returns:
            One of the types in ``REPLICATION_KEY_TYPES``.
        """
        table = self.connector.get_table(full_table_name=self.fully_qualified_name)
        type_name = type(table.columns[self.replication_key].type).__name__.upper()
        return type_name if type_name in REPLICATION_KEY_TYPES else "TIMESTAMP"

    def _get_replication_key_watermark(self):
        """Return MAX(replication_key) for the table, or None.

        Returns:
            The highest replication key value present, or None when the stream is
            not incremental or the table holds no non-NULL values.
        """
        if not self.replication_key:
            return None

        query = (
            f"SELECT MAX({quote_identifier(self.replication_key)}) AS watermark "
            f"FROM {quote_identifier(self.fully_qualified_name)}"
        )
        rows = list(self.client.query(query).result())
        return rows[0]["watermark"] if rows else None

    def _build_extract_query(self):
        """Build the EXPORT DATA statement and its query parameters.

        Returns:
            A tuple of the SQL statement and the list of query parameters it binds.
        """
        expressions = _generate_property_expressions(
            self.get_selected_schema()["properties"],
        )

        where_clause = ""
        query_parameters: list = []
        if self.replication_key:
            start_value = self.get_starting_replication_key_value(None)
            if start_value:
                self.logger.info(
                    "Incremental extract: %s > '%s' (or NULL)",
                    self.replication_key,
                    start_value,
                )
                column = quote_identifier(self.replication_key)
                where_clause = (
                    f"WHERE {column} > @{BOOKMARK_PARAMETER} "
                    f"OR {column} IS NULL"
                )
                query_parameters.append(
                    bigquery.ScalarQueryParameter(
                        BOOKMARK_PARAMETER,
                        self._replication_key_parameter_type(),
                        start_value,
                    ),
                )

        query = """
        EXPORT DATA
            OPTIONS (
                uri = 'gs://{bucket}/{table}-*.json.gz',
                format = 'JSON',
                compression='GZIP',
                overwrite = true
            )
        AS (
            SELECT {expressions}
            FROM {quoted_table}
            {where_clause}
        )
        """

        return (
            query.format(
                bucket=self.config["google_storage_bucket"],
                table=self.fully_qualified_name,
                quoted_table=quote_identifier(self.fully_qualified_name),
                expressions=", ".join(expressions),
                where_clause=where_clause,
            ),
            query_parameters,
        )


def _generate_property_expressions(properties: dict, qualifier: str | None = None):
    for name, schema in properties.items():
        qualified_name = f"{qualifier}.{name}" if qualifier else name

        if "properties" in schema:
            struct = "STRUCT({expressions}) AS {name}"  # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
            expressions = _generate_property_expressions(
                schema["properties"],
                qualifier=qualified_name,
            )

            yield struct.format(expressions=", ".join(expressions), name=name)

        else:
            yield qualified_name
