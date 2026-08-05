"""Tests replication key discovery and incremental query generation."""

import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from sqlalchemy import DATE, DATETIME, TIMESTAMP, create_mock_engine
from sqlalchemy.types import String

from tap_bigquery.client import quote_identifier
from tap_bigquery.connector import BigQueryConnector
from tap_bigquery.tap import TapBigQuery
from tests.utils.mockinspector import MockInspector

BOOKMARK = "2026-01-01T00:00:00+00:00"
STREAM_NAME = "mock-schema-mock_table"


def _dump(sql, *multiparams, **params):
    pass


@contextmanager
def mock_stream(columns, **config):
    """Discover a catalog from mock columns, then yield the resulting stream.

    The discovered catalog is fed back in, because a replication key reaches the
    stream through ``apply_catalog`` rather than through discovery alone.
    """
    cfg = {
        "project_id": "mock-project",
        "google_application_credentials": "MOCK",
        **config,
    }
    engine = create_mock_engine("bigquery://mockprojectid", _dump)
    inspector = MockInspector(
        ["mock-schema"],
        ["mock_table"],
        {"mock-schema.mock_table": columns},
    )
    with mock.patch("sqlalchemy.create_engine", return_value=engine), mock.patch(
        "sqlalchemy.inspect",
        return_value=inspector,
    ), mock.patch.object(
        BigQueryConnector,
        "_create_bigquery_client",
        return_value=mock.MagicMock(),
    ):
        catalog = TapBigQuery(config=cfg, catalog=None).catalog_dict
        tap = TapBigQuery(config=cfg, catalog=catalog)
        yield tap.streams[STREAM_NAME], engine


def select_only(stream, *names):
    """Restrict the stream's selected schema to the named columns."""
    schema = {"properties": {name: {"type": ["string", "null"]} for name in names}}
    return mock.patch.object(stream, "get_selected_schema", return_value=schema)


class TestReplicationKeyDiscovery(unittest.TestCase):
    """Test class for replication key selection at discovery time."""

    def test_prefers_updated_at_over_created_at(self):
        # given a table with several timestamp columns
        columns = [
            {"name": "id", "type": String(50)},
            {"name": "created_at", "type": TIMESTAMP()},
            {"name": "updated_at", "type": TIMESTAMP()},
        ]
        # when the catalog is discovered
        with mock_stream(columns) as (stream, _):
            # expect the most preferred name wins
            self.assertEqual(stream.replication_key, "updated_at")
            self.assertEqual(stream.replication_method, "INCREMENTAL")

    def test_falls_back_through_the_preference_order(self):
        # given a table whose only timestamp is further down the order
        columns = [
            {"name": "id", "type": String(50)},
            {"name": "created_at", "type": TIMESTAMP()},
        ]
        # when the catalog is discovered
        with mock_stream(columns) as (stream, _):
            # expect the next preferred name is used
            self.assertEqual(stream.replication_key, "created_at")

    def test_replication_key_column_config_overrides_auto_detection(self):
        # given a configured replication key that is not the preferred name
        columns = [
            {"name": "updated_at", "type": TIMESTAMP()},
            {"name": "ingested_at", "type": TIMESTAMP()},
        ]
        # when the catalog is discovered
        with mock_stream(columns, replication_key_column="ingested_at") as (stream, _):
            # expect the configured column wins
            self.assertEqual(stream.replication_key, "ingested_at")

    def test_unusable_replication_key_column_falls_back(self):
        # given a configured replication key that is not a timestamp column
        columns = [
            {"name": "name", "type": String(50)},
            {"name": "updated_at", "type": TIMESTAMP()},
        ]
        # when the catalog is discovered
        with mock_stream(columns, replication_key_column="name") as (stream, _):
            # expect auto-detection rather than an unusable key
            self.assertEqual(stream.replication_key, "updated_at")

    def test_no_timestamp_column_is_full_table(self):
        # given a table with no timestamp column at all
        columns = [{"name": "id", "type": String(50)}]
        # when the catalog is discovered
        with mock_stream(columns) as (stream, _):
            # expect full table replication
            self.assertIsNone(stream.replication_key)
            self.assertEqual(stream.replication_method, "FULL_TABLE")

    def test_timestamp_columns_are_offered_as_valid_replication_keys(self):
        # given a table with date and datetime columns
        columns = [
            {"name": "id", "type": String(50)},
            {"name": "event_date", "type": DATE()},
            {"name": "seen_at", "type": DATETIME()},
        ]
        # when the catalog is discovered
        with mock_stream(columns) as (stream, _):
            metadata = stream.catalog_entry["metadata"]
            stream_metadata = next(
                entry["metadata"] for entry in metadata if entry["breadcrumb"] == []
            )
            # expect both are advertised, even though neither is a preferred name
            self.assertCountEqual(
                stream_metadata["valid-replication-keys"],
                ["event_date", "seen_at"],
            )

    def test_reflect_indices_false_skips_index_reflection(self):
        # given an inspector that records index reflection
        inspector = MockInspector(
            ["mock-schema"],
            ["mock_table"],
            {"mock-schema.mock_table": [{"name": "id", "type": String(50)}]},
        )
        inspector.get_indexes = mock.Mock(return_value=[])
        engine = create_mock_engine("bigquery://mockprojectid", _dump)

        with mock.patch("sqlalchemy.create_engine", return_value=engine), mock.patch(
            "sqlalchemy.inspect",
            return_value=inspector,
        ), mock.patch.object(
            BigQueryConnector,
            "_create_bigquery_client",
            return_value=mock.MagicMock(),
        ):
            connector = BigQueryConnector(config={"project_id": "mock-project"})
            # when discovery runs with index reflection disabled
            connector.discover_catalog_entries(reflect_indices=False)
            # expect indexes were never reflected
            inspector.get_indexes.assert_not_called()

            # and when it runs with the default
            connector.discover_catalog_entries()
            # expect they were
            inspector.get_indexes.assert_called()


class TestIncrementalRowQuery(unittest.TestCase):
    """Test class for the row-by-row incremental query."""

    columns = [
        {"name": "id", "type": String(50)},
        {"name": "updated_at", "type": TIMESTAMP()},
        {"name": "payload", "type": String(50)},
    ]

    def compile_query(self, stream, engine, bookmark=BOOKMARK):
        query = stream._build_incremental_query(bookmark)
        compiled = query.compile(dialect=engine.dialect)
        return " ".join(str(compiled).split()), compiled.params

    def test_uses_strict_greater_than_and_emits_null_keys(self):
        with mock_stream(self.columns) as (stream, engine):
            with select_only(stream, "id", "updated_at", "payload"):
                sql, params = self.compile_query(stream, engine)

        # expect a strict comparison, never >=
        self.assertIn("`updated_at` > ", sql)
        self.assertNotIn(">=", sql)
        # expect NULL keys are still emitted
        self.assertIn("`updated_at` IS NULL", sql)
        # expect the bookmark is bound, not interpolated
        self.assertNotIn(BOOKMARK, sql)
        self.assertIn(BOOKMARK, params.values())

    def test_orders_by_replication_key(self):
        # is_sorted is True for INCREMENTAL streams, so unordered rows would raise
        # InvalidStreamSortException during state increment.
        with mock_stream(self.columns) as (stream, engine):
            self.assertTrue(stream.is_sorted)
            with select_only(stream, "id", "updated_at", "payload"):
                sql, _ = self.compile_query(stream, engine)

        self.assertIn("ORDER BY `mock_table`.`updated_at` ASC", sql)

    def test_selects_only_selected_columns(self):
        # given only a subset of columns selected
        with mock_stream(self.columns) as (stream, engine):
            with select_only(stream, "id", "updated_at"):
                sql, _ = self.compile_query(stream, engine)

        # expect deselected columns are not fetched
        self.assertIn("`mock_table`.`id`", sql)
        self.assertIn("`mock_table`.`updated_at`", sql)
        self.assertNotIn("payload", sql)

    def test_a_quote_in_the_bookmark_cannot_alter_the_query(self):
        # given a bookmark carrying SQL syntax
        malicious = "2026-01-01') OR TRUE OR ('x"
        with mock_stream(self.columns) as (stream, engine):
            with select_only(stream, "id", "updated_at"):
                sql, params = self.compile_query(stream, engine, bookmark=malicious)

        # expect it stays a bound value
        self.assertNotIn("OR TRUE", sql)
        self.assertIn(malicious, params.values())


class TestBatchExtractQuery(unittest.TestCase):
    """Test class for the EXPORT DATA batch path."""

    columns = [
        {"name": "id", "type": String(50)},
        {"name": "updated_at", "type": TIMESTAMP()},
    ]

    @contextmanager
    def batch_stream(self, columns=None, bookmark=BOOKMARK, **config):
        with mock_stream(
            columns or self.columns,
            google_storage_bucket="mock-bucket",
            **config,
        ) as (stream, engine):
            with mock.patch.object(
                stream,
                "get_starting_replication_key_value",
                return_value=bookmark,
            ), select_only(stream, "id", "updated_at"):
                yield stream, engine

    def test_parameterises_the_bookmark_and_quotes_identifiers(self):
        with self.batch_stream() as (stream, _):
            query, params = stream._build_extract_query()

        sql = " ".join(query.split())
        # expect a named parameter rather than an inlined literal
        self.assertIn("WHERE `updated_at` > @bookmark", sql)
        self.assertIn("`updated_at` IS NULL", sql)
        self.assertNotIn(BOOKMARK, sql)
        # expect the table reference is quoted
        self.assertIn("FROM `mock-schema.mock_table`", sql)

        self.assertEqual(len(params), 1)
        self.assertEqual(params[0].name, "bookmark")
        self.assertEqual(params[0].type_, "TIMESTAMP")
        self.assertEqual(params[0].value, BOOKMARK)

    def test_parameter_type_matches_a_date_replication_key(self):
        # given a DATE replication key, which BigQuery will not compare to a TIMESTAMP
        columns = [
            {"name": "id", "type": String(50)},
            {"name": "event_date", "type": DATE()},
        ]
        with self.batch_stream(
            columns=columns,
            replication_key_column="event_date",
        ) as (stream, _):
            self.assertEqual(stream.replication_key, "event_date")
            _, params = stream._build_extract_query()

        self.assertEqual(params[0].type_, "DATE")

    def test_a_quote_in_the_bookmark_cannot_alter_the_query(self):
        malicious = "2026-01-01') OR TRUE OR ('x"
        with self.batch_stream(bookmark=malicious) as (stream, _):
            query, params = stream._build_extract_query()

        self.assertNotIn("OR TRUE", query)
        self.assertEqual(params[0].value, malicious)

    def test_no_bookmark_means_no_filter(self):
        with self.batch_stream(bookmark=None) as (stream, _):
            query, params = stream._build_extract_query()

        self.assertNotIn("WHERE", query)
        self.assertEqual(params, [])

    def test_watermark_query_is_quoted(self):
        with self.batch_stream() as (stream, _):
            client = mock.MagicMock()
            client.query.return_value.result.return_value = [{"watermark": "2026-06-01"}]
            stream.__dict__["client"] = client

            watermark = stream._get_replication_key_watermark()

        query = " ".join(client.query.call_args.args[0].split())
        self.assertEqual(
            query,
            "SELECT MAX(`updated_at`) AS watermark FROM `mock-schema.mock_table`",
        )
        self.assertEqual(watermark, "2026-06-01")

    def test_get_batches_advances_the_bookmark_after_a_successful_export(self):
        # _sync_batches never calls _increment_stream_state, so without this the
        # bookmark would never move and every run would re-export the same delta.
        tempdir = Path(tempfile.mkdtemp(prefix="tap-bigquery-test-"))
        (tempdir / "part-000.json.gz").touch()

        with self.batch_stream() as (stream, _):
            client = mock.MagicMock()
            client.query.return_value.result.return_value = [
                {"watermark": "2026-06-01T00:00:00+00:00"},
            ]
            stream.__dict__["client"] = client

            with mock.patch("tap_bigquery.client.GCSFileSystem"), mock.patch(
                "tap_bigquery.client.tempfile.mkdtemp",
                return_value=str(tempdir),
            ), mock.patch.object(stream, "_increment_stream_state") as increment:
                batches = list(stream.get_batches("mock-bucket", None))

        self.assertEqual(len(batches), 1)
        increment.assert_called_once_with(
            {"updated_at": "2026-06-01T00:00:00+00:00"},
            context=None,
        )
        # expect the export ran with the bookmark bound as a query parameter
        job_config = client.query.call_args.kwargs["job_config"]
        self.assertEqual(job_config.query_parameters[0].name, "bookmark")

    def test_get_batches_leaves_the_bookmark_alone_when_there_is_no_watermark(self):
        tempdir = Path(tempfile.mkdtemp(prefix="tap-bigquery-test-"))
        (tempdir / "part-000.json.gz").touch()

        with self.batch_stream() as (stream, _):
            client = mock.MagicMock()
            client.query.return_value.result.return_value = [{"watermark": None}]
            stream.__dict__["client"] = client

            with mock.patch("tap_bigquery.client.GCSFileSystem"), mock.patch(
                "tap_bigquery.client.tempfile.mkdtemp",
                return_value=str(tempdir),
            ), mock.patch.object(stream, "_increment_stream_state") as increment:
                list(stream.get_batches("mock-bucket", None))

        increment.assert_not_called()


class TestQuoteIdentifier(unittest.TestCase):
    """Test class for identifier quoting."""

    def test_quotes(self):
        self.assertEqual(quote_identifier("updated_at"), "`updated_at`")

    def test_rejects_a_backtick(self):
        with self.assertRaises(ValueError):
            quote_identifier("updated_at` OR TRUE OR `x")
