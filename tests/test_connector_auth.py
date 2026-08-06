"""Tests auth selection in BigQueryConnector._create_bigquery_client."""

import json
import unittest
from unittest import mock

from tap_bigquery.connector import BigQueryConnector

SERVICE_ACCOUNT_KEY = {
    "client_email": "tap@mock-project.iam.gserviceaccount.com",
    "private_key": "-----BEGIN PRIVATE KEY-----\nmock\n-----END PRIVATE KEY-----\n",
}


def connector(**config):
    return BigQueryConnector(config={"project_id": "mock-project", **config})


@mock.patch("tap_bigquery.connector.bigquery")
class TestCreateBigQueryClient(unittest.TestCase):
    """Test class for the auth-mode selection tests."""

    def test_service_account_json_content(self, mock_bigquery):
        # given google_application_credentials holding the key JSON itself
        conn = connector(
            google_application_credentials=json.dumps(SERVICE_ACCOUNT_KEY),
        )
        # when a client is created
        client = conn._create_bigquery_client("service_account")

        # expect the parsed key is used, with the service account defaults filled in
        mock_bigquery.Client.from_service_account_info.assert_called_once_with(
            {
                **SERVICE_ACCOUNT_KEY,
                "type": "service_account",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            project="mock-project",
        )
        mock_bigquery.Client.from_service_account_json.assert_not_called()
        self.assertEqual(
            client,
            mock_bigquery.Client.from_service_account_info.return_value,
        )

    def test_service_account_dict(self, mock_bigquery):
        # given google_application_credentials already decoded to a dict
        conn = connector(google_application_credentials=dict(SERVICE_ACCOUNT_KEY))
        # when a client is created
        conn._create_bigquery_client("service_account")

        # expect the dict is used directly
        mock_bigquery.Client.from_service_account_info.assert_called_once_with(
            {
                **SERVICE_ACCOUNT_KEY,
                "type": "service_account",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            project="mock-project",
        )

    def test_service_account_file_path(self, mock_bigquery):
        # given google_application_credentials holding a path rather than JSON
        conn = connector(google_application_credentials="/tmp/creds.json")
        # when a client is created
        client = conn._create_bigquery_client("service_account")

        # expect the value is treated as a key file path, not parsed as JSON
        mock_bigquery.Client.from_service_account_json.assert_called_once_with(
            "/tmp/creds.json",
            project="mock-project",
        )
        mock_bigquery.Client.from_service_account_info.assert_not_called()
        self.assertEqual(
            client,
            mock_bigquery.Client.from_service_account_json.return_value,
        )

    def test_service_account_is_the_default_auth_type(self, mock_bigquery):
        # given no auth_type configured at all
        conn = connector(google_application_credentials="/tmp/creds.json")
        self.assertNotIn("auth_type", conn.config)

        # when the engine resolves the mode itself, rather than being told
        with mock.patch("sqlalchemy.create_engine") as create_engine:
            conn.create_engine()

        # expect service account handling, including the path fallback
        mock_bigquery.Client.from_service_account_json.assert_called_once_with(
            "/tmp/creds.json",
            project="mock-project",
        )
        # and expect that client is the one handed to SQLAlchemy
        self.assertEqual(
            create_engine.call_args.kwargs["connect_args"]["client"],
            mock_bigquery.Client.from_service_account_json.return_value,
        )

    def test_unsupported_auth_type_raises(self, mock_bigquery):  # noqa: ARG002
        # given a typo in auth_type, which must not silently use ambient credentials
        conn = connector(auth_type="oauth2", google_application_credentials="{}")

        # when a client is created, expect a clear error
        with self.assertRaises(ValueError) as ctx:
            conn._create_bigquery_client("oauth2")

        self.assertIn("oauth2", str(ctx.exception))

    def test_oauth_scopes_bigquery_only_without_a_bucket(self, mock_bigquery):
        # given oauth with no batch bucket configured
        conn = connector(
            auth_type="oauth",
            client_id="mock-client-id",
            client_secret="mock-client-secret",
            refresh_token="mock-refresh-token",
        )
        # when a client is created
        conn._create_bigquery_client("oauth")

        # expect no Cloud Storage scope is requested
        credentials = mock_bigquery.Client.call_args.kwargs["credentials"]
        self.assertEqual(
            list(credentials.scopes),
            ["https://www.googleapis.com/auth/bigquery"],
        )

    def test_oauth_adds_storage_scope_for_batch_extracts(self, mock_bigquery):
        # given oauth with a batch bucket, which the batch path reads and deletes from
        conn = connector(
            auth_type="oauth",
            client_id="mock-client-id",
            client_secret="mock-client-secret",
            refresh_token="mock-refresh-token",
            google_storage_bucket="mock-bucket",
        )
        # when a client is created
        conn._create_bigquery_client("oauth")

        # expect the Cloud Storage scope is requested too
        credentials = mock_bigquery.Client.call_args.kwargs["credentials"]
        self.assertIn(
            "https://www.googleapis.com/auth/devstorage.read_write",
            credentials.scopes,
        )

    def test_oauth_builds_refreshable_credentials(self, mock_bigquery):
        # given a full set of oauth settings
        conn = connector(
            auth_type="oauth",
            client_id="mock-client-id",
            client_secret="mock-client-secret",
            refresh_token="mock-refresh-token",
        )
        # when a client is created
        conn._create_bigquery_client("oauth")

        # expect credentials that can refresh themselves
        mock_bigquery.Client.assert_called_once()
        credentials = mock_bigquery.Client.call_args.kwargs["credentials"]
        self.assertEqual(credentials.refresh_token, "mock-refresh-token")
        self.assertEqual(credentials.client_id, "mock-client-id")
        self.assertEqual(credentials.client_secret, "mock-client-secret")
        self.assertEqual(
            mock_bigquery.Client.call_args.kwargs["project"],
            "mock-project",
        )

    def test_oauth_missing_settings_raises(self, mock_bigquery):  # noqa: ARG002
        for missing in ("client_id", "client_secret", "refresh_token"):
            with self.subTest(missing=missing):
                # given oauth settings with one value absent
                oauth_config = {
                    "auth_type": "oauth",
                    "client_id": "mock-client-id",
                    "client_secret": "mock-client-secret",
                    "refresh_token": "mock-refresh-token",
                }
                del oauth_config[missing]
                conn = connector(**oauth_config)

                # when a client is created, expect a clear error
                with self.assertRaises(RuntimeError) as ctx:
                    conn._create_bigquery_client("oauth")

                self.assertIn("required", str(ctx.exception))

    def test_no_credentials_falls_back_to_adc(self, mock_bigquery):
        # given neither oauth nor service account settings
        conn = connector()
        # when a client is created
        conn._create_bigquery_client("service_account")

        # expect Application Default Credentials
        mock_bigquery.Client.assert_called_once_with(project="mock-project")
        mock_bigquery.Client.from_service_account_info.assert_not_called()
        mock_bigquery.Client.from_service_account_json.assert_not_called()
