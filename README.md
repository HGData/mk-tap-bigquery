# `tap-bigquery`

Singer tap for Google BigQuery.

Built with the [Meltano Singer SDK](https://sdk.meltano.com), forked from
[MeltanoLabs/tap-bigquery](https://github.com/MeltanoLabs/tap-bigquery).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| project_id | True | None | GCP project that owns the datasets to extract from. |
| auth_type | False | `service_account` | Authentication mode: `service_account` or `oauth`. See [Source Authentication and Authorization](#source-authentication-and-authorization). |
| google_application_credentials | False | None | Service account credentials, given either as the key JSON itself or as a path to a key file. Used when `auth_type` is `service_account`. |
| client_id | False | None | Google OAuth client ID. Required when `auth_type` is `oauth`. |
| client_secret | False | None | Google OAuth client secret. Required when `auth_type` is `oauth`. |
| refresh_token | False | None | Google OAuth refresh token. Required when `auth_type` is `oauth`. |
| replication_key_column | False | None | Name of a TIMESTAMP column to use as the replication key for incremental extraction (e.g. `updated_at`). See [Replication](#replication). |
| filter_schemas | False | None | Array of schema (dataset) names. When provided, only these datasets are processed. If left blank, the tap discovers ALL available datasets. |
| filter_tables | False | None | Array of table names. When provided, only these tables are processed. Shell patterns (`fnmatch`) are supported, e.g. `events_*`. If left blank, the tap discovers ALL available tables. |
| google_storage_bucket | False | None | An optional Google Storage bucket. When supplied, a file based (`BATCH`) extract is used instead of row-by-row streaming. |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| flattening_enabled | False | None | `True` to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |
| batch_config | False | None | Configuration for BATCH message capabilities. |

A full list of supported settings and capabilities is available by running:
`tap-bigquery --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working
directory's `.env` if the `--config=ENV` is provided, such that config values will be
considered if a matching environment variable is set either in the terminal context or
in the `.env` file.

## Source Authentication and Authorization

The tap supports two authentication modes, selected with `auth_type`. If neither mode is
configured, the tap falls back to
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

### `service_account` (default)

Set `google_application_credentials` to **either** the service account key JSON itself
**or** a path to a key file — both are accepted:

```json
{
  "project_id": "my-gcp-project",
  "auth_type": "service_account",
  "google_application_credentials": "/secrets/bigquery-key.json"
}
```

```json
{
  "project_id": "my-gcp-project",
  "auth_type": "service_account",
  "google_application_credentials": "{\"type\": \"service_account\", \"client_email\": \"...\", \"private_key\": \"...\"}"
}
```

The value may also be supplied as an already-decoded JSON object rather than a string.
If the value does not parse as JSON, it is treated as a file path.

The service account needs `roles/bigquery.dataViewer` on the datasets being read and
`roles/bigquery.jobUser` on the project. When `google_storage_bucket` is set, it also
needs write access to that bucket, since the batch extract writes objects there and
deletes them after download.

### `oauth`

Set `client_id`, `client_secret` and `refresh_token`. The tap builds refresh-capable
credentials, so access tokens are obtained and renewed automatically for the lifetime of
the refresh token — no pre-fetched access token is needed. All three settings are
required in this mode; if any is missing the tap fails at startup with a clear error.

```json
{
  "project_id": "my-gcp-project",
  "auth_type": "oauth",
  "client_id": "...apps.googleusercontent.com",
  "client_secret": "...",
  "refresh_token": "..."
}
```

## Replication

Streams replicate incrementally when a TIMESTAMP replication key is available, and
`FULL_TABLE` otherwise.

The key is chosen at discovery time, per table:

1. If `replication_key_column` is set and that column exists on the table as a
   TIMESTAMP/DATETIME/DATE column, it is used.
2. Otherwise the tap auto-detects, preferring, in order:
   `updated_at`, `modified_at`, `last_modified`, `_sdc_batched_at`, `created_at`.
3. If the table has no timestamp column at all, the stream is `FULL_TABLE`.

Incremental extracts filter with a **strict** `>` against the bookmark, plus rows whose
key is NULL:

```sql
WHERE updated_at > TIMESTAMP('<bookmark>') OR updated_at IS NULL
```

Strict `>` is deliberate. With `>=`, a table whose rows share a uniform batch timestamp
(e.g. 50k events all loaded with the same `updated_at`) re-pulls the entire batch on
every run. The trade-off is that a row committed *after* a sync but carrying an
`updated_at` exactly equal to the bookmark is not picked up — there is no lookback
window. Rows with a NULL replication key are re-emitted on every run, so downstream
de-duplication should be in place.

The same filter is applied on both extract paths: the row-by-row path and the
`EXPORT DATA` path used when `google_storage_bucket` is set.

## Usage

You can easily run `tap-bigquery` by itself or in a pipeline using
[Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-bigquery --version
tap-bigquery --help
tap-bigquery --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-bigquery` CLI interface directly using `poetry run`:

```bash
poetry run tap-bigquery --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-bigquery
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-bigquery --version
```

```bash
# OR run a test `elt` pipeline:
meltano elt tap-bigquery target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more
instructions on how to use the SDK to develop your own taps and targets.
