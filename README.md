# pbk-ingest

Ingests PubliBike station availability every 5 minutes: API -> GCS (raw + parsed) -> BigQuery `bronze.publibike_snapshots`.
Design and rationale: `ingest.md`.

```
Cloud Scheduler -> Cloud Run Job (main_extract.py) -> GCS -> Pub/Sub (pbk-load) -> Cloud Function (main_load.py) -> BigQuery
```

- **extract** fetches the master list (`velospot.responseData`) and every station page concurrently, parses them (`src/parsing.py`) and writes
  `stations/raw_data/{date}/{time}/raw.jsonl` (untouched responses, one line per station) then `data.json` (parsed snapshot), then publishes the `gs://` URI of `data.json`.
- **load** reads `data.json`, validates it and appends one row to BigQuery. It writes no files; the load job id is derived from the snapshot id, so redelivered messages are no-ops.

## Develop

```bash
cp .envrc.example .envrc && direnv allow
make install
make hooks       # one-time: run the pre-commit hooks on every git commit
make tests
make ruff
make extract     # real run: writes to the bucket and publishes to Pub/Sub of $GCP_PROJECT_ID
```

`archive/` holds the previous Airflow / monolithic implementation (not linted, not tested, not deployed).

## Deploy

`.github/workflows`: PR checks (CI only), `cd.yml` (push to `dev`/`stg`: CI + deploy; image built on dev, promoted by SHA), `release.yml` (tag `v*.*.*`: deploy to prd).
Infrastructure (SAs, topic, bucket, dataset, WIF) comes from pbk-infra; variables from pbk-variables.
