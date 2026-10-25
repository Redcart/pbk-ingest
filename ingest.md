# pbk-ingest — Data Ingestion Context

**Purpose:** Ingest PubliBike real-time data from external APIs, store raw in Google Cloud Storage (bronze layer), load to BigQuery. Creates immutable, timestamped snapshots of bike availability across the network.

## What This Repository Is NOT

- Infrastructure provisioning (Terraform) — handled by pbk-infra
- Data transformation — handled by pbk-dbt
- Machine learning — handled by pbk-ml
- BI dashboarding — handled by pbk-dash

---

## Architecture: Cloud Run Job → Pub/Sub → Cloud Function

**Event-driven, asynchronous ingestion pipeline:**

```
Cloud Scheduler (every 5 min)
         ↓
   Cloud Run Job (collect, parse, merge)
  (async PubliBike API calls)
         ↓
   Write raw.jsonl, then data.json to GCS
         ↓
   Publish the gs:// URI of data.json to Pub/Sub
         ↓
   Cloud Function (validate & load)
         ↓
   BigQuery (append one row per snapshot)
```

### Why This Design?

- **Cloud Run Job** for extract: handles the long-running async collection (~1,700 station calls) **and the parsing** — everything that touches the PubliBike API; a parse failure fails the run before anything is published
- **Pub/Sub** in between: Decouples extract from load, buffers spikes, enables retries
- **Cloud Function** for load: only moves a validated `data.json` into BigQuery (reads GCS, writes no files); simple, fast, triggered by message
- **Immutable raw data:** Always append, never overwrite (audit trail)

---

## Data Collection: PubliBike API

### Step 1: Master Endpoint (All Stations)

```
GET https://rest.publibike.ch/v1/public/all/stations
```

The response has two top-level keys; **`publibike` is empty** and the stations are under **`velospot.responseData`** (~1,700 stations, all Velospot networks — not filtered by network):

```json
{
  "publibike": {},
  "velospot": {
    "responseData": [
      {
        "station_id": "SElvRkxlVnpsMjVFeiswQjVBR3JuZz09",
        "station_name": "1820 Restaurant & Cocktails Bar - Montreux",
        "station_address": "Grand' Rue 57, 1820 Montreux, Suisse",
        "stationNumber": "600654",
        "lat": "46.4349", "lng": "6.9098",
        "detailsRoute": "https://www.velospot.info/customer/public/getStationInfo/<network>/<station>",
        "totalBike": 3, "totalElectricalBike": 3, "totalNonElectricalBike": 0
      }
    ],
    "responseStatus": {"STATUS": "SUCCESS"}
  }
}
```

- **`station_id` and the network id are opaque encrypted strings** (base64-like, may contain `/`, `+`, `=`). Each station carries its own **`detailsRoute`**, so the station URL is never built from a template.
- **The master totals lag the station pages** (cached; off by one here and there). They are only used as a debug-level cross-check, never as data.

### Step 2: Per-Station Endpoint (Async)

For each station, `GET detailsRoute`. The answer is **JSON wrapping an HTML fragment** (`{"renderHtml": "<div>…"}`): bike ID and, for e-bikes, the remaining range (e.g. `41-50 km`), in a table. The extract step unwraps and parses it:

```python
# src/parsing.py (shape)
class Bike(BaseModel):
    bike_id: str
    range: str | None = None          # e-bikes only; "&nbsp;" cell = no range

class StationStatus(BaseModel):
    bikes: list[Bike]
    # computed (also in model_dump): total_bikes, total_electrical_bikes
    # (range present), total_non_electrical_bikes

def unwrap_station_response(body: str) -> str: ...   # JSON -> renderHtml
def parse_station_html(html: str) -> StationStatus: ...
def parse_station_response(body: str) -> StationStatus: ...
```

- **Pure functions** — text in, Pydantic model out, no I/O. Unit-tested against saved real responses (`tests/fixtures/station_*.json`: mixed, e-bikes only, empty, velostation).
- **Fail loudly on layout changes** — a missing bike table raises. An **empty `<tbody>` means "no bikes docked"**; a missing table means "parser broke". A failing parse fails the whole run: nothing is published, so a half-parsed snapshot never reaches BigQuery.
- **One exception, by design: velostations** (secured bike parkings, icon `stationIsVelostation`). Their page has no counters and no table at all; they are recorded with 0 bikes. Any other page without the table still raises.
- **Libraries** — `beautifulsoup4` (switch to `selectolax` only if parsing becomes slow: 1,687 pages currently take ~17 s end to end), `zoneinfo`/`datetime` for times; no `pytz`, no `pandas`.

**Async collection:** `httpx.AsyncClient` with an `asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)`, transport-level retries (`retries=2`) and a per-request timeout; `asyncio.gather` over all stations. Any HTTP error (`raise_for_status`) fails the run.

### Step 3: Merge & Store

Master fields + parsed status are merged into one snapshot (see `publibike.json` for an example):

```json
{
  "snapshot_id": "20260918T174500Z",
  "observed_at": "2026-09-18T17:45:00Z",
  "source": "publibike",
  "stations": [
    {
      "station_id": "…", "station_name": "…", "station_address": "…",
      "lat": 46.802, "lng": 7.151, "station_number": "300240",
      "details_route": "https://www.velospot.info/customer/public/getStationInfo/…",
      "status": {
        "bikes": [ { "bike_id": "23485", "range": "41-50 km" }, { "bike_id": "23510", "range": null } ],
        "total_bikes": 2, "total_electrical_bikes": 1, "total_non_electrical_bikes": 1
      }
    }
  ]
}
```

`observed_at` is the run's start, truncated to the minute (`snapshot_id` is the same instant, compact). **Two objects per run**, partitioned by date:

```
gs://pbk-raw-dev/stations/raw_data/
  2026-09-20/
    12:30:00/
      raw.jsonl     # landing: every station response untouched, one {"station_id","body"} per line
      data.json     # bronze: merged, parsed snapshot (the only file the load step reads)
```

- **Write order** — `raw.jsonl` first, `data.json` last (and only then the Pub/Sub message): a `data.json` never exists without its raw data behind it.
- **Why keep the raw responses** — `data.json` is already an interpretation (parser output). If the parser has a bug or ignores a field, the raw responses allow re-parsing the full history; the API only returns the *current* state, so they are the only copy of the past. A full refresh from BigQuery alone cannot recover what the parser never extracted.
- **Why one file per run, not one per station** — GCS bills per operation and a bucket-lifecycle transition is an operation too: 1,687 objects × 288 runs/day ≈ 486,000 writes/day would cost more than the rest of the platform. One `raw.jsonl` (~5.5 MB, plain text, no gzip: storage is ~$1/month) keeps it at 2 objects per run. A re-parse reads ~17,000 files per 2 months instead of ~29 million.
- **Lifecycle** — a rule (pbk-infra) moves `raw.jsonl` objects to Coldline after 30 days.
- **Re-parse (when a field was missed)** — read the `raw.jsonl` files from a job *inside GCP* (egress), write the new snapshots to a **new prefix or table**, never over the originals (immutability); a new load needs its own job-id suffix (see *Load*).

---

## Configuration with Pydantic

**See dev-environment.md** for the Pydantic BaseSettings pattern, `.envrc` setup, and how configuration works across all repos.

For pbk-ingest, one flat settings class per entry point — each reads only the variables it needs:

```python
# src/settings.py
class ExtractSettings(BaseSettings):
    """Extract (Cloud Run Job) configuration."""

    gcp_project_id: str
    gcs_bucket: str
    pubsub_topic: str
    api_master_url: str
    gcs_output_path_template: str = "stations/raw_data/{date}/{time}/data.json"
    max_concurrent_requests: int = 10
    request_timeout_seconds: int = 30


class LoadSettings(BaseSettings):
    """Load (Cloud Function) configuration."""

    gcp_project_id: str
    bq_dataset: str = "bronze"
    bq_table: str = "publibike_snapshots"
```

- **Environment variables only** — fields map to `GCP_PROJECT_ID`, `GCS_BUCKET`, … (case-insensitive, Pydantic v2 default). No `env_file`: locally direnv exports `.envrc`, in GCP Cloud Run / Cloud Functions inject the variables.
- **Flat, one class per entry point** — the job doesn't need BigQuery settings and the function doesn't need API URLs, so each fails fast only on what it uses. The load function does **not** need the bucket: the Pub/Sub message carries the full `gs://` URI.
- **Instantiated in the entry points**, not as a module-level singleton — tests build them with explicit values.
- **Pydantic v2 style** — `model_config = SettingsConfigDict(...)` if needed, never the v1 `class Config:`.
- **Google client libraries** also read `GOOGLE_CLOUD_PROJECT` (set in `.envrc` to avoid the "no project ID" warning); clients are additionally built with an explicit `project=`.

---

## Code Structure

### Extract: Cloud Run Job

```python
# src/extract.py (shape)
class Extract:
    def __init__(self, config, gcs_client=None, transport=None): ...   # transport injectable for tests

    async def get_master_data(self, client) -> list[dict]:
        """GET master -> response['velospot']['responseData'] (raises if empty)."""

    async def get_station_data(self, client, semaphore, master) -> tuple[dict, str]:
        """GET detailsRoute -> (merged station dict, raw response body); parses with parse_station_response."""

    async def collect_all_stations(self, now) -> tuple[dict, dict[str, str]]:
        """Master, then gather all stations. Returns (snapshot, {station_id: raw body})."""

    def write_to_gcs(self, data, raw_pages, now) -> str:
        """Write raw.jsonl, then data.json. Returns the gs:// URI of data.json."""
```

```python
# main_extract.py
async def main():
    settings = ExtractSettings()
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    extractor = Extract(settings)
    data, raw_pages = await extractor.collect_all_stations(now)
    gcs_uri = extractor.write_to_gcs(data, raw_pages, now)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic)
    publisher.publish(topic_path, gcs_uri.encode()).result()   # message body = gs:// URI of data.json
```

Each step logs (master count, fetched/parsed count and duration, snapshot stations and bikes, each object written with its size, the publish). `httpx`'s per-request INFO logging is silenced (one line per station otherwise). Logging goes to **stdout**.

### Load: Cloud Function

```python
# src/load.py (shape)
class Load:
    def load_from_gcs(self, gcs_uri: str) -> dict: ...
    def validate_schema(self, data: dict) -> None: ...      # src/validation.py; raises ValueError
    def load_to_bigquery(self, data: dict) -> None: ...

# main_load.py
@functions_framework.cloud_event
def load(cloud_event):
    """Pub/Sub message (base64 of the gs:// URI) -> read -> validate -> BigQuery. Exceptions propagate."""
```

- **`payload` is the snapshot as a JSON *object*** — pass the dict, never `json.dumps(...)`: a string would be stored as a JSON string value and `$.stations` would return NULL.
- **Load job, not streaming insert** — one `load_table_from_json` job per message (288/day, far below the daily limit), `WRITE_APPEND`, explicit schema, table created by pbk-infra (never by the code).
- **Idempotent** — Pub/Sub is at-least-once. The job id is derived from the snapshot (`load_<source>_<snapshot_id>`). On "already exists", the code looks at the existing job: **succeeded → already loaded, skip; failed → retry with `…_r1`, `…_r2`**, and raise after 3 attempts. (Treating any conflict as "already loaded" would silently drop a snapshot whose first job failed.) Job ids live ~180 days.
- **Retries** — the function is deployed with `--retry`: any exception redelivers the message.
- **Local run** — `main_load.py` has a `__main__` entry point taking a `gs://` URI (`make load URI=…`), sharing the code path with the handler.
- **Logging** — configured **at import time** (`basicConfig(stream=sys.stdout, level=INFO, force=True)`): the deployed function never runs `__main__`, and Python's root logger drops INFO, so without this Cloud Logging only shows the platform's request lines. Each invocation logs the snapshot read (id, stations), the load, and "already loaded" on duplicates.

### Project Structure

```
pbk-ingest/
├── src/
│   ├── settings.py              # Pydantic BaseSettings
│   ├── parsing.py               # pure parser: response/HTML -> StationStatus
│   ├── extract.py               # Extract class
│   ├── load.py                  # Load class
│   ├── validation.py            # snapshot validation
│   └── __init__.py
├── main_extract.py              # Cloud Run Job entry point
├── main_load.py                 # Cloud Function entry point (+ local __main__)
├── tests/
│   ├── fixtures/                # real station responses + a master sample
│   ├── test_parsing.py
│   ├── test_extract.py          # httpx.MockTransport, mocked GCS
│   ├── test_load.py             # mocked BigQuery
│   └── test_settings.py
├── archive/                     # previous Airflow / monolithic implementation (excluded from lint, tests, hooks)
├── .github/workflows/           # reusable_wf.yml, pull_request_checks.yml, cd.yml, release.yml
├── pyproject.toml               # uv project, patch-level deps, Python 3.14
├── .envrc.example               # config template (commit); .envrc is gitignored
├── .pre-commit-config.yaml
├── Makefile                     # install, tests, ruff, pre-commit, login, extract, load
├── Dockerfile                   # Cloud Run Job image
└── README.md
```

---

## Configuration (.envrc)

**See dev-environment.md** for `.envrc` setup (local development only, never committed).

```bash
# GCP
export GCP_PROJECT_ID=pbk-dev-bda8
export GCP_REGION=europe-west6
export GOOGLE_CLOUD_PROJECT=$GCP_PROJECT_ID   # default project for Google client libraries
export GCS_BUCKET=pbk-raw-dev
export BQ_DATASET=bronze

# PubliBike API (stations come from `velospot.responseData`, each with its own detailsRoute)
export API_MASTER_URL=https://rest.publibike.ch/v1/public/all/stations

# Pub/Sub
export PUBSUB_TOPIC=pbk-load

# Extraction settings
export MAX_CONCURRENT_REQUESTS=10
export REQUEST_TIMEOUT_SECONDS=30
```

**Local credentials:** `make login` runs `gcloud auth login`, `gcloud auth application-default login` and `gcloud auth application-default set-quota-project $GCP_PROJECT_ID`. The **quota project** is the project Google bills/limits API calls to when code runs with *user* credentials (ADC); it must be a project (not a billing account or folder), with the APIs enabled, and the user needs `serviceusage.services.use` on it. ADC is a single file per machine, so the last value set wins — hence it is part of `make login`, with the environment's project. Deployed code uses service accounts and has no quota project.

**Production:** variables injected as Cloud Run job / Cloud Function env vars, secrets from Secret Manager (see dev-environment.md).

---

## Dependencies (pyproject.toml)

**See dev-environment.md** for the versioning strategy (patch-level constraints only: `>=X.Y.Z,<X.Y+1.0`). Python 3.14.

```toml
[project]
requires-python = ">=3.14,<3.15"
dependencies = [
    "google-cloud-storage>=3.14.1,<3.15.0",
    "google-cloud-bigquery>=3.45.2,<3.46.0",
    "google-cloud-pubsub>=2.41.0,<2.42.0",
    "pydantic>=2.13.5,<2.14.0",
    "pydantic-settings>=2.15.0,<2.16.0",
    "httpx>=0.28.1,<0.29.0",
    "beautifulsoup4>=4.15.0,<4.16.0",
    "functions-framework>=3.9.2,<3.10.0",
]

[dependency-groups]
dev = ["pre-commit", "pytest", "pytest-asyncio", "ruff"]   # asyncio_mode = "auto"
```

---

## Deployment

### GitHub Actions (CI/CD)

Workflows are inline in the repository for now (`.github/workflows/`; to be factored into pbk-actions, see `actions.md` for triggers, WIF auth and the build-once/promote-by-SHA rule):

| Workflow | Trigger | Does |
|---|---|---|
| `pull_request_checks.yml` | PR to `dev` / `stg` / `prd` | CI only (`reusable_wf.yml`: `pre-commit` and `tests` jobs in parallel) |
| `cd.yml` | push to `dev` / `stg` | 1) CI → 2) `build-image` (build & push on **dev**, check it exists on stg) → 3) `deploy-cloud-run`, `deploy-cloud-function`, `deploy-scheduler` in parallel |
| `release.yml` | tag `v*.*.*` | 1) CI → 2) `promote-image` (check the dev-built image exists, add the version tag) → 3) the same three deploy jobs on prd |

The image is `${GCP_REGION}-docker.pkg.dev/${GAR_PROJECT_ID}/pbk-ingest/pbk-ingest:${sha}` (repository and image name = `${PROJECT}-${STREAM}`).

- **Variables** — repository-scope ones (`GCP_REGION`, `PUBSUB_TOPIC`, `WIF_POOL_*`, `PROJECT`, `STREAM`, `IMAGE`) are in a workflow-level `env:`; environment-scope ones (`GCP_PROJECT_ID`, `GCP_PROJECT_NUMBER`, `SERVICE_ACCOUNT_*`, `GCS_BUCKET_NAME`) only resolve inside a job that declares `environment:`, so each job lists the few it needs.
- **Stage 3 is three independent jobs** — a failed function deploy does not stop the job and scheduler deploys (re-run only the failed job from the Actions UI). On the very first deploy the scheduler can be created a few seconds before the job exists, so its first trigger may miss; add `needs: deploy-cloud-run` to `deploy-scheduler` if that matters. A failed function deploy while extract runs means snapshots pile up in GCS without reaching BigQuery (recoverable with `make load URI=…`).

The deploy steps:

```bash
# Cloud Run Job (extract): one task, never overlapping the next run
# (--set-env-vars may be repeated, one variable per line: gcloud merges them into one dict)
gcloud run jobs deploy pbk-ingest-extract --image <image>:<sha> --region europe-west6 \
  --memory 2Gi --cpu 1 --task-timeout 240s --max-retries 0 \
  --service-account $SERVICE_ACCOUNT_EXTRACT \
  --set-env-vars GCP_PROJECT_ID=… \
  --set-env-vars GCS_BUCKET=… \
  --set-env-vars PUBSUB_TOPIC=… \
  --set-env-vars API_MASTER_URL=…

# Cloud Function (load): Cloud Functions needs main.py + requirements.txt, so stage them
mkdir build_fn && cp -r src build_fn/src && cp main_load.py build_fn/main.py
uv export --no-dev --no-hashes --no-emit-project --format requirements-txt > build_fn/requirements.txt
gcloud functions deploy pbk-ingest-load --gen2 --region europe-west6 --runtime python314 \
  --source build_fn --entry-point load --trigger-topic pbk-load --retry --memory 512Mi \
  --service-account $SERVICE_ACCOUNT_LOAD --trigger-service-account $SERVICE_ACCOUNT_LOAD \
  --build-service-account projects/$GCP_PROJECT_ID/serviceAccounts/$SERVICE_ACCOUNT_GH \
  --set-env-vars GCP_PROJECT_ID=…

# Cloud Scheduler: describe → create or update (no create-or-update command exists)
gcloud scheduler jobs $ACTION http pbk-ingest-extract --location europe-west6 \
  --schedule "*/5 * * * *" --time-zone "Etc/UTC" \
  --uri "https://run.googleapis.com/v2/projects/$GCP_PROJECT_ID/locations/europe-west6/jobs/pbk-ingest-extract:run" \
  --http-method POST --oauth-service-account-email $SERVICE_ACCOUNT_EXTRACT
```

- **`run jobs deploy`** creates the job on the first run and updates it afterwards (`create` would fail on the second deploy).
- **Function defaults** — a new gen2 function gets 256 MB and a 60 s timeout; load is set to 512 Mi (a run takes 4–15 s including cold starts).
- **`--retry`** — without it a failed event is dropped; with it Pub/Sub redelivers, which is safe because load is idempotent (job id from the snapshot). A message that always fails (e.g. a corrupt `data.json`) is retried with backoff for up to 7 days; a dead-letter topic on the trigger subscription would cap that — **not provisioned yet**.
- **Build service account** — a gen2 function is built by Cloud Build, by default as the project's default compute account, which `pbk-ingest-gh` cannot act as (403 `iam.serviceaccounts.actAs`). The deploy passes `--build-service-account` = `pbk-ingest-gh` itself (fewer identities than a dedicated one; the CI identity already has deploy rights).
- **Two runtime service accounts, created by pbk-infra:**

| Service account | Used by | Roles |
|---|---|---|
| `pbk-ingest-extract` (`SERVICE_ACCOUNT_EXTRACT`) | Cloud Run Job **and** Cloud Scheduler | `storage.objectCreator` on the bucket, `pubsub.publisher` on `pbk-load`, `run.invoker` on the job (so the scheduler can start it) |
| `pbk-ingest-load` (`SERVICE_ACCOUNT_LOAD`) | Cloud Function (runtime + Pub/Sub trigger) | `storage.objectViewer` on the bucket, `bigquery.dataEditor` on `bronze`, `bigquery.jobUser`, `run.invoker` on the function (so the trigger can call it) |

- **Why the scheduler shares the extract SA** — it only starts that job; a third account would add nothing.
- **Why load is separate** — it is the only identity allowed to write to BigQuery `bronze`; the extract side can only add files and publish.
- **`pbk-ingest-gh`** (`SERVICE_ACCOUNT_GH`, the repository's own CI/CD SA) needs `roles/iam.serviceAccountUser` on both runtime SAs **and on itself** (it is the build account), plus `run.developer`, `cloudfunctions.developer`, `cloudscheduler.admin` and `cloudbuild.builds.builder` on the project.

Environment variables are injected at runtime (never in Docker images). See **dev-environment.md**.

### Terraform (pbk-infra)

Provisions the static GCP components pbk-ingest relies on:
- Service accounts `pbk-ingest-extract` (job + scheduler) and `pbk-ingest-load` (function) with least-privilege IAM roles
- Pub/Sub topic (`pbk-load`) — no retention, so messages published while no subscription exists (e.g. before the function is deployed) are discarded
- GCS bucket (`pbk-raw-dev`, `EU` multi-region, see *Locations* in `infra.md`) with retention policy + lifecycle rule (`raw.jsonl` → Coldline after 30 days)
- BigQuery dataset & table (`bronze.publibike_snapshots`)
- Service account `pbk-ingest-gh` (CI/CD) with the deploy roles above (including `cloudbuild.builds.builder`, and `serviceAccountUser` on itself) and the WIF binding letting only the `pbk-ingest` repository act as it (pool and provider come from gcp-org)

**Not here:** Cloud Run Job, Cloud Function and Cloud Scheduler (GitHub Actions, above); the Artifact Registry repository (shared, in `envs/artifacts/` of pbk-infra).

---

## BigQuery Table Schema

```sql
CREATE TABLE IF NOT EXISTS bronze.publibike_snapshots (
  snapshot_id STRING NOT NULL,
  observed_at TIMESTAMP NOT NULL,
  source STRING NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  payload JSON
)
PARTITION BY DATE(observed_at)
OPTIONS(
  description="Raw PubliBike station snapshots (bronze layer, immutable)"
);
```

- **`payload`** — the full merged snapshot (`data.json`, see `publibike.json`) as a JSON object, untouched. dbt staging extracts fields from it (`UNNEST(JSON_QUERY_ARRAY(payload, '$.stations'))`, then `$.status.bikes`; `JSON_VALUE`, `LAX_FLOAT64`, `LAX_INT64`), so any field can be picked up later and rebuilt over the full history (see *Full Refresh* in `dbt.md`).
- **`observed_at` vs `ingested_at`** — when the snapshot was taken vs. when it landed in BigQuery; dbt uses `observed_at` as the business time.
- **No partition expiration** — the full history is kept; silver/gold are rebuilt from it.
- **Layers** — *landing* (truly raw): GCS `raw.jsonl`. *Bronze*: GCS `data.json` and this table (parsed from HTML to JSON, not modeled; nothing flattened or typed). *Silver*: dbt (flattened, typed, deduplicated, Data Vault).
- **Size** — each snapshot is ~1,700 stations and ~8,000 bikes (~1.1 MB of JSON); filter on `observed_at` (partition) when exploring bike-level queries.

---

## Testing

```bash
make install        # uv sync --dev
make tests          # unit tests (CI runs: pytest tests -m "not integration_test")
make ruff           # lint
make hooks          # one-time per clone: install the git hook so `git commit` runs the hooks
make pre-commit     # hooks over all files (archive/ and tests/fixtures/ excluded)

make login          # gcloud + ADC + quota project (see Configuration)

# Local extract: REAL run — writes to the bucket and publishes to Pub/Sub of $GCP_PROJECT_ID
make extract

# Local load: reads a data.json from GCS and appends one row to BigQuery
make load URI=gs://pbk-raw-dev/stations/raw_data/2026-09-20/12:30:00/data.json
```

- **Hooks** — a hook only runs on commit after `make hooks`; CI always runs `make pre-commit` and fails if a hook modifies a file. Ruff 0.17 also formats code blocks inside Markdown, so `*.md` is in ruff's `extend-exclude` (the docs hold illustrative snippets).
- **Unit tests** use real response fixtures, `httpx.MockTransport` (no network) and mocked GCS / BigQuery clients. Mocks cannot catch type mistakes in the BigQuery payload: check a real load with `JSON_TYPE(payload)` (must be `object`) and `ARRAY_LENGTH(JSON_QUERY_ARRAY(payload,'$.stations'))`.
- **Idempotency check** — run the same `make load` twice: the second logs "already loaded", the table keeps one row.
- **Test row cleanup** — a deleted row's load job id stays taken (~180 days): reload with another snapshot.

---

## Monitoring & Alerts

**Cloud Run Job metrics:**
- Execution time
- Success/failure rate
- API error rates

**Cloud Function metrics:**
- Invocation count
- Duration
- Errors (Pub/Sub dead-letter queue)

**BigQuery metrics:**
- Row count per day
- Table size
- Query performance

**Setup:** Cloud Monitoring dashboard + Cloud Alerting policies

---

## Modular for Frequency Changes

The `MAX_CONCURRENT_REQUESTS` and `REQUEST_TIMEOUT_SECONDS` env vars allow easy tuning:
- Adjust Cloud Scheduler frequency (currently 5 min)
- Adjust concurrency if API rate limits
- Extend timeout if API slow

The Cloud Run job itself has a 240 s task timeout and no retries, so a slow or failing run never overlaps the next one (a full run takes ~20 s).

---

## Related Repositories

- **gcp-org** — GCP projects, WIF pool and provider
- **pbk-infra** — Terraform for service accounts, Pub/Sub, GCS, BigQuery, WIF bindings
- **pbk-dbt** — Transform (bronze → silver/gold)
- **pbk-ml** — Forecasting from gold layer
- **pbk-dash** — Visualizations of current + forecasted availability
- **pbk-project** — Global architecture docs
