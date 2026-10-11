import json
import logging
from datetime import UTC, datetime

from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage

from src.settings import LoadSettings
from src.validation import validate_snapshot

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3

SCHEMA = [
    bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("observed_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON"),
]


class Load:
    def __init__(
        self,
        config: LoadSettings,
        bq_client: bigquery.Client | None = None,
        gcs_client: storage.Client | None = None,
    ):
        self.config = config
        self.bq_client = bq_client or bigquery.Client(project=config.gcp_project_id)
        self.gcs_client = gcs_client or storage.Client(project=config.gcp_project_id)

    def load_from_gcs(self, gcs_uri: str) -> dict:
        """Read `gs://bucket/path` as JSON."""
        blob = storage.Blob.from_string(gcs_uri, client=self.gcs_client)
        return json.loads(blob.download_as_text())

    def validate_schema(self, data: dict) -> None:
        validate_snapshot(data)

    def load_to_bigquery(self, data: dict) -> None:
        """Append one row. The job id is derived from the snapshot, so a redelivered
        Pub/Sub message cannot insert the same snapshot twice."""
        row = {
            "snapshot_id": data["snapshot_id"],
            "observed_at": data["observed_at"],
            "source": data["source"],
            "ingested_at": datetime.now(UTC).isoformat(),
            "payload": data,
        }
        table = f"{self.config.gcp_project_id}.{self.config.bq_dataset}.{self.config.bq_table}"
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        base_job_id = f"load_{data['source']}_{data['snapshot_id']}"
        # a failed job keeps its id, so a retry needs a new one; a succeeded one means "already loaded"
        for attempt in range(MAX_ATTEMPTS):
            job_id = base_job_id if attempt == 0 else f"{base_job_id}_r{attempt}"
            try:
                job = self.bq_client.load_table_from_json(
                    [row], table, job_config=job_config, job_id=job_id
                )
            except Conflict:
                if self.bq_client.get_job(job_id).error_result is None:
                    logger.info(
                        "snapshot %s already loaded, skipping", data["snapshot_id"]
                    )
                    return
                continue
            job.result()
            return
        raise RuntimeError(
            f"snapshot {data['snapshot_id']}: all {MAX_ATTEMPTS} load jobs failed"
        )
